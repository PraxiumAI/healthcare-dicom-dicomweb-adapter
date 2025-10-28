// Copyright 2018 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.google.cloud.healthcare.imaging.dicomadapter;

import com.google.api.client.http.HttpResponseException;
import com.google.cloud.healthcare.IDicomWebClient.DicomWebException;
import com.google.cloud.healthcare.deid.redactor.DicomRedactor;
import com.google.cloud.healthcare.imaging.dicomadapter.cstore.DicomStreamUtil;
import com.google.cloud.healthcare.imaging.dicomadapter.cstore.destination.DestinationHolder;
import com.google.cloud.healthcare.imaging.dicomadapter.cstore.destination.IDestinationClientFactory;
import com.google.cloud.healthcare.imaging.dicomadapter.cstore.multipledest.IMultipleDestinationUploadService;
import com.google.cloud.healthcare.imaging.dicomadapter.cstore.multipledest.IMultipleDestinationUploadService.MultipleDestinationUploadServiceException;
import com.google.cloud.healthcare.imaging.dicomadapter.monitoring.Event;
import com.google.cloud.healthcare.imaging.dicomadapter.monitoring.MonitoringService;
import com.google.common.io.CountingInputStream;
import io.sentry.Sentry;
import io.sentry.SentryLevel;
import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorCompletionService;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.dcm4che3.data.Attributes;
import org.dcm4che3.data.Tag;
import org.dcm4che3.data.UID;
import org.dcm4che3.data.VR;
import org.dcm4che3.imageio.codec.Transcoder;
import org.dcm4che3.io.DicomInputStream;
import org.dcm4che3.io.DicomOutputStream;
import org.dcm4che3.net.Association;
import org.dcm4che3.net.PDVInputStream;
import org.dcm4che3.net.Status;
import org.dcm4che3.net.pdu.PresentationContext;
import org.dcm4che3.net.service.BasicCStoreSCP;
import org.dcm4che3.net.service.DicomServiceException;
import org.dcm4che3.util.StreamUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class to handle server-side C-STORE DICOM requests.
 */
public class CStoreService extends BasicCStoreSCP {

  private static Logger log = LoggerFactory.getLogger(CStoreService.class);

  private static final Pattern VARIABLE_PATTERN = Pattern.compile("\\{([A-Z_]+)\\}");

  private final IDestinationClientFactory destinationClientFactory;
  private final IMultipleDestinationUploadService multipleSendService;
  private final DicomRedactor redactor;
  private final String transcodeToSyntax;
  private final List<ImportAdapter.PrivateTagConfig> privateTagConfigs;
  private final DatabaseConfigService databaseConfigService;
  private final boolean sentryEnabled;

  CStoreService(IDestinationClientFactory destinationClientFactory,
                DicomRedactor redactor,
                String transcodeToSyntax,
                IMultipleDestinationUploadService multipleSendService,
                List<ImportAdapter.PrivateTagConfig> privateTagConfigs,
                DatabaseConfigService databaseConfigService,
                boolean sentryEnabled) {
    this.destinationClientFactory = destinationClientFactory;
    this.redactor = redactor;
    this.transcodeToSyntax = transcodeToSyntax != null && transcodeToSyntax.length() > 0 ? transcodeToSyntax : null;
    this.multipleSendService = multipleSendService;
    this.privateTagConfigs = privateTagConfigs != null ? privateTagConfigs : new ArrayList<>();
    this.databaseConfigService = databaseConfigService;
    this.sentryEnabled = sentryEnabled;

    if(this.transcodeToSyntax != null) {
      log.info("Transcoding to: " + transcodeToSyntax);
    }

    if(!this.privateTagConfigs.isEmpty()) {
      log.info("Private tags configured: " + this.privateTagConfigs.size() + " tags will be added during C-STORE");
    }

    if (log.isDebugEnabled()) {
      log.debug("C-STORE pipeline: redactor={}, addPrivateTags={}, transcodeTo={}, multiDest={}",
          this.redactor != null,
          !this.privateTagConfigs.isEmpty(),
          this.transcodeToSyntax,
          this.multipleSendService != null);
    }

    // Log build info to confirm runtime code version
    try {
      String shortSha = BuildInfo.shortCommit();
      String time = BuildInfo.buildTime();
      String ver = BuildInfo.version();
      log.info("Import Adapter build: version={}, commit={}, builtAt={}", ver, shortSha, time);
    } catch (Throwable ignored) {
    }
  }

  @Override
  protected void store(
      Association association,
      PresentationContext presentationContext,
      Attributes request,
      PDVInputStream inPdvStream,
      Attributes response)
      throws IOException {
    try {
      MonitoringService.addEvent(Event.CSTORE_REQUEST);

      // Perform AET-based authorization check if database service is available
      if (databaseConfigService != null) {
        String callingAET = association.getAAssociateAC().getCallingAET();
        String calledAET = association.getAAssociateAC().getCalledAET();

        // Will throw DicomServiceException on unauthorized or DB error
        UUID deviceId = databaseConfigService.getAuthorization(callingAET, calledAET);
        association.setProperty("DEVICE_ID", deviceId.toString());
      }

      String sopClassUID = request.getString(Tag.AffectedSOPClassUID);
      String sopInstanceUID = request.getString(Tag.AffectedSOPInstanceUID);
      String transferSyntax = presentationContext.getTransferSyntax();

      validateParam(sopClassUID, "AffectedSOPClassUID");
      validateParam(sopInstanceUID, "AffectedSOPInstanceUID");

      DestinationHolder destinationHolder =
          destinationClientFactory.create(association.getAAssociateAC().getCallingAET(), transferSyntax, inPdvStream);

      final CountingInputStream countingStream = destinationHolder.getCountingInputStream();

      List<StreamProcessor> processorList = new ArrayList<>();

      // 1. Redaction (if configured)
      if (redactor != null) {
        processorList.add(redactor::redact);
      }

      // 2. Add private tags (if configured)
      if (!privateTagConfigs.isEmpty()) {
        processorList.add((inputStream, outputStream) ->
          addPrivateTags(inputStream, outputStream, association, transferSyntax, destinationHolder)
        );
      }

      // 3. Transcoding (if needed)
      if(transcodeToSyntax != null && !transcodeToSyntax.equals(transferSyntax)) {
        processorList.add((inputStream, outputStream) -> {
          try (Transcoder transcoder = new Transcoder(inputStream)) {
            transcoder.setIncludeFileMetaInformation(true);
            transcoder.setDestinationTransferSyntax(transcodeToSyntax);
            transcoder.transcode((transcoder1, dataset) -> outputStream);
          }
        });
      }

      if (multipleSendService != null) {
        processorList.add((inputStream, outputStream) -> {
          multipleSendService.start(
              destinationHolder.getHealthcareDestinations(),
              destinationHolder.getDicomDestinations(),
              inputStream,
              sopClassUID,
              sopInstanceUID,
              association.getSerialNo()
            );
        });
      } else {
        processorList.add((inputStream, outputStream) -> {
          destinationHolder.getSingleDestination().stowRs(inputStream);
        });
      }

      try(InputStream inWithHeader = DicomStreamUtil.dicomStreamWithFileMetaHeader(
              sopInstanceUID, sopClassUID, transferSyntax, countingStream)) {
        processStream(association.getApplicationEntity().getDevice().getExecutor(),
            inWithHeader, processorList);
      } catch (IOException e) {
        throw new DicomServiceException(Status.ProcessingFailure, e);
      }

      response.setInt(Tag.Status, VR.US, Status.Success);
      MonitoringService.addEvent(Event.CSTORE_BYTES, countingStream.getCount());
    } catch (DicomWebException e) {
      // Handle duplicate instances - log and set ErrorComment but return success to avoid failures in batch uploads
      if (isDuplicateInstance(e)) {
        String sopInstanceUID = request.getString(Tag.AffectedSOPInstanceUID);
        log.info("Duplicate instance detected - instance already exists in DICOM store. " +
                 "Returning success with ErrorComment. SOP Instance UID = {}", sopInstanceUID);

        if (sentryEnabled) {
          Sentry.captureMessage(
              "Duplicate DICOM instance detected: " + sopInstanceUID,
              SentryLevel.INFO);
        }

        // Return success (0x0000) with descriptive ErrorComment
        // By returning normally (not throwing DicomServiceException), we avoid A-ABORT and keep batch uploads going
        response.setInt(Tag.Status, VR.US, Status.Success);
        response.setString(Tag.ErrorComment, VR.LO,
            "Instance already exists (duplicate upload detected)");

        // Note: Cannot report byte count here as countingStream is out of scope
        return;
      }

      // For other DicomWeb errors (non-duplicate 409s, or other HTTP errors), report and throw
      reportError(e, Event.CSTORE_ERROR);
      throw new DicomServiceException(e.getStatus(), e);
    } catch (DicomServiceException e) {
      reportError(e, Event.CSTORE_ERROR);
      throw e;
    } catch (MultipleDestinationUploadServiceException me) {
      reportError(me, null);
      throw new DicomServiceException(me.getDicomStatus() != null ? me.getDicomStatus() : Status.ProcessingFailure, me);
    } catch (Throwable e) {
      reportError(e, Event.CSTORE_ERROR);
      throw new DicomServiceException(Status.ProcessingFailure, e);
    }
  }

  @Override
  public void onClose(Association association) {
    super.onClose(association);
    if (multipleSendService != null) {
      multipleSendService.cleanup(association.getSerialNo());
    }
  }

  private void reportError(Throwable e, Event event) {
    if (event != null) {
      MonitoringService.addEvent(event);
    }
    log.error("C-STORE request failed: ", e);

    // Send error to Sentry if initialized
    if (sentryEnabled) {
      Sentry.captureException(e);
    }
  }

  private void validateParam(String value, String name) throws DicomServiceException {
    if (value == null || value.trim().length() == 0) {
      throw new DicomServiceException(Status.CannotUnderstand, "Mandatory tag empty: " + name);
    }
  }

  /**
   * Checks if a DicomWebException indicates a duplicate instance error.
   *
   * @param e The DicomWebException to check
   * @return true if the exception indicates the instance already exists
   */
  private boolean isDuplicateInstance(DicomWebException e) {
    // Check for HTTP 409 Conflict status
    if (e.getHttpStatus() != 409) {
      return false;
    }

    // For Healthcare API STOW-RS, check if the underlying cause contains "already exists"
    // The response XML includes: <DicomAttribute tag="00090097">already exists</DicomAttribute>
    Throwable cause = e.getCause();
    String causeMessage = cause != null ? cause.getMessage() : null;
    String content = null;

    // Get response content if available
    if (cause instanceof com.google.api.client.http.HttpResponseException) {
      try {
        content = ((com.google.api.client.http.HttpResponseException) cause).getContent();
      } catch (Exception ignored) {
        // If we can't get content, continue to other checks
      }
    }

    // Check all available messages
    String exceptionMessage = e.getMessage();

    // Check for "already exists" in all message sources
    if ((causeMessage != null && causeMessage.toLowerCase().contains("already exists")) ||
        (content != null && content.toLowerCase().contains("already exists")) ||
        (exceptionMessage != null && exceptionMessage.toLowerCase().contains("already exists"))) {
      return true;
    }

    // Check for "resource is too large" errors - these are NOT duplicates
    // These should propagate as errors with proper status code (272 / 0x0110)
    if ((causeMessage != null && causeMessage.toLowerCase().contains("resource is too large")) ||
        (content != null && content.toLowerCase().contains("resource is too large"))) {
      log.error("DICOM instance exceeds Healthcare API size limit. " +
                "Exception: {}, Cause: {}", exceptionMessage, causeMessage);
      return false;
    }

    // If we can't confirm it's a duplicate, treat as a different type of 409 conflict
    // and let it propagate as an error
    log.warn("Received HTTP 409 Conflict but cannot confirm it's a duplicate instance. " +
             "Exception: {}, Cause: {}", exceptionMessage, causeMessage);
    return false;
  }

  /**
   * Resolves variable placeholders in a template string.
   *
   * @param template String potentially containing {VARIABLE} placeholders
   * @param association DICOM association for extracting runtime values
   * @return Resolved string with all variables replaced
   */
  private String resolveVariables(String template, Association association) {
    if (!template.contains("{")) {
      return template; // No variables to resolve
    }

    Matcher matcher = VARIABLE_PATTERN.matcher(template);
    StringBuffer result = new StringBuffer();

    while (matcher.find()) {
      String varName = matcher.group(1);
      String value = resolveVariable(varName, association);
      matcher.appendReplacement(result, Matcher.quoteReplacement(value));
    }
    matcher.appendTail(result);

    return result.toString();
  }

  /**
   * Resolves a single variable to its runtime value.
   *
   * @param varName Variable name (e.g., "CALLING_AET")
   * @param association DICOM association
   * @return Resolved value
   * @throws IllegalArgumentException if variable is unknown
   */
  private String resolveVariable(String varName, Association association) {
    switch (varName) {
      case "CALLING_AET":
        return association.getAAssociateAC().getCallingAET();
      case "CALLED_AET":
        return association.getAAssociateAC().getCalledAET();
      case "DEVICE_ID":
        Object deviceId = association.getProperty("DEVICE_ID");
        return deviceId != null ? deviceId.toString() : "";
      case "TIMESTAMP":
        return new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());
      default:
        throw new IllegalArgumentException("Unknown variable: " + varName);
    }
  }

  /**
   * Adds configured private tags to the DICOM dataset.
   * Reuses metadata already read by DestinationClientFactory to avoid double-reading.
   *
   * @param inputStream Input DICOM stream
   * @param outputStream Output DICOM stream with added tags
   * @param association DICOM association for variable resolution
   * @param transferSyntax Transfer syntax for output
   * @param destinationHolder Holder containing pre-read metadata
   * @throws IOException if reading/writing fails
   */
  private void addPrivateTags(
      InputStream inputStream,
      OutputStream outputStream,
      Association association,
      String transferSyntax,
      DestinationHolder destinationHolder) throws IOException {

    if (privateTagConfigs.isEmpty()) {
      StreamUtils.copy(inputStream, outputStream);
      return;
    }

    // Get metadata already read by DestinationClientFactory/DatabaseDestinationClientFactory
    Attributes dataset = destinationHolder.getMetadata();

    if (dataset != null) {
      // PATH 1: Metadata already read - reuse them
      // Add private tags to metadata
      for (ImportAdapter.PrivateTagConfig config : privateTagConfigs) {
        try {
          String value = resolveVariables(config.getValueTemplate(), association);
          dataset.setString(config.getTag(), config.getVr(), value);
        } catch (Exception e) {
          log.error("Failed to add private tag (0x" + Integer.toHexString(config.getTag()) + "): " + e.getMessage());
          throw new IOException("Failed to add private tag: " + e.getMessage(), e);
        }
      }

      // Create FMI with modified metadata
      Attributes fmi = Attributes.createFileMetaInformation(
          dataset.getString(Tag.SOPInstanceUID),
          dataset.getString(Tag.SOPClassUID),
          transferSyntax);

      // Write complete DICOM file with FMI + modified metadata + PixelData
      DicomOutputStream dos = new DicomOutputStream(outputStream, UID.ExplicitVRLittleEndian);
      dos.writeDataset(fmi, dataset);
      StreamUtils.copy(inputStream, dos);
      dos.finish();

    } else {
      // PATH 2: Fallback - metadata not read (shouldn't happen with current code)
      log.warn("addPrivateTags: Metadata not available, copying stream without modifications");
      StreamUtils.copy(inputStream, outputStream);
    }
  }

  private void processStream(Executor underlyingExecutor, InputStream inputStream,
      List<StreamProcessor> processorList) throws Throwable {
    if (processorList.size() == 1) {
      StreamProcessor singleProcessor = processorList.get(0);
      singleProcessor.process(inputStream, null);
    } else if (processorList.size() > 1) {
      List<StreamCallable> callableList = new ArrayList<>();

      PipedOutputStream pdvPipeOut = new PipedOutputStream();
      InputStream nextInputStream = new PipedInputStream(pdvPipeOut);
      for(int i=0; i < processorList.size(); i++){
        StreamProcessor processor = processorList.get(i);
        InputStream processorInput = nextInputStream;
        OutputStream processorOutput = null;

        if(i < processorList.size() - 1) {
          PipedOutputStream pipeOut = new PipedOutputStream();
          processorOutput = pipeOut;
          nextInputStream = new PipedInputStream(pipeOut);
        }

        callableList.add(new StreamCallable(processorInput, processorOutput, processor));
      }

      ExecutorCompletionService<Void> ecs = new ExecutorCompletionService<>(underlyingExecutor);
      for(StreamCallable callable : callableList){
        ecs.submit(callable);
      }

      try (pdvPipeOut) {
        // PDVInputStream is thread-locked
        StreamUtils.copy(inputStream, pdvPipeOut);
      } catch (IOException e) {
        // causes or is caused by exception in callables, no need to throw this up
        log.trace("Error copying inputStream to pdvPipeOut", e);
      }

      try {
        for (int i = 0; i < callableList.size(); i++) {
          ecs.take().get();
        }
      } catch (ExecutionException e) {
        throw e.getCause();
      }
    }
  }

  @FunctionalInterface
  private interface StreamProcessor {
    void process(InputStream inputStream, OutputStream outputStream) throws Exception;
  }

  private static class StreamCallable implements Callable<Void> {
    private final InputStream inputStream;
    private final OutputStream outputStream;
    private final StreamProcessor processor;

    public StreamCallable(InputStream inputStream, OutputStream outputStream,
        StreamProcessor processor) {
      this.inputStream = inputStream;
      this.outputStream = outputStream;
      this.processor = processor;
    }

    @Override
    public Void call() throws Exception {
      try (inputStream) {
        try (outputStream) {
          processor.process(inputStream, outputStream);
        }
      }
      return null;
    }
  }
}
