// Copyright 2024 Google LLC
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

package com.google.cloud.healthcare.imaging.dicomadapter.cstore.destination;

import com.google.api.client.http.HttpRequestFactory;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.healthcare.DicomWebClient;
import com.google.cloud.healthcare.DicomWebClientJetty;
import com.google.cloud.healthcare.IDicomWebClient;
import com.google.cloud.healthcare.StringUtil;
import com.google.cloud.healthcare.imaging.dicomadapter.DatabaseConfigService;
import com.google.cloud.healthcare.imaging.dicomadapter.cstore.DicomStreamUtil;
import java.sql.SQLException;
import java.util.concurrent.ConcurrentHashMap;
import org.dcm4che3.data.Attributes;
import org.dcm4che3.data.Tag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Factory for creating destination clients with database-driven dynamic routing.
 * Implements the routing logic:
 * 1. Check study_storage table by StudyInstanceUID
 * 2. If not found, check aet_storage table by calling AET
 * 3. If found via aet_storage, persist the mapping to study_storage
 * 4. If nothing found, use default destination
 */
public class DatabaseDestinationClientFactory implements IDestinationClientFactory {

  private static final Logger log = LoggerFactory.getLogger(DatabaseDestinationClientFactory.class);

  private final DatabaseConfigService databaseConfigService;
  private final IDicomWebClient defaultDicomWebClient;
  private final GoogleCredentials credentials;
  private final boolean useStowOverwrite;
  private final boolean useHttp2ForStow;

  // Cache of DicomWebClient instances by destination URL to avoid recreating clients
  private final ConcurrentHashMap<String, IDicomWebClient> clientCache = new ConcurrentHashMap<>();

  /**
   * Creates a new DatabaseDestinationClientFactory.
   *
   * @param databaseConfigService Service for database operations
   * @param defaultDicomWebClient Default client to use when no database routing applies
   * @param credentials Google credentials for creating new DicomWebClient instances
   * @param useStowOverwrite Whether to use STOW-RS overwrite mode
   * @param useHttp2ForStow Whether to use HTTP/2 for STOW-RS requests
   */
  public DatabaseDestinationClientFactory(
      DatabaseConfigService databaseConfigService,
      IDicomWebClient defaultDicomWebClient,
      GoogleCredentials credentials,
      boolean useStowOverwrite,
      boolean useHttp2ForStow) {
    this.databaseConfigService = databaseConfigService;
    this.defaultDicomWebClient = defaultDicomWebClient;
    this.credentials = credentials;
    this.useStowOverwrite = useStowOverwrite;
    this.useHttp2ForStow = useHttp2ForStow;
  }


  @Override
  public DestinationHolder create(String callingAet, String transferSyntax,
      java.io.InputStream inPdvStream) throws java.io.IOException {

    // For database routing, we need StudyInstanceUID (0020,000D) from stream.
    //
    // PROBLEM: PDVInputStream is a network stream that cannot be rewound (no mark/reset)
    //
    // SOLUTION: Buffer metadata using TeeInputStream + SequenceInputStream:
    // 1. Create ByteArrayOutputStream to buffer metadata as we read it
    // 2. Wrap inPdvStream with TeeInputStream that writes to buffer
    // 3. Read metadata through TeeInputStream WITHOUT BufferedInputStream wrapper
    //    (BufferedInputStream would buffer extra data that doesn't get written to buffer!)
    // 4. Create SequenceInputStream(buffer + remaining stream) for upload
    // 5. This ensures we can read metadata AND send complete file without race conditions

    // 1. Create buffer for metadata (typically < 64KB for DICOM headers)
    java.io.ByteArrayOutputStream metadataBuffer = new java.io.ByteArrayOutputStream();

    // 2. Create TeeInputStream that writes everything it reads into the buffer
    java.io.InputStream teeStream = new DicomStreamUtil.TeeInputStream(inPdvStream, metadataBuffer);

    // 3. Read attributes from TeeInputStream
    //    IMPORTANT: Do NOT wrap in BufferedInputStream here!
    //    BufferedInputStream would read ahead and buffer data that doesn't get written to metadataBuffer
    Attributes attrs;
    String studyInstanceUID;
    try (org.dcm4che3.io.DicomInputStream tempStream =
        new org.dcm4che3.io.DicomInputStream(teeStream, transferSyntax)) {
      attrs = tempStream.readDataset(-1, Tag.PixelData);
      studyInstanceUID = attrs.getString(Tag.StudyInstanceUID);
    }

    // 4. Create "rewindable" stream for sending:
    //    First replay buffered metadata, then continue with remaining stream (PixelData)
    java.io.InputStream streamForSending = new java.io.SequenceInputStream(
        new java.io.ByteArrayInputStream(metadataBuffer.toByteArray()),
        inPdvStream // Continues from where TeeInputStream stopped (at PixelData)
    );

    if (studyInstanceUID == null || studyInstanceUID.isEmpty()) {
      log.warn("StudyInstanceUID not found in DICOM attributes. Using default destination.");
      return new DestinationHolder(streamForSending, defaultDicomWebClient);
    }

    try {
      // Step 1: Check study_storage table
      String destination = databaseConfigService.getStorageForStudy(studyInstanceUID);

      if (destination != null) {
        log.debug("Found destination for study_uid={} in study_storage: {}",
            studyInstanceUID, destination);
        IDicomWebClient client = getOrCreateClient(destination);
        DestinationHolder holder = new DestinationHolder(streamForSending, defaultDicomWebClient);
        holder.setSingleDestination(client);
        return holder;
      }

      // Step 2: Check aet_storage table
      destination = databaseConfigService.getStorageForAet(callingAet);

      if (destination != null) {
        log.info("Found destination for aet={} in aet_storage: {}. Creating study mapping.",
            callingAet, destination);

        // Step 3: Persist the mapping to study_storage
        try {
          databaseConfigService.mapStudyToStorage(studyInstanceUID, destination);
        } catch (SQLException e) {
          // Log error but don't fail the request - we can still route to the destination
          log.error("Failed to persist study_uid={} to storage mapping. Continuing with routing.",
              studyInstanceUID, e);
        }

        IDicomWebClient client = getOrCreateClient(destination);
        DestinationHolder holder = new DestinationHolder(streamForSending, defaultDicomWebClient);
        holder.setSingleDestination(client);
        return holder;
      }

      // Step 4: No database routing found, use default
      log.debug("No database routing found for study_uid={}, aet={}. Using default destination.",
          studyInstanceUID, callingAet);
      return new DestinationHolder(streamForSending, defaultDicomWebClient);

    } catch (SQLException e) {
      log.error("Database error during destination routing for study_uid={}, aet={}. " +
          "Using default destination.", studyInstanceUID, callingAet, e);
      // On database errors, fall back to default destination rather than failing the request
      return new DestinationHolder(streamForSending, defaultDicomWebClient);
    }
  }

  /**
   * Gets or creates a DicomWebClient for the specified destination URL.
   * Clients are cached to avoid creating multiple instances for the same destination.
   *
   * @param destinationUrl The DICOMweb destination URL
   * @return A DicomWebClient instance
   */
  private IDicomWebClient getOrCreateClient(String destinationUrl) {
    return clientCache.computeIfAbsent(destinationUrl, url -> {
      log.info("Creating new DicomWebClient (HTTP/2={}) for destination: {}", useHttp2ForStow, url);

      if (useHttp2ForStow) {
        // HTTP/2 client (DicomWebClientJetty)
        String stowUrl = url.endsWith("/studies") ? url : url + "/studies";
        return new DicomWebClientJetty(credentials, stowUrl, useStowOverwrite);
      } else {
        // HTTP/1.1 client (DicomWebClient)
        HttpRequestFactory requestFactory =
            new NetHttpTransport().createRequestFactory(new HttpCredentialsAdapter(credentials));

        // Extract base address and path
        String baseAddress = url;
        String path = "studies";
        if (url.endsWith("/studies")) {
          int lastSlash = url.lastIndexOf("/studies");
          baseAddress = url.substring(0, lastSlash);
        }

        return new DicomWebClient(requestFactory, baseAddress, path, useStowOverwrite);
      }
    });
  }
}
