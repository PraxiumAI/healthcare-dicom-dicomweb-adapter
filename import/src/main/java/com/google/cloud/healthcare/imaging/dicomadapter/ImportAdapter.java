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

import com.beust.jcommander.JCommander;
import com.google.api.client.http.HttpRequestFactory;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.healthcare.DicomWebClient;
import com.google.cloud.healthcare.DicomWebClientJetty;
import com.google.cloud.healthcare.DicomWebValidation;
import com.google.cloud.healthcare.IDicomWebClient;
import com.google.cloud.healthcare.LogUtil;
import com.google.cloud.healthcare.StringUtil;
import com.google.cloud.healthcare.deid.redactor.DicomRedactor;
import com.google.cloud.healthcare.deid.redactor.protos.DicomConfigProtos;
import com.google.cloud.healthcare.deid.redactor.protos.DicomConfigProtos.DicomConfig;
import com.google.cloud.healthcare.deid.redactor.protos.DicomConfigProtos.DicomConfig.TagFilterProfile;
import com.google.cloud.healthcare.imaging.dicomadapter.cmove.CMoveSenderFactory;
import com.google.cloud.healthcare.imaging.dicomadapter.cstore.backup.BackupUploadService;
import com.google.cloud.healthcare.imaging.dicomadapter.cstore.backup.DelayCalculator;
import com.google.cloud.healthcare.imaging.dicomadapter.cstore.backup.GcpBackupUploader;
import com.google.cloud.healthcare.imaging.dicomadapter.cstore.backup.IBackupUploader;
import com.google.cloud.healthcare.imaging.dicomadapter.cstore.backup.LocalBackupUploader;
import com.google.cloud.healthcare.imaging.dicomadapter.cstore.destination.IDestinationClientFactory;
import com.google.cloud.healthcare.imaging.dicomadapter.cstore.destination.MultipleDestinationClientFactory;
import com.google.cloud.healthcare.imaging.dicomadapter.cstore.destination.SingleDestinationClientFactory;
import com.google.cloud.healthcare.imaging.dicomadapter.cstore.multipledest.MultipleDestinationUploadService;
import com.google.cloud.healthcare.imaging.dicomadapter.cstore.multipledest.sender.CStoreSenderFactory;
import com.google.cloud.healthcare.imaging.dicomadapter.monitoring.Event;
import com.google.cloud.healthcare.imaging.dicomadapter.monitoring.MonitoringService;
import com.google.common.collect.ImmutableList;
import io.sentry.Sentry;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.dcm4che3.data.VR;
import org.dcm4che3.net.Device;
import org.dcm4che3.net.service.BasicCEchoSCP;
import org.dcm4che3.net.service.DicomServiceRegistry;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ImportAdapter {

  private static final Logger log = LoggerFactory.getLogger(ImportAdapter.class);
  private static final String STUDIES = "studies";
  private static final String GCP_PATH_PREFIX = "gs://";
  private static final String FILTER = "filter";

  public static void main(String[] args) throws IOException, GeneralSecurityException {
    Flags flags = new Flags();
    JCommander jCommander = new JCommander(flags);
    jCommander.parse(args);

    String dicomwebAddress = DicomWebValidation.validatePath(flags.dicomwebAddress, DicomWebValidation.DICOMWEB_ROOT_VALIDATION);
    final boolean sentryEnabled = !flags.sentryDsn.isEmpty();

    if(flags.help){
      jCommander.usage();
      return;
    }

    // Adjust logging.
    LogUtil.Log4jToStdout(flags.verbose ? "DEBUG" : "ERROR");

    // Initialize Sentry if DSN is provided
    if (sentryEnabled) {
      Sentry.init(options -> {
        options.setDsn(flags.sentryDsn);
        options.setEnvironment("production");
        options.setTracesSampleRate(1.0);

        // Add data like request headers and IP for users
        options.setSendDefaultPii(true);

        // Enable debug mode (useful for troubleshooting)
        options.setDebug(flags.verbose);
      });
      log.info("Sentry error monitoring initialized with debug={}", flags.verbose);

      // Set global uncaught exception handler to capture all unhandled exceptions
      Thread.setDefaultUncaughtExceptionHandler((thread, throwable) -> {
        log.error("Uncaught exception in thread {}", thread.getName(), throwable);
        Sentry.captureException(throwable);
        // Give Sentry time to send the event before the thread dies
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      });
      log.info("Sentry global exception handler registered");
    }

    // Credentials, use the default service credentials.
    GoogleCredentials credentials = GoogleCredentials.getApplicationDefault();
    if (!flags.oauthScopes.isEmpty()) {
      credentials = credentials.createScoped(Arrays.asList(flags.oauthScopes.split(",")));
    }

    HttpRequestFactory requestFactory =
        new NetHttpTransport().createRequestFactory(new HttpCredentialsAdapter(credentials));

    // Initialize Database Configuration Service if database URL is provided
    DatabaseConfigService databaseConfigService = null;
    if (!flags.dbUrl.isEmpty()) {
      if (flags.dbUser.isEmpty() || flags.dbPassword.isEmpty()) {
        throw new IllegalArgumentException(
            "Database credentials are incomplete. If --db_url is specified, both --db_user and "
                + "--db_password must also be provided.");
      }

      try {
        databaseConfigService = new DatabaseConfigService(
            flags.dbUrl,
            flags.dbUser,
            flags.dbPassword,
            sentryEnabled);
        log.info("DatabaseConfigService initialized successfully");
      } catch (SQLException e) {
        log.error("Failed to initialize database connection. Application cannot start.", e);
        if (sentryEnabled) {
          Sentry.captureException(e);
        }
        throw new RuntimeException("Failed to connect to database on startup", e);
      }
    } else {
      log.info("Database configuration not provided. Running without database features.");
    }

    // Initialize Monitoring
    if (!flags.monitoringProjectId.isEmpty()) {
      MonitoringService.initialize(flags.monitoringProjectId, Event.values(), requestFactory);
      MonitoringService.addEvent(Event.STARTED);
    } else {
      MonitoringService.disable();
    }

    // Dicom service handlers.
    DicomServiceRegistry serviceRegistry = new DicomServiceRegistry();

    // Handle C-ECHO (all nodes which accept associations must support this).
    serviceRegistry.addDicomService(new BasicCEchoSCP());

    // Handle C-STORE
    String cstoreDicomwebAddr = dicomwebAddress;
    String cstoreDicomwebStowPath = STUDIES;
    if (cstoreDicomwebAddr.length() == 0) {
      cstoreDicomwebAddr = flags.dicomwebAddr;
      cstoreDicomwebStowPath = flags.dicomwebStowPath;
    }

    String cstoreSubAet = flags.dimseCmoveAET.equals("") ? flags.dimseAET : flags.dimseCmoveAET;
    if (cstoreSubAet.isBlank()) {
      throw new IllegalArgumentException("--dimse_aet flag must be set.");
    }

    IDicomWebClient defaultCstoreDicomWebClient = configureDefaultDicomWebClient(
        requestFactory, cstoreDicomwebAddr, cstoreDicomwebStowPath, credentials, flags);

    DicomRedactor redactor = configureRedactor(flags);

    List<PrivateTagConfig> privateTagConfigs = validateAndParsePrivateTags(flags.addPrivateTags);

    BackupUploadService backupUploadService = configureBackupUploadService(flags, credentials);

    IDestinationClientFactory destinationClientFactory;
    if (databaseConfigService != null) {
      // Use database-driven routing when database is configured
      log.info("Using DatabaseDestinationClientFactory for dynamic routing (HTTP/2={})", flags.useHttp2ForStow);
      destinationClientFactory = new com.google.cloud.healthcare.imaging.dicomadapter.cstore.destination.DatabaseDestinationClientFactory(
          databaseConfigService,
          defaultCstoreDicomWebClient,
          credentials,
          flags.useStowOverwrite,
          flags.useHttp2ForStow);
    } else {
      // Use static configuration when database is not configured
      destinationClientFactory = configureDestinationClientFactory(
          defaultCstoreDicomWebClient, credentials, flags, backupUploadService != null);
    }

    MultipleDestinationUploadService multipleDestinationSendService = configureMultipleDestinationUploadService(
        flags, cstoreSubAet, backupUploadService);

    CStoreService cStoreService =
        new CStoreService(destinationClientFactory, redactor, flags.transcodeToSyntax,
            multipleDestinationSendService, privateTagConfigs, databaseConfigService, sentryEnabled);
    serviceRegistry.addDicomService(cStoreService);

    // Handle C-FIND
    IDicomWebClient dicomWebClient =
        new DicomWebClient(requestFactory, dicomwebAddress, STUDIES);
    CFindService cFindService = new CFindService(dicomWebClient, flags);
    serviceRegistry.addDicomService(cFindService);

    // Handle C-MOVE
    CMoveSenderFactory cMoveSenderFactory = new CMoveSenderFactory(cstoreSubAet, dicomWebClient);
    AetDictionary aetDict = new AetDictionary(flags.aetDictionaryInline, flags.aetDictionaryPath);
    CMoveService cMoveService = new CMoveService(dicomWebClient, aetDict, cMoveSenderFactory);
    serviceRegistry.addDicomService(cMoveService);

    // Handle Storage Commitment N-ACTION
    serviceRegistry.addDicomService(new StorageCommitmentService(dicomWebClient, aetDict));

    // Start DICOM server
    Device device = DeviceUtil.createServerDevice(
        flags.dimseAET,
        flags.dimsePort,
        flags.dimseTlsPort,
        serviceRegistry,
        flags.tlsKeystore,
        flags.tlsKeystorePass,
        flags.tlsTruststore,
        flags.tlsTruststorePass,
        flags.tlsNeedClientAuth);
    device.bindConnections();
  }

  private static IDicomWebClient configureDefaultDicomWebClient(
      HttpRequestFactory requestFactory,
      String cstoreDicomwebAddr,
      String cstoreDicomwebStowPath,
      GoogleCredentials credentials,
      Flags flags) {
    IDicomWebClient defaultCstoreDicomWebClient;
    if (flags.useHttp2ForStow) {
      defaultCstoreDicomWebClient =
          new DicomWebClientJetty(
              credentials, StringUtil.joinPath(cstoreDicomwebAddr, cstoreDicomwebStowPath), flags.useStowOverwrite);
    } else {
      defaultCstoreDicomWebClient =
          new DicomWebClient(
              requestFactory, cstoreDicomwebAddr, cstoreDicomwebStowPath, flags.useStowOverwrite);
    }
    return defaultCstoreDicomWebClient;
  }

  private static IDestinationClientFactory configureDestinationClientFactory(
      IDicomWebClient defaultCstoreDicomWebClient,
      GoogleCredentials credentials,
      Flags flags, boolean backupServicePresent) throws IOException {
    IDestinationClientFactory destinationClientFactory;
    if (flags.sendToAllMatchingDestinations) {
      if (backupServicePresent == false) {
        throw new IllegalArgumentException(
            "backup is not configured properly. '--send_to_all_matching_destinations' flag must be"
                + " used only in pair with backup, local or GCP. Please see readme to configure"
                + " backup.");
      }
      Pair<ImmutableList<Pair<DestinationFilter, IDicomWebClient>>,
          ImmutableList<Pair<DestinationFilter, AetDictionary.Aet>>> multipleDestinations = configureMultipleDestinationTypesMap(
          flags.destinationConfigInline,
          flags.destinationConfigPath,
          DestinationsConfig.ENV_DESTINATION_CONFIG_JSON,
          credentials, flags.useStowOverwrite);

      destinationClientFactory = new MultipleDestinationClientFactory(
          multipleDestinations.getLeft(),
          multipleDestinations.getRight(),
          defaultCstoreDicomWebClient);
    } else { // with or without backup usage.
      if ((flags.destinationConfigPath != null || flags.destinationConfigInline != null)
          && flags.useStowOverwrite) {
        throw new IllegalArgumentException(
            "Must use '--send_to_all_matching_destinations' when using '--stow_overwrite' and"
                + " providing a destination config.");
      }
      destinationClientFactory = new SingleDestinationClientFactory(
          configureDestinationMap(
              flags.destinationConfigInline, flags.destinationConfigPath, credentials, flags.useStowOverwrite),
          defaultCstoreDicomWebClient);
    }
    return destinationClientFactory;
  }

  private static MultipleDestinationUploadService configureMultipleDestinationUploadService(
      Flags flags, String cstoreSubAet, BackupUploadService backupUploadService) {
    if (flags.autoAckCStore) {
      if (backupUploadService == null) {
        throw new IllegalArgumentException(
            "--auto_ack_cstore requires the use of --persistent_file_storage_location and"
                + " --persistent_file_upload_retry_amount >= 1");
      }
      log.warn(
          "Using flag --auto_ack_cstore will acknowledge DIMSE store requests before the"
              + " instances are persisted to the DICOMweb destination.");
    }
    if (backupUploadService != null) {
      return new MultipleDestinationUploadService(
          new CStoreSenderFactory(cstoreSubAet),
          backupUploadService,
          flags.persistentFileUploadRetryAmount,
          flags.autoAckCStore);
    }
    return null;
  }

  private static BackupUploadService configureBackupUploadService(
      Flags flags, GoogleCredentials credentials) throws IOException {
    String uploadPath = flags.persistentFileStorageLocation;
    if (flags.useStowOverwrite
        && (uploadPath.isBlank() || flags.persistentFileUploadRetryAmount < 1)) {
      throw new IllegalArgumentException(
          "--stow_overwrite requires the use of --persistent_file_storage_location and"
              + " --persistent_file_upload_retry_amount >= 1");
    }
    if (!uploadPath.isBlank()) {
      final IBackupUploader backupUploader;
      if (uploadPath.startsWith(GCP_PATH_PREFIX)) {
        backupUploader = new GcpBackupUploader(uploadPath, flags.gcsBackupProjectId, credentials);
      } else {
        backupUploader = new LocalBackupUploader(uploadPath);
      }
      return new BackupUploadService(
          backupUploader,
          flags.persistentFileUploadRetryAmount,
          ImmutableList.copyOf(flags.httpErrorCodesToRetry),
          new DelayCalculator(flags.minUploadDelay, flags.maxWaitingTimeBetweenUploads));
    }
    return null;
  }

  private static DicomRedactor configureRedactor(Flags flags) throws IOException{
    DicomRedactor redactor = null;
    int tagEditFlags = (flags.tagsToRemove.isEmpty() ? 0 : 1) +
        (flags.tagsToKeep.isEmpty() ? 0 : 1) +
        (flags.tagsProfile.isEmpty() ? 0 : 1);
    if (tagEditFlags > 1) {
      throw new IllegalArgumentException("Only one of 'redact' flags may be present");
    }
    if (tagEditFlags > 0) {
      DicomConfigProtos.DicomConfig.Builder configBuilder = DicomConfig.newBuilder();
      if (!flags.tagsToRemove.isEmpty()) {
        List<String> removeList = Arrays.asList(flags.tagsToRemove.split(","));
        configBuilder.setRemoveList(
            DicomConfig.TagFilterList.newBuilder().addAllTags(removeList));
      } else if (!flags.tagsToKeep.isEmpty()) {
        List<String> keepList = Arrays.asList(flags.tagsToKeep.split(","));
        configBuilder.setKeepList(
            DicomConfig.TagFilterList.newBuilder().addAllTags(keepList));
      } else if (!flags.tagsProfile.isEmpty()){
        configBuilder.setFilterProfile(TagFilterProfile.valueOf(flags.tagsProfile));
      }

      try {
        redactor = new DicomRedactor(configBuilder.build());
      } catch (Exception e) {
        throw new IOException("Failure creating DICOM redactor", e);
      }
    }

    return redactor;
  }

  private static final Set<String> SUPPORTED_VARIABLES = new HashSet<>(
      Arrays.asList("CALLING_AET", "CALLED_AET", "TIMESTAMP"));

  private static final Pattern TAG_PATTERN = Pattern.compile(
      "\\(([0-9A-Fa-f]{4}),([0-9A-Fa-f]{4})\\):([A-Z]{2}):(.*)");

  private static final Pattern VARIABLE_PATTERN = Pattern.compile("\\{([A-Z_]+)\\}");

  /**
   * Validates and parses private tag configurations from command line flags.
   *
   * @param addPrivateTags List of tag specifications in format (gggg,eeee):VR:value
   * @return List of validated PrivateTagConfig objects
   * @throws IllegalArgumentException if validation fails
   */
  private static List<PrivateTagConfig> validateAndParsePrivateTags(List<String> addPrivateTags) {
    List<PrivateTagConfig> configs = new ArrayList<>();

    if (addPrivateTags == null || addPrivateTags.isEmpty()) {
      return configs;
    }

    for (String tagSpec : addPrivateTags) {
      Matcher matcher = TAG_PATTERN.matcher(tagSpec);

      if (!matcher.matches()) {
        System.err.println("ERROR: Invalid private tag format: " + tagSpec);
        System.err.println("Expected format: (gggg,eeee):VR:value");
        System.err.println("Example: (0777,1000):SH:{CALLING_AET}");
        System.exit(1);
      }

      String groupHex = matcher.group(1);
      String elementHex = matcher.group(2);
      String vrString = matcher.group(3);
      String value = matcher.group(4);

      // Parse tag
      int group = Integer.parseInt(groupHex, 16);
      int element = Integer.parseInt(elementHex, 16);
      int tag = (group << 16) | element;

      // Validate group is odd (requirement for private tags)
      if (group % 2 == 0) {
        System.err.println("ERROR: Private tag group must be odd: " + tagSpec);
        System.err.println("Group " + groupHex + " is even. Private tags require odd groups.");
        System.exit(1);
      }

      // Validate VR
      VR vr;
      try {
        vr = VR.valueOf(vrString);
      } catch (IllegalArgumentException e) {
        System.err.println("ERROR: Invalid VR in private tag: " + tagSpec);
        System.err.println("VR '" + vrString + "' is not recognized.");
        System.exit(1);
        return null; // unreachable, but makes compiler happy
      }

      // Validate variables in value
      Matcher varMatcher = VARIABLE_PATTERN.matcher(value);
      while (varMatcher.find()) {
        String varName = varMatcher.group(1);
        if (!SUPPORTED_VARIABLES.contains(varName)) {
          System.err.println("ERROR: Unknown variable {" + varName + "} in private tag: " + tagSpec);
          System.err.println("Supported variables: {CALLING_AET}, {CALLED_AET}, {TIMESTAMP}");
          System.exit(1);
        }
      }

      configs.add(new PrivateTagConfig(tag, vr, value));
      log.info("Registered private tag: " + tagSpec);
    }

    return configs;
  }

  private static ImmutableList<Pair<DestinationFilter, IDicomWebClient>> configureDestinationMap(
    String destinationJsonInline,
    String destinationsJsonPath,
    GoogleCredentials credentials,
    Boolean useStowOverwrite) throws IOException {
  DestinationsConfig conf = new DestinationsConfig(destinationJsonInline, destinationsJsonPath);

  ImmutableList.Builder<Pair<DestinationFilter, IDicomWebClient>> filterPairBuilder = ImmutableList.builder();
  for (String filterString : conf.getMap().keySet()) {
    String filterPath = StringUtil.trim(conf.getMap().get(filterString));
    filterPairBuilder.add(
        new Pair(
            new DestinationFilter(filterString),
            new DicomWebClientJetty(credentials, filterPath.endsWith(STUDIES)? filterPath : StringUtil.joinPath(filterPath, STUDIES), useStowOverwrite)
    ));
  }
  ImmutableList resultList = filterPairBuilder.build();
  return resultList.size() > 0 ? resultList : null;
}

  public static Pair<ImmutableList<Pair<DestinationFilter, IDicomWebClient>>,
                     ImmutableList<Pair<DestinationFilter, AetDictionary.Aet>>> configureMultipleDestinationTypesMap(
      String destinationJsonInline,
      String jsonPath,
      String jsonEnvKey,
      GoogleCredentials credentials,
      Boolean useStowOverwrite) throws IOException {

    ImmutableList.Builder<Pair<DestinationFilter, AetDictionary.Aet>> dicomDestinationFiltersBuilder = ImmutableList.builder();
    ImmutableList.Builder<Pair<DestinationFilter, IDicomWebClient>> healthDestinationFiltersBuilder = ImmutableList.builder();

    JSONArray jsonArray = JsonUtil.parseConfig(destinationJsonInline, jsonPath, jsonEnvKey);

    if (jsonArray != null) {
      for (Object elem : jsonArray) {
        JSONObject elemJson = (JSONObject) elem;
        if (elemJson.has(FILTER) == false) {
          throw new IOException("Mandatory key absent: " + FILTER);
        }
        String filter = elemJson.getString(FILTER);
        DestinationFilter destinationFilter = new DestinationFilter(StringUtil.trim(filter));

        // try to create Aet instance
        if (elemJson.has("host")) {
          dicomDestinationFiltersBuilder.add(
              new Pair(destinationFilter,
                  new AetDictionary.Aet(elemJson.getString("name"),
                  elemJson.getString("host"), elemJson.getInt("port"))));
        } else {
          // in this case to try create IDicomWebClient instance
          String filterPath = elemJson.getString("dicomweb_destination");
          healthDestinationFiltersBuilder.add(
              new Pair(
                  destinationFilter,
                  new DicomWebClientJetty(credentials, filterPath.endsWith(STUDIES)? filterPath : StringUtil.joinPath(filterPath, STUDIES), useStowOverwrite)));
        }
      }
    }
    return new Pair(healthDestinationFiltersBuilder.build(), dicomDestinationFiltersBuilder.build());
  }

  /**
   * Configuration for a private DICOM tag to be added during C-STORE processing.
   */
  public static class PrivateTagConfig {
    private final int tag;
    private final VR vr;
    private final String valueTemplate;

    public PrivateTagConfig(int tag, VR vr, String valueTemplate) {
      this.tag = tag;
      this.vr = vr;
      this.valueTemplate = valueTemplate;
    }

    public int getTag() {
      return tag;
    }

    public VR getVr() {
      return vr;
    }

    public String getValueTemplate() {
      return valueTemplate;
    }
  }

  public static class Pair<A, D>{
    private final A left;
    private final D right;

    public Pair(A left, D right) {
      this.left = left;
      this.right = right;
    }

    public A getLeft() {
      return left;
    }

    public D getRight() {
      return right;
    }
  }
}
