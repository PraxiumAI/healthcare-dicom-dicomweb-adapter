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

package com.google.cloud.healthcare.imaging.dicomadapter;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.sentry.Sentry;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.UUID;
import org.dcm4che3.net.Status;
import org.dcm4che3.net.service.DicomServiceException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Service for managing database connections and queries for DICOM adapter dynamic configuration.
 * Handles AET authorization, storage routing, and study-to-storage mapping.
 */
public class DatabaseConfigService {

  private static final Logger log = LoggerFactory.getLogger(DatabaseConfigService.class);

  private final HikariDataSource dataSource;
  private final boolean sentryEnabled;

  // SQL queries
  private static final String SQL_CHECK_AUTHORIZATION =
      "SELECT id FROM dicom_device WHERE calling_aet = ? AND called_aet = ? LIMIT 1";

  private static final String SQL_GET_STORAGE_FOR_STUDY =
      "SELECT dest.dicomweb_destination FROM dicom_study_destination sd "
          + "JOIN dicom_destination dest ON sd.dicomweb_destination_id = dest.id "
          + "WHERE sd.study_uid = ? LIMIT 1";

  private static final String SQL_GET_STORAGE_FOR_AET =
      "SELECT dest.dicomweb_destination FROM dicom_device_destination dd "
          + "JOIN dicom_device dev ON dd.device_id = dev.id "
          + "JOIN dicom_destination dest ON dd.dicomweb_destination_id = dest.id "
          + "WHERE dev.calling_aet = ? LIMIT 1";

  private static final String SQL_MAP_STUDY_TO_STORAGE =
      "INSERT INTO dicom_study_destination (study_uid, dicomweb_destination_id) "
          + "SELECT ?, id FROM dicom_destination WHERE dicomweb_destination = ? "
          + "ON CONFLICT (study_uid) DO UPDATE "
          + "SET dicomweb_destination_id = (SELECT id FROM dicom_destination WHERE dicomweb_destination = EXCLUDED.dicomweb_destination)";

  /**
   * Creates a new DatabaseConfigService with the specified configuration.
   *
   * @param dbUrl JDBC URL for PostgreSQL database
   * @param dbUser Database username
   * @param dbPassword Database password
   * @param sentryEnabled Whether Sentry error reporting is enabled
   * @throws SQLException if initial database connection fails
   */
  public DatabaseConfigService(String dbUrl, String dbUser, String dbPassword, boolean sentryEnabled)
      throws SQLException {
    this.sentryEnabled = sentryEnabled;

    log.info("Initializing DatabaseConfigService with connection pool");

    HikariConfig config = new HikariConfig();
    config.setJdbcUrl(dbUrl);
    config.setUsername(dbUser);
    config.setPassword(dbPassword);

    // Connection pool configuration
    config.setMaximumPoolSize(10);
    config.setMinimumIdle(2);
    config.setConnectionTimeout(30000); // 30 seconds
    config.setIdleTimeout(600000); // 10 minutes
    config.setMaxLifetime(1800000); // 30 minutes

    // Connection validation
    config.setConnectionTestQuery("SELECT 1");
    config.setValidationTimeout(5000);

    // Pool name for monitoring
    config.setPoolName("DicomAdapterPool");

    try {
      this.dataSource = new HikariDataSource(config);

      // Test initial connection
      try (Connection conn = dataSource.getConnection()) {
        log.info("Database connection pool initialized successfully");
      }
    } catch (SQLException e) {
      log.error("Failed to initialize database connection pool", e);
      if (sentryEnabled) {
        Sentry.captureException(e);
      }
      throw e;
    }
  }

  /**
   * Package-private constructor for testing with mocked DataSource.
   * Allows unit tests to inject mock HikariDataSource without creating real database connections.
   *
   * @param dataSource Mocked HikariDataSource
   * @param sentryEnabled Whether Sentry error reporting is enabled
   */
  DatabaseConfigService(HikariDataSource dataSource, boolean sentryEnabled) {
    this.dataSource = dataSource;
    this.sentryEnabled = sentryEnabled;
  }

  /**
   * Returns the authorized device UUID for a given calling AET and called AET pair.
   *
   * @param callingAET The calling Application Entity Title
   * @param calledAET The called Application Entity Title
   * @return UUID of the authorized device
   * @throws DicomServiceException if unauthorized or on database error
   */
  public UUID getAuthorization(String callingAET, String calledAET) throws DicomServiceException {
    try (Connection conn = dataSource.getConnection();
        PreparedStatement stmt = conn.prepareStatement(SQL_CHECK_AUTHORIZATION)) {

      stmt.setString(1, callingAET);
      stmt.setString(2, calledAET);

      try (ResultSet rs = stmt.executeQuery()) {
        if (rs.next()) {
          String deviceIdStr = rs.getString("id");
          UUID deviceId = UUID.fromString(deviceIdStr);
          log.debug("Authorization successful for calling_aet={}, called_aet={}, device_id={}",
              callingAET, calledAET, deviceId);
          return deviceId;
        }
        log.warn("Authorization denied for calling_aet={}, called_aet={}", callingAET, calledAET);
        throw new DicomServiceException(Status.NotAuthorized,
            "Not authorized: calling_aet=" + callingAET + ", called_aet=" + calledAET);
      }
    } catch (SQLException e) {
      log.error("Database error during authorization check for calling_aet={}, called_aet={}",
          callingAET, calledAET, e);
      if (sentryEnabled) {
        Sentry.captureException(e);
      }
      throw new DicomServiceException(Status.ProcessingFailure, e);
    }
  }

  /**
   * Retrieves the DICOMweb destination for a given study UID.
   *
   * @param studyUID The Study Instance UID
   * @return The DICOMweb destination URL, or null if not found
   * @throws SQLException if database query fails
   */
  public String getStorageForStudy(String studyUID) throws SQLException {
    try (Connection conn = dataSource.getConnection();
        PreparedStatement stmt = conn.prepareStatement(SQL_GET_STORAGE_FOR_STUDY)) {

      stmt.setString(1, studyUID);

      try (ResultSet rs = stmt.executeQuery()) {
        if (rs.next()) {
          String destination = rs.getString("dicomweb_destination");
          log.debug("Found storage for study_uid={}: {}", studyUID, destination);
          return destination;
        }
        log.debug("No storage mapping found for study_uid={}", studyUID);
        return null;
      }
    } catch (SQLException e) {
      log.error("Database error retrieving storage for study_uid={}", studyUID, e);
      if (sentryEnabled) {
        Sentry.captureException(e);
      }
      throw e;
    }
  }

  /**
   * Retrieves the DICOMweb destination for a given AET.
   *
   * @param aet The Application Entity Title
   * @return The DICOMweb destination URL, or null if not found
   * @throws SQLException if database query fails
   */
  public String getStorageForAet(String aet) throws SQLException {
    try (Connection conn = dataSource.getConnection();
        PreparedStatement stmt = conn.prepareStatement(SQL_GET_STORAGE_FOR_AET)) {

      stmt.setString(1, aet);

      try (ResultSet rs = stmt.executeQuery()) {
        if (rs.next()) {
          String destination = rs.getString("dicomweb_destination");
          log.debug("Found storage for aet={}: {}", aet, destination);
          return destination;
        }
        log.debug("No storage mapping found for aet={}", aet);
        return null;
      }
    } catch (SQLException e) {
      log.error("Database error retrieving storage for aet={}", aet, e);
      if (sentryEnabled) {
        Sentry.captureException(e);
      }
      throw e;
    }
  }

  /**
   * Maps a study UID to a DICOMweb destination storage.
   * Uses PostgreSQL's ON CONFLICT to ensure atomic, conflict-free writes.
   *
   * @param studyUID The Study Instance UID
   * @param destinationUrl The DICOMweb destination URL
   * @throws SQLException if database operation fails
   */
  public void mapStudyToStorage(String studyUID, String destinationUrl) throws SQLException {
    try (Connection conn = dataSource.getConnection();
        PreparedStatement stmt = conn.prepareStatement(SQL_MAP_STUDY_TO_STORAGE)) {

      stmt.setString(1, studyUID);
      stmt.setString(2, destinationUrl);

      int rowsAffected = stmt.executeUpdate();
      log.info("Mapped study_uid={} to destination={} (rows affected: {})",
          studyUID, destinationUrl, rowsAffected);

    } catch (SQLException e) {
      log.error("Database error mapping study_uid={} to destination={}",
          studyUID, destinationUrl, e);
      if (sentryEnabled) {
        Sentry.captureException(e);
      }
      throw e;
    }
  }

  /**
   * Checks if the database connection is healthy.
   *
   * @return true if connection is healthy, false otherwise
   */
  public boolean isHealthy() {
    try (Connection conn = dataSource.getConnection();
        PreparedStatement stmt = conn.prepareStatement("SELECT 1")) {
      stmt.executeQuery();
      return true;
    } catch (SQLException e) {
      log.warn("Database health check failed", e);
      return false;
    }
  }

  /**
   * Closes the database connection pool.
   * Should be called during application shutdown.
   */
  public void close() {
    if (dataSource != null && !dataSource.isClosed()) {
      log.info("Closing database connection pool");
      dataSource.close();
    }
  }
}
