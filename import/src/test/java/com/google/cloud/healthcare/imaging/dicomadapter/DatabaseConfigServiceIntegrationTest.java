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

import static org.junit.Assert.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import org.dcm4che3.net.Status;
import org.dcm4che3.net.service.DicomServiceException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Integration tests for DatabaseConfigService using local PostgreSQL database.
 *
 * Prerequisites:
 * - PostgreSQL running locally
 * - Database: praxium_dev
 * - User: praxium / Password: praxium
 * - Execute test-db-setup.sql before running tests
 */
public class DatabaseConfigServiceIntegrationTest {

  private static final String DB_URL = "jdbc:postgresql://localhost:5432/praxium_dev";
  private static final String DB_USER = "praxium";
  private static final String DB_PASSWORD = "praxium";

  private DatabaseConfigService databaseConfigService;

  @Before
  public void setUp() throws SQLException {
    // Initialize DatabaseConfigService with test database
    databaseConfigService = new DatabaseConfigService(
        DB_URL,
        DB_USER,
        DB_PASSWORD,
        false); // Sentry disabled for tests
  }

  @After
  public void tearDown() {
    if (databaseConfigService != null) {
      databaseConfigService.close();
    }
  }

  @Test
  public void testDatabaseConnectionSucceeds() {
    assertTrue("Database should be healthy", databaseConfigService.isHealthy());
  }

  @Test
  public void testGetAuthorization_AuthorizedPair_ReturnsUuid() throws org.dcm4che3.net.service.DicomServiceException {
    assertNotNull(databaseConfigService.getAuthorization("TEST_HOSPITAL_A", "TEST_PRAXIUM"));
  }

  @Test
  public void testGetAuthorization_UnauthorizedPair_ThrowsNotAuthorized() {
    try {
      databaseConfigService.getAuthorization("UNKNOWN_AET", "TEST_PRAXIUM");
      fail("Should throw NotAuthorized");
    } catch (DicomServiceException e) {
      assertEquals(Status.NotAuthorized, e.getStatus());
    }
  }

  @Test
  public void testGetAuthorization_WrongCalledAET_ThrowsNotAuthorized() {
    try {
      databaseConfigService.getAuthorization("UNAUTHORIZED_AET", "TEST_PRAXIUM");
      fail("Should throw NotAuthorized");
    } catch (DicomServiceException e) {
      assertEquals(Status.NotAuthorized, e.getStatus());
    }
  }

  @Test
  public void testGetStorageForStudy_ExistingStudy_ReturnsDestination() throws SQLException {
    String destination = databaseConfigService.getStorageForStudy("1.2.3.4.5.6.7.8.9.TEST_STUDY_1");
    assertNotNull("Should return destination for existing study", destination);
    assertTrue("Destination should contain test-project-a",
        destination.contains("test-project-a"));
  }

  @Test
  public void testGetStorageForStudy_NonExistingStudy_ReturnsNull() throws SQLException {
    String destination = databaseConfigService.getStorageForStudy("1.2.3.NONEXISTENT");
    assertNull("Should return null for non-existing study", destination);
  }

  @Test
  public void testGetStorageForAet_ExistingAET_ReturnsDestination() throws SQLException {
    String destination = databaseConfigService.getStorageForAet("TEST_HOSPITAL_A");
    assertNotNull("Should return destination for existing AET", destination);
    assertTrue("Destination should contain test-project-a",
        destination.contains("test-project-a"));
  }

  @Test
  public void testGetStorageForAet_NonExistingAET_ReturnsNull() throws SQLException {
    String destination = databaseConfigService.getStorageForAet("NONEXISTENT_AET");
    assertNull("Should return null for non-existing AET", destination);
  }

  @Test
  public void testMapStudyToStorage_NewStudy_InsertsSuccessfully() throws SQLException {
    String studyUID = "1.2.3.4.5.TEST_NEW_STUDY_" + System.currentTimeMillis();
    String destination = "https://healthcare.googleapis.com/v1/projects/test/dicomWeb";

    databaseConfigService.mapStudyToStorage(studyUID, destination);

    String retrievedDestination = databaseConfigService.getStorageForStudy(studyUID);
    assertEquals("Retrieved destination should match inserted", destination, retrievedDestination);

    // Cleanup
    cleanupTestStudy(studyUID);
  }

  @Test
  public void testMapStudyToStorage_ExistingStudy_UpdatesSuccessfully() throws SQLException {
    String studyUID = "1.2.3.4.5.TEST_UPDATE_STUDY_" + System.currentTimeMillis();
    String destination1 = "https://healthcare.googleapis.com/v1/projects/test1/dicomWeb";
    String destination2 = "https://healthcare.googleapis.com/v1/projects/test2/dicomWeb";

    // Insert first time
    databaseConfigService.mapStudyToStorage(studyUID, destination1);
    String retrieved1 = databaseConfigService.getStorageForStudy(studyUID);
    assertEquals("First destination should match", destination1, retrieved1);

    // Update with conflict handling
    databaseConfigService.mapStudyToStorage(studyUID, destination2);
    String retrieved2 = databaseConfigService.getStorageForStudy(studyUID);
    assertEquals("Second destination should update", destination2, retrieved2);

    // Cleanup
    cleanupTestStudy(studyUID);
  }

  @Test
  public void testMapStudyToStorage_ConcurrentInserts_HandlesConflict() throws Exception {
    String studyUID = "1.2.3.4.5.TEST_CONCURRENT_" + System.currentTimeMillis();
    String destination1 = "https://healthcare.googleapis.com/v1/projects/test1/dicomWeb";
    String destination2 = "https://healthcare.googleapis.com/v1/projects/test2/dicomWeb";

    // Simulate concurrent inserts
    Thread thread1 = new Thread(() -> {
      try {
        DatabaseConfigService service1 = new DatabaseConfigService(DB_URL, DB_USER, DB_PASSWORD, false);
        service1.mapStudyToStorage(studyUID, destination1);
        service1.close();
      } catch (SQLException e) {
        fail("Thread 1 should not throw exception: " + e.getMessage());
      }
    });

    Thread thread2 = new Thread(() -> {
      try {
        DatabaseConfigService service2 = new DatabaseConfigService(DB_URL, DB_USER, DB_PASSWORD, false);
        service2.mapStudyToStorage(studyUID, destination2);
        service2.close();
      } catch (SQLException e) {
        fail("Thread 2 should not throw exception: " + e.getMessage());
      }
    });

    thread1.start();
    thread2.start();
    thread1.join();
    thread2.join();

    // One of the destinations should be persisted
    String finalDestination = databaseConfigService.getStorageForStudy(studyUID);
    assertNotNull("Destination should be persisted despite concurrent inserts", finalDestination);
    assertTrue("Destination should be one of the two",
        finalDestination.equals(destination1) || finalDestination.equals(destination2));

    // Cleanup
    cleanupTestStudy(studyUID);
  }

  @Test
  public void testHealthCheck_DatabaseAvailable_ReturnsTrue() {
    assertTrue("Health check should pass when database is available",
        databaseConfigService.isHealthy());
  }

  @Test
  public void testMultipleConnections_WithinPoolLimit_Succeeds() throws org.dcm4che3.net.service.DicomServiceException {
    // Test that connection pool can handle multiple operations
    for (int i = 0; i < 5; i++) {
      assertNotNull("Authorization should succeed on iteration " + i,
          databaseConfigService.getAuthorization("TEST_HOSPITAL_A", "TEST_PRAXIUM"));
    }
  }

  /**
   * Helper method to cleanup test studies from database
   */
  private void cleanupTestStudy(String studyUID) {
    try (Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
         Statement stmt = conn.createStatement()) {
      stmt.executeUpdate("DELETE FROM dicom_study_destination WHERE study_uid = '" + studyUID + "'");
    } catch (SQLException e) {
      System.err.println("Warning: Failed to cleanup test study " + studyUID + ": " + e.getMessage());
    }
  }

  /**
   * Test that verifies schema exists
   */
  @Test
  public void testDatabaseSchema_TablesExist() throws SQLException {
    try (Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
         Statement stmt = conn.createStatement()) {

      // Check dicom_device table exists (contains authorization records)
      stmt.executeQuery("SELECT 1 FROM dicom_device LIMIT 1");

      // Check dicom_device_destination table exists
      stmt.executeQuery("SELECT 1 FROM dicom_device_destination LIMIT 1");

      // Check dicom_study_destination table exists
      stmt.executeQuery("SELECT 1 FROM dicom_study_destination LIMIT 1");
    }
  }

  /**
   * Test authorization with multiple AETs
   */
  @Test
  public void testIsAuthorized_MultipleAETs_AllWork() throws SQLException, org.dcm4che3.net.service.DicomServiceException {
    assertNotNull(databaseConfigService.getAuthorization("TEST_HOSPITAL_A", "TEST_PRAXIUM"));
    assertNotNull(databaseConfigService.getAuthorization("TEST_HOSPITAL_B", "TEST_PRAXIUM"));
    assertNotNull(databaseConfigService.getAuthorization("TEST_MODALITY_01", "TEST_PRAXIUM"));
  }
}
