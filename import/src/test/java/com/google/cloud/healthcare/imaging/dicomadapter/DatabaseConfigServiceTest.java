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
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/**
 * Unit tests for DatabaseConfigService using mocked database connections.
 */
public class DatabaseConfigServiceTest {

  @Mock
  private HikariDataSource mockDataSource;

  @Mock
  private Connection mockConnection;

  @Mock
  private PreparedStatement mockStatement;

  @Mock
  private ResultSet mockResultSet;

  private DatabaseConfigService service;

  @Before
  public void setUp() throws SQLException {
    MockitoAnnotations.initMocks(this);

    // Mock basic connection behavior
    when(mockDataSource.getConnection()).thenReturn(mockConnection);
    when(mockConnection.prepareStatement(anyString())).thenReturn(mockStatement);
    when(mockStatement.executeQuery()).thenReturn(mockResultSet);
  }

  @Test
  public void testIsAuthorized_AuthorizedPair_ReturnsTrue() throws SQLException {
    // Arrange
    when(mockResultSet.next()).thenReturn(true); // Authorization exists

    service = createServiceWithMockedDataSource();

    // Act
    boolean result = service.isAuthorized("HOSPITAL_A", "PRAXIUM");

    // Assert
    assertTrue("Should return true for authorized AET pair", result);
    verify(mockStatement).setString(1, "HOSPITAL_A");
    verify(mockStatement).setString(2, "PRAXIUM");
  }

  @Test
  public void testIsAuthorized_UnauthorizedPair_ReturnsFalse() throws SQLException {
    // Arrange
    when(mockResultSet.next()).thenReturn(false); // No authorization

    service = createServiceWithMockedDataSource();

    // Act
    boolean result = service.isAuthorized("UNKNOWN_AET", "PRAXIUM");

    // Assert
    assertFalse("Should return false for unauthorized AET pair", result);
    verify(mockStatement).setString(1, "UNKNOWN_AET");
    verify(mockStatement).setString(2, "PRAXIUM");
  }

  @Test
  public void testIsAuthorized_DatabaseError_ThrowsSQLException() throws SQLException {
    // Arrange
    when(mockStatement.executeQuery()).thenThrow(new SQLException("Connection timeout"));

    service = createServiceWithMockedDataSource();

    // Act & Assert
    try {
      service.isAuthorized("HOSPITAL_A", "PRAXIUM");
      fail("Should throw SQLException on database error");
    } catch (SQLException e) {
      assertEquals("Connection timeout", e.getMessage());
    }
  }

  @Test
  public void testGetStorageForStudy_ExistingStudy_ReturnsDestination() throws SQLException {
    // Arrange
    String expectedDestination = "https://healthcare.googleapis.com/v1/projects/test/dicomWeb";
    when(mockResultSet.next()).thenReturn(true);
    when(mockResultSet.getString("dicomweb_destination")).thenReturn(expectedDestination);

    service = createServiceWithMockedDataSource();

    // Act
    String result = service.getStorageForStudy("1.2.3.4.5.TEST");

    // Assert
    assertEquals("Should return correct destination", expectedDestination, result);
    verify(mockStatement).setString(1, "1.2.3.4.5.TEST");
  }

  @Test
  public void testGetStorageForStudy_NonExistingStudy_ReturnsNull() throws SQLException {
    // Arrange
    when(mockResultSet.next()).thenReturn(false);

    service = createServiceWithMockedDataSource();

    // Act
    String result = service.getStorageForStudy("1.2.3.NONEXISTENT");

    // Assert
    assertNull("Should return null for non-existing study", result);
  }

  @Test
  public void testGetStorageForStudy_DatabaseError_ThrowsSQLException() throws SQLException {
    // Arrange
    when(mockStatement.executeQuery()).thenThrow(new SQLException("Query timeout"));

    service = createServiceWithMockedDataSource();

    // Act & Assert
    try {
      service.getStorageForStudy("1.2.3.4.5.TEST");
      fail("Should throw SQLException on database error");
    } catch (SQLException e) {
      assertEquals("Query timeout", e.getMessage());
    }
  }

  @Test
  public void testGetStorageForAet_ExistingAET_ReturnsDestination() throws SQLException {
    // Arrange
    String expectedDestination = "https://healthcare.googleapis.com/v1/projects/hospital-a/dicomWeb";
    when(mockResultSet.next()).thenReturn(true);
    when(mockResultSet.getString("dicomweb_destination")).thenReturn(expectedDestination);

    service = createServiceWithMockedDataSource();

    // Act
    String result = service.getStorageForAet("HOSPITAL_A");

    // Assert
    assertEquals("Should return correct destination", expectedDestination, result);
    verify(mockStatement).setString(1, "HOSPITAL_A");
  }

  @Test
  public void testGetStorageForAet_NonExistingAET_ReturnsNull() throws SQLException {
    // Arrange
    when(mockResultSet.next()).thenReturn(false);

    service = createServiceWithMockedDataSource();

    // Act
    String result = service.getStorageForAet("NONEXISTENT_AET");

    // Assert
    assertNull("Should return null for non-existing AET", result);
  }

  @Test
  public void testMapStudyToStorage_Success_ExecutesUpdate() throws SQLException {
    // Arrange
    when(mockStatement.executeUpdate()).thenReturn(1);

    service = createServiceWithMockedDataSource();

    // Act
    service.mapStudyToStorage("1.2.3.4.5.TEST", "https://destination/dicomWeb");

    // Assert
    verify(mockStatement).setString(1, "1.2.3.4.5.TEST");
    verify(mockStatement).setString(2, "https://destination/dicomWeb");
    verify(mockStatement).executeUpdate();
  }

  @Test
  public void testMapStudyToStorage_DatabaseError_ThrowsSQLException() throws SQLException {
    // Arrange
    when(mockStatement.executeUpdate()).thenThrow(new SQLException("Insert failed"));

    service = createServiceWithMockedDataSource();

    // Act & Assert
    try {
      service.mapStudyToStorage("1.2.3.4.5.TEST", "https://destination/dicomWeb");
      fail("Should throw SQLException on database error");
    } catch (SQLException e) {
      assertEquals("Insert failed", e.getMessage());
    }
  }

  @Test
  public void testIsHealthy_HealthyConnection_ReturnsTrue() throws SQLException {
    // Arrange
    when(mockResultSet.next()).thenReturn(true);

    service = createServiceWithMockedDataSource();

    // Act
    boolean result = service.isHealthy();

    // Assert
    assertTrue("Should return true for healthy connection", result);
  }

  @Test
  public void testIsHealthy_UnhealthyConnection_ReturnsFalse() throws SQLException {
    // Arrange
    when(mockDataSource.getConnection()).thenThrow(new SQLException("Connection failed"));

    service = createServiceWithMockedDataSource();

    // Act
    boolean result = service.isHealthy();

    // Assert
    assertFalse("Should return false for unhealthy connection", result);
  }

  @Test
  public void testClose_ClosesDataSource() {
    // Arrange
    when(mockDataSource.isClosed()).thenReturn(false);
    service = createServiceWithMockedDataSource();

    // Act
    service.close();

    // Assert
    verify(mockDataSource).close();
  }

  @Test
  public void testClose_AlreadyClosed_DoesNothing() {
    // Arrange
    when(mockDataSource.isClosed()).thenReturn(true);
    service = createServiceWithMockedDataSource();

    // Act
    service.close();

    // Assert
    verify(mockDataSource, never()).close();
  }

  /**
   * Helper method to create DatabaseConfigService with mocked DataSource
   * Uses reflection to inject mock since constructor creates real HikariDataSource
   */
  private DatabaseConfigService createServiceWithMockedDataSource() {
    DatabaseConfigService service = new DatabaseConfigService(mockDataSource, false);
    return service;
  }

  // Additional constructor for DatabaseConfigService to support mocking
  // This would need to be added to DatabaseConfigService.java:
  /**
   * Package-private constructor for testing with mocked DataSource.
   *
   * DatabaseConfigService(HikariDataSource dataSource, boolean sentryEnabled) {
   *   this.dataSource = dataSource;
   *   this.sentryEnabled = sentryEnabled;
   * }
   */
}
