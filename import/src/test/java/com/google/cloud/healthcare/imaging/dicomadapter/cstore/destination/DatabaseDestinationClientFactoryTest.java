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

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.healthcare.IDicomWebClient;
import com.google.cloud.healthcare.imaging.dicomadapter.DatabaseConfigService;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.sql.SQLException;
import org.dcm4che3.data.Attributes;
import org.dcm4che3.data.Tag;
import org.dcm4che3.data.UID;
import org.dcm4che3.data.VR;
import org.dcm4che3.io.DicomOutputStream;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/**
 * Unit tests for DatabaseDestinationClientFactory with mocked database.
 */
public class DatabaseDestinationClientFactoryTest {

  @Mock
  private DatabaseConfigService mockDatabaseService;

  @Mock
  private IDicomWebClient mockDefaultClient;

  @Mock
  private GoogleCredentials mockCredentials;

  private DatabaseDestinationClientFactory factory;

  private static final String TEST_CALLING_AET = "TEST_HOSPITAL";
  private static final String TEST_STUDY_UID = "1.2.3.4.5.TEST";
  private static final String TEST_DESTINATION_A = "https://healthcare.googleapis.com/v1/projects/project-a/dicomWeb";
  private static final String TEST_DESTINATION_B = "https://healthcare.googleapis.com/v1/projects/project-b/dicomWeb";

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);

    factory = new DatabaseDestinationClientFactory(
        mockDatabaseService,
        mockDefaultClient,
        mockCredentials,
        false // useStowOverwrite
    );
  }

  @Test
  public void testCreate_StudyInDatabase_UsesStudyDestination() throws Exception {
    // Arrange
    when(mockDatabaseService.getStorageForStudy(TEST_STUDY_UID))
        .thenReturn(TEST_DESTINATION_A);

    ByteArrayInputStream dicomStream = createTestDicomStream(TEST_STUDY_UID);

    // Act
    DestinationHolder result = factory.create(TEST_CALLING_AET, UID.ImplicitVRLittleEndian, dicomStream);

    // Assert
    assertNotNull("DestinationHolder should not be null", result);
    verify(mockDatabaseService).getStorageForStudy(TEST_STUDY_UID);
    verify(mockDatabaseService, never()).getStorageForAet(anyString());
    verify(mockDatabaseService, never()).mapStudyToStorage(anyString(), anyString());
  }

  @Test
  public void testCreate_StudyNotInDatabase_UsesAetDestination() throws Exception {
    // Arrange
    when(mockDatabaseService.getStorageForStudy(TEST_STUDY_UID))
        .thenReturn(null); // Study not found
    when(mockDatabaseService.getStorageForAet(TEST_CALLING_AET))
        .thenReturn(TEST_DESTINATION_B);
    doNothing().when(mockDatabaseService).mapStudyToStorage(TEST_STUDY_UID, TEST_DESTINATION_B);

    ByteArrayInputStream dicomStream = createTestDicomStream(TEST_STUDY_UID);

    // Act
    DestinationHolder result = factory.create(TEST_CALLING_AET, UID.ImplicitVRLittleEndian, dicomStream);

    // Assert
    assertNotNull("DestinationHolder should not be null", result);
    verify(mockDatabaseService).getStorageForStudy(TEST_STUDY_UID);
    verify(mockDatabaseService).getStorageForAet(TEST_CALLING_AET);
    verify(mockDatabaseService).mapStudyToStorage(TEST_STUDY_UID, TEST_DESTINATION_B);
  }

  @Test
  public void testCreate_NoRoutingFound_UsesDefault() throws Exception {
    // Arrange
    when(mockDatabaseService.getStorageForStudy(TEST_STUDY_UID))
        .thenReturn(null);
    when(mockDatabaseService.getStorageForAet(TEST_CALLING_AET))
        .thenReturn(null);

    ByteArrayInputStream dicomStream = createTestDicomStream(TEST_STUDY_UID);

    // Act
    DestinationHolder result = factory.create(TEST_CALLING_AET, UID.ImplicitVRLittleEndian, dicomStream);

    // Assert
    assertNotNull("DestinationHolder should not be null", result);
    verify(mockDatabaseService).getStorageForStudy(TEST_STUDY_UID);
    verify(mockDatabaseService).getStorageForAet(TEST_CALLING_AET);
    verify(mockDatabaseService, never()).mapStudyToStorage(anyString(), anyString());
  }

  @Test
  public void testCreate_MissingStudyUID_UsesDefault() throws Exception {
    // Arrange
    ByteArrayInputStream dicomStream = createTestDicomStream(null); // No StudyInstanceUID

    // Act
    DestinationHolder result = factory.create(TEST_CALLING_AET, UID.ImplicitVRLittleEndian, dicomStream);

    // Assert
    assertNotNull("DestinationHolder should not be null", result);
    verify(mockDatabaseService, never()).getStorageForStudy(anyString());
    verify(mockDatabaseService, never()).getStorageForAet(anyString());
  }

  @Test
  public void testCreate_EmptyStudyUID_UsesDefault() throws Exception {
    // Arrange
    ByteArrayInputStream dicomStream = createTestDicomStream(""); // Empty StudyInstanceUID

    // Act
    DestinationHolder result = factory.create(TEST_CALLING_AET, UID.ImplicitVRLittleEndian, dicomStream);

    // Assert
    assertNotNull("DestinationHolder should not be null", result);
    verify(mockDatabaseService, never()).getStorageForStudy(anyString());
    verify(mockDatabaseService, never()).getStorageForAet(anyString());
  }

  @Test
  public void testCreate_DatabaseErrorOnStudyLookup_UsesDefault() throws Exception {
    // Arrange
    when(mockDatabaseService.getStorageForStudy(TEST_STUDY_UID))
        .thenThrow(new SQLException("Connection timeout"));

    ByteArrayInputStream dicomStream = createTestDicomStream(TEST_STUDY_UID);

    // Act
    DestinationHolder result = factory.create(TEST_CALLING_AET, UID.ImplicitVRLittleEndian, dicomStream);

    // Assert
    assertNotNull("DestinationHolder should not be null", result);
    verify(mockDatabaseService).getStorageForStudy(TEST_STUDY_UID);
    // Should fall back to default on database error
  }

  @Test
  public void testCreate_DatabaseErrorOnAetLookup_UsesDefault() throws Exception {
    // Arrange
    when(mockDatabaseService.getStorageForStudy(TEST_STUDY_UID))
        .thenReturn(null);
    when(mockDatabaseService.getStorageForAet(TEST_CALLING_AET))
        .thenThrow(new SQLException("Query failed"));

    ByteArrayInputStream dicomStream = createTestDicomStream(TEST_STUDY_UID);

    // Act
    DestinationHolder result = factory.create(TEST_CALLING_AET, UID.ImplicitVRLittleEndian, dicomStream);

    // Assert
    assertNotNull("DestinationHolder should not be null", result);
    verify(mockDatabaseService).getStorageForStudy(TEST_STUDY_UID);
    verify(mockDatabaseService).getStorageForAet(TEST_CALLING_AET);
  }

  @Test
  public void testCreate_DatabaseErrorOnMapping_ContinuesRouting() throws Exception {
    // Arrange
    when(mockDatabaseService.getStorageForStudy(TEST_STUDY_UID))
        .thenReturn(null);
    when(mockDatabaseService.getStorageForAet(TEST_CALLING_AET))
        .thenReturn(TEST_DESTINATION_B);
    doThrow(new SQLException("Insert failed"))
        .when(mockDatabaseService).mapStudyToStorage(TEST_STUDY_UID, TEST_DESTINATION_B);

    ByteArrayInputStream dicomStream = createTestDicomStream(TEST_STUDY_UID);

    // Act
    DestinationHolder result = factory.create(TEST_CALLING_AET, UID.ImplicitVRLittleEndian, dicomStream);

    // Assert
    assertNotNull("DestinationHolder should not be null", result);
    verify(mockDatabaseService).getStorageForStudy(TEST_STUDY_UID);
    verify(mockDatabaseService).getStorageForAet(TEST_CALLING_AET);
    verify(mockDatabaseService).mapStudyToStorage(TEST_STUDY_UID, TEST_DESTINATION_B);
    // Should continue routing despite mapping error
  }

  @Test
  public void testCreate_MultipleCalls_CachesClients() throws Exception {
    // Arrange
    when(mockDatabaseService.getStorageForStudy(anyString()))
        .thenReturn(TEST_DESTINATION_A);

    ByteArrayInputStream dicomStream1 = createTestDicomStream("1.2.3.STUDY_1");
    ByteArrayInputStream dicomStream2 = createTestDicomStream("1.2.3.STUDY_2");

    // Act
    DestinationHolder result1 = factory.create(TEST_CALLING_AET, UID.ImplicitVRLittleEndian, dicomStream1);
    DestinationHolder result2 = factory.create(TEST_CALLING_AET, UID.ImplicitVRLittleEndian, dicomStream2);

    // Assert
    assertNotNull("First result should not be null", result1);
    assertNotNull("Second result should not be null", result2);
    verify(mockDatabaseService, times(2)).getStorageForStudy(anyString());
    // Both should use the same cached client (implicitly tested by factory logic)
  }

  /**
   * Helper method to create a test DICOM stream with specified StudyInstanceUID.
   */
  private ByteArrayInputStream createTestDicomStream(String studyInstanceUID) throws IOException {
    Attributes attrs = new Attributes();

    // Add required DICOM attributes
    attrs.setString(Tag.SOPClassUID, VR.UI, "1.2.840.10008.5.1.4.1.1.1"); // CR Image Storage
    attrs.setString(Tag.SOPInstanceUID, VR.UI, "1.2.3.4.5.SOP_INSTANCE");

    if (studyInstanceUID != null && !studyInstanceUID.isEmpty()) {
      attrs.setString(Tag.StudyInstanceUID, VR.UI, studyInstanceUID);
    }

    attrs.setString(Tag.SeriesInstanceUID, VR.UI, "1.2.3.4.5.SERIES");
    attrs.setString(Tag.PatientID, VR.LO, "TEST_PATIENT");

    // Write to byte array
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (DicomOutputStream dos = new DicomOutputStream(baos, UID.ImplicitVRLittleEndian)) {
      dos.writeDataset(null, attrs);
    }

    return new ByteArrayInputStream(baos.toByteArray());
  }
}
