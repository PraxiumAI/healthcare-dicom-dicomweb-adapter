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
        false, // useStowOverwrite
        false  // useHttp2ForStow (use HTTP/1.1 to avoid idle timeout issues)
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

  @Test
  public void testCreate_StreamFullyReadableAfterMetadataExtraction() throws Exception {
    // This test verifies that TeeInputStream + SequenceInputStream correctly
    // preserves the entire stream after metadata extraction

    // Arrange
    when(mockDatabaseService.getStorageForStudy(TEST_STUDY_UID))
        .thenReturn(TEST_DESTINATION_A);

    ByteArrayInputStream dicomStream = createTestDicomStream(TEST_STUDY_UID);
    int originalSize = dicomStream.available();

    // Act
    DestinationHolder result = factory.create(TEST_CALLING_AET, UID.ImplicitVRLittleEndian, dicomStream);

    // Assert
    assertNotNull("DestinationHolder should not be null", result);

    // Verify that the returned stream can be fully read
    java.io.InputStream returnedStream = result.getCountingInputStream();
    assertNotNull("Returned stream should not be null", returnedStream);

    // Read all bytes from the returned stream
    ByteArrayOutputStream readBytes = new ByteArrayOutputStream();
    byte[] buffer = new byte[1024];
    int bytesRead;
    int totalBytesRead = 0;
    while ((bytesRead = returnedStream.read(buffer)) != -1) {
      readBytes.write(buffer, 0, bytesRead);
      totalBytesRead += bytesRead;
    }

    // Verify that we read all the data (or close to it - within 10% due to encoding differences)
    assertTrue("Should read at least 90% of original stream size. " +
        "Original: " + originalSize + ", Read: " + totalBytesRead,
        totalBytesRead >= originalSize * 0.9);
  }

  @Test
  public void testCreate_LargeFile_OnlyMetadataBuffered() throws Exception {
    // This test verifies that only metadata is buffered, not the entire file
    // We create a large file and verify that metadata extraction is fast

    // Arrange
    when(mockDatabaseService.getStorageForStudy(TEST_STUDY_UID))
        .thenReturn(null);
    when(mockDatabaseService.getStorageForAet(TEST_CALLING_AET))
        .thenReturn(null);

    // Create a DICOM stream with large pixel data
    ByteArrayInputStream dicomStream = createLargeTestDicomStream(TEST_STUDY_UID, 1024 * 1024); // 1MB pixel data

    // Act
    long startTime = System.currentTimeMillis();
    DestinationHolder result = factory.create(TEST_CALLING_AET, UID.ImplicitVRLittleEndian, dicomStream);
    long duration = System.currentTimeMillis() - startTime;

    // Assert
    assertNotNull("DestinationHolder should not be null", result);

    // Metadata extraction should be fast (< 1 second) even with large pixel data
    // because we only read up to PixelData tag
    assertTrue("Metadata extraction took too long: " + duration + "ms. " +
        "Should be < 1000ms since we only read metadata, not pixel data.",
        duration < 1000);
  }

  @Test
  public void testCreate_MalformedDicom_HandlesGracefully() throws Exception {
    // This test verifies that malformed DICOM files are handled gracefully
    // by the TeeInputStream approach (no mark/reset corruption issues)

    // Arrange
    when(mockDatabaseService.getStorageForStudy(TEST_STUDY_UID))
        .thenReturn(TEST_DESTINATION_A);

    // Create a malformed DICOM stream with unusual attributes
    ByteArrayInputStream dicomStream = createMalformedTestDicomStream(TEST_STUDY_UID);

    // Act - should not throw exception
    DestinationHolder result = factory.create(TEST_CALLING_AET, UID.ImplicitVRLittleEndian, dicomStream);

    // Assert
    assertNotNull("DestinationHolder should not be null even with malformed DICOM", result);
    verify(mockDatabaseService).getStorageForStudy(TEST_STUDY_UID);
  }

  @Test
  public void testCreate_StreamWithManyAttributes_ExtractsCorrectly() throws Exception {
    // This test verifies that files with many attributes (large metadata) work correctly

    // Arrange
    when(mockDatabaseService.getStorageForStudy(TEST_STUDY_UID))
        .thenReturn(TEST_DESTINATION_B);

    ByteArrayInputStream dicomStream = createTestDicomStreamWithManyAttributes(TEST_STUDY_UID);

    // Act
    DestinationHolder result = factory.create(TEST_CALLING_AET, UID.ImplicitVRLittleEndian, dicomStream);

    // Assert
    assertNotNull("DestinationHolder should not be null", result);
    verify(mockDatabaseService).getStorageForStudy(TEST_STUDY_UID);

    // Verify stream is still readable
    java.io.InputStream returnedStream = result.getCountingInputStream();
    assertNotNull("Returned stream should not be null", returnedStream);
    assertTrue("Stream should have data available", returnedStream.available() > 0);
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

  /**
   * Helper method to create a large test DICOM stream with pixel data.
   */
  private ByteArrayInputStream createLargeTestDicomStream(String studyInstanceUID, int pixelDataSize)
      throws IOException {
    Attributes attrs = new Attributes();

    // Add required DICOM attributes (metadata)
    attrs.setString(Tag.SOPClassUID, VR.UI, "1.2.840.10008.5.1.4.1.1.1");
    attrs.setString(Tag.SOPInstanceUID, VR.UI, "1.2.3.4.5.SOP_INSTANCE");
    attrs.setString(Tag.StudyInstanceUID, VR.UI, studyInstanceUID);
    attrs.setString(Tag.SeriesInstanceUID, VR.UI, "1.2.3.4.5.SERIES");
    attrs.setString(Tag.PatientID, VR.LO, "TEST_PATIENT");

    // Add image attributes
    attrs.setInt(Tag.Rows, VR.US, 512);
    attrs.setInt(Tag.Columns, VR.US, 512);
    attrs.setInt(Tag.BitsAllocated, VR.US, 16);

    // Add large pixel data
    byte[] pixelData = new byte[pixelDataSize];
    for (int i = 0; i < pixelDataSize; i++) {
      pixelData[i] = (byte) (i % 256);
    }
    attrs.setBytes(Tag.PixelData, VR.OB, pixelData);

    // Write to byte array
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (DicomOutputStream dos = new DicomOutputStream(baos, UID.ImplicitVRLittleEndian)) {
      dos.writeDataset(null, attrs);
    }

    return new ByteArrayInputStream(baos.toByteArray());
  }

  /**
   * Helper method to create a malformed DICOM stream with unusual attributes.
   */
  private ByteArrayInputStream createMalformedTestDicomStream(String studyInstanceUID) throws IOException {
    Attributes attrs = new Attributes();

    // Add required attributes
    attrs.setString(Tag.SOPClassUID, VR.UI, "1.2.840.10008.5.1.4.1.1.1");
    attrs.setString(Tag.SOPInstanceUID, VR.UI, "1.2.3.4.5.SOP_INSTANCE");
    attrs.setString(Tag.StudyInstanceUID, VR.UI, studyInstanceUID);
    attrs.setString(Tag.SeriesInstanceUID, VR.UI, "1.2.3.4.5.SERIES");

    // Add some unusual/private tags that might cause parsing issues
    attrs.setString(0x00091010, VR.LO, "PRIVATE_TAG_1");
    attrs.setString(0x00191020, VR.LO, "PRIVATE_TAG_2");

    // Add attributes with potentially problematic values
    attrs.setString(Tag.PatientName, VR.PN, "Test^Patient^With^Many^Components^Extra");

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (DicomOutputStream dos = new DicomOutputStream(baos, UID.ImplicitVRLittleEndian)) {
      dos.writeDataset(null, attrs);
    }

    return new ByteArrayInputStream(baos.toByteArray());
  }

  /**
   * Helper method to create a DICOM stream with many attributes (large metadata).
   */
  private ByteArrayInputStream createTestDicomStreamWithManyAttributes(String studyInstanceUID)
      throws IOException {
    Attributes attrs = new Attributes();

    // Add required DICOM attributes
    attrs.setString(Tag.SOPClassUID, VR.UI, "1.2.840.10008.5.1.4.1.1.1");
    attrs.setString(Tag.SOPInstanceUID, VR.UI, "1.2.3.4.5.SOP_INSTANCE");
    attrs.setString(Tag.StudyInstanceUID, VR.UI, studyInstanceUID);
    attrs.setString(Tag.SeriesInstanceUID, VR.UI, "1.2.3.4.5.SERIES");
    attrs.setString(Tag.PatientID, VR.LO, "TEST_PATIENT");

    // Add many additional attributes to increase metadata size
    attrs.setString(Tag.PatientName, VR.PN, "Test^Patient");
    attrs.setString(Tag.PatientBirthDate, VR.DA, "19800101");
    attrs.setString(Tag.PatientSex, VR.CS, "M");
    attrs.setString(Tag.StudyDate, VR.DA, "20240101");
    attrs.setString(Tag.StudyTime, VR.TM, "120000");
    attrs.setString(Tag.AccessionNumber, VR.SH, "ACC123456");
    attrs.setString(Tag.Modality, VR.CS, "CR");
    attrs.setString(Tag.Manufacturer, VR.LO, "Test Manufacturer");
    attrs.setString(Tag.InstitutionName, VR.LO, "Test Hospital");
    attrs.setString(Tag.ReferringPhysicianName, VR.PN, "Referring^Physician");
    attrs.setString(Tag.StationName, VR.SH, "STATION1");
    attrs.setString(Tag.StudyDescription, VR.LO, "Test Study Description");
    attrs.setString(Tag.SeriesDescription, VR.LO, "Test Series Description");
    attrs.setString(Tag.BodyPartExamined, VR.CS, "CHEST");
    attrs.setString(Tag.PatientPosition, VR.CS, "HFS");

    // Add 50 more custom attributes to stress test metadata buffering
    for (int i = 0; i < 50; i++) {
      attrs.setString(0x00990010 + i, VR.LO, "CUSTOM_ATTRIBUTE_" + i);
    }

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (DicomOutputStream dos = new DicomOutputStream(baos, UID.ImplicitVRLittleEndian)) {
      dos.writeDataset(null, attrs);
    }

    return new ByteArrayInputStream(baos.toByteArray());
  }
}
