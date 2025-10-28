package com.google.cloud.healthcare.imaging.dicomadapter.cstore.destination;

import com.google.cloud.healthcare.IDicomWebClient;
import com.google.cloud.healthcare.imaging.dicomadapter.AetDictionary;
import com.google.common.collect.ImmutableList;
import com.google.common.io.CountingInputStream;
import java.io.InputStream;
import org.dcm4che3.data.Attributes;

public class DestinationHolder {

  private IDicomWebClient singleDestination;
  private ImmutableList<IDicomWebClient> healthcareDestinations;
  private ImmutableList<AetDictionary.Aet> dicomDestinations;
  private CountingInputStream countingInputStream;
  private Attributes metadata;
  private String tempFilePath;

  public DestinationHolder(InputStream destinationInputStream, IDicomWebClient defaultDestination) {
    this.countingInputStream = new CountingInputStream(destinationInputStream);
    //default values
    this.singleDestination = defaultDestination;
    this.healthcareDestinations = ImmutableList.of(defaultDestination);
    this.dicomDestinations = ImmutableList.of();
  }

  public CountingInputStream getCountingInputStream() {
    return countingInputStream;
  }

  public void setSingleDestination(IDicomWebClient dicomWebClient) {
    this.singleDestination = dicomWebClient;
  }

  public IDicomWebClient getSingleDestination() {
    return singleDestination;
  }

  public void setHealthcareDestinations(ImmutableList<IDicomWebClient> healthcareDestinations) {
    this.healthcareDestinations = healthcareDestinations;
  }

  public void setDicomDestinations(ImmutableList<AetDictionary.Aet> dicomDestinations) {
    this.dicomDestinations = dicomDestinations;
  }

  public ImmutableList<IDicomWebClient> getHealthcareDestinations() {
    return healthcareDestinations;
  }

  public ImmutableList<AetDictionary.Aet> getDicomDestinations() {
    return dicomDestinations;
  }

  public Attributes getMetadata() {
    return metadata;
  }

  public void setMetadata(Attributes metadata) {
    this.metadata = metadata;
  }

  public String getTempFilePath() {
    return tempFilePath;
  }

  public void setTempFilePath(String tempFilePath) {
    this.tempFilePath = tempFilePath;
  }
}
