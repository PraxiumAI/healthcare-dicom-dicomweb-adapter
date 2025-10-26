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

import java.util.concurrent.Executors;
import org.dcm4che3.net.ApplicationEntity;
import org.dcm4che3.net.Connection;
import org.dcm4che3.net.Device;
import org.dcm4che3.net.TransferCapability;
import org.dcm4che3.net.service.DicomServiceRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Static utilities for creating {@link Device} used in dcm4che library.
 */
public class DeviceUtil {

  private static final Logger log = LoggerFactory.getLogger(DeviceUtil.class);
  private static final String ALL_ALLOWED_SOP_CLASSES = "*";
  private static final String ALL_ALLOWED_TRANSFER_SYNTAXES = "*";
  private DeviceUtil() {
  }

  /**
   * Creates a DICOM server listening to the port for the given services handling all syntaxes
   */
  public static Device createServerDevice(
      String applicationEntityName, Integer dicomPort, DicomServiceRegistry serviceRegistry) {
    TransferCapability transferCapability =
        new TransferCapability(
            null /* commonName */,
            ALL_ALLOWED_SOP_CLASSES,
            TransferCapability.Role.SCP,
            ALL_ALLOWED_TRANSFER_SYNTAXES);
    return createServerDevice(
        applicationEntityName, dicomPort, serviceRegistry, transferCapability);
  }

  /**
   * Creates a DICOM server listening to the port for the given services
   */
  public static Device createServerDevice(
      String applicationEntityName,
      Integer dicomPort,
      DicomServiceRegistry serviceRegistry,
      TransferCapability transferCapability) {
    // Create a DICOM device.
    Device device = new Device("dicom-to-dicomweb-adapter-server");
    Connection connection = new Connection();
    connection.setPort(dicomPort);
    device.addConnection(connection);

    // Create an application entity (a network node) listening on input port.
    ApplicationEntity applicationEntity = new ApplicationEntity(applicationEntityName);
    applicationEntity.setAssociationAcceptor(true);
    applicationEntity.addConnection(connection);
    applicationEntity.addTransferCapability(transferCapability);
    device.addApplicationEntity(applicationEntity);

    // Add the DICOM request handlers to the device.
    device.setDimseRQHandler(serviceRegistry);
    device.setScheduledExecutor(Executors.newSingleThreadScheduledExecutor());
    device.setExecutor(Executors.newCachedThreadPool());
    return device;
  }

  /**
   * Creates a DICOM server with support for both plain and TLS connections.
   * Supports dual-port listening for backward compatibility with legacy devices.
   *
   * @param applicationEntityName The AET name
   * @param dicomPort Port for plain (unencrypted) connections, 0 or null to disable
   * @param dicomTlsPort Port for TLS connections, 0 or null to disable
   * @param serviceRegistry DICOM service handlers
   * @param tlsKeystore Path to keystore file (required if TLS port is enabled)
   * @param tlsKeystorePass Keystore password (required if TLS port is enabled)
   * @param tlsTruststore Path to truststore for mTLS (optional)
   * @param tlsTruststorePass Truststore password (optional)
   * @param tlsNeedClientAuth Whether to require client certificates (mTLS)
   * @return Configured Device instance
   */
  public static Device createServerDevice(
      String applicationEntityName,
      Integer dicomPort,
      Integer dicomTlsPort,
      DicomServiceRegistry serviceRegistry,
      String tlsKeystore,
      String tlsKeystorePass,
      String tlsTruststore,
      String tlsTruststorePass,
      boolean tlsNeedClientAuth) {

    Device device = new Device("dicom-to-dicomweb-adapter-server");
    ApplicationEntity applicationEntity = new ApplicationEntity(applicationEntityName);
    applicationEntity.setAssociationAcceptor(true);
    device.addApplicationEntity(applicationEntity);

    // 1. Configure plain (unencrypted) connection if port is specified
    if (dicomPort != null && dicomPort > 0) {
      Connection plainConnection = new Connection("plain", null, dicomPort);
      log.info("DIMSE service listening on plain port {}", dicomPort);
      device.addConnection(plainConnection);
      applicationEntity.addConnection(plainConnection);
    }

    // 2. Configure TLS connection if port and keystore are specified
    if (dicomTlsPort != null && dicomTlsPort > 0 && tlsKeystore != null && !tlsKeystore.isEmpty()) {
      Connection tlsConnection = new Connection("tls", null, dicomTlsPort);
      log.info("DIMSE service listening on TLS port {}", dicomTlsPort);

      try {
        // TLS settings are configured on the Device
        // Set keystore URL (path to the keystore file)
        device.setKeyStoreURL(tlsKeystore);
        device.setKeyStoreType("PKCS12");
        device.setKeyStorePin(tlsKeystorePass);
        device.setKeyStoreKeyPin(tlsKeystorePass); // Same password for key

        // Set truststore if provided (for mTLS)
        if (tlsTruststore != null && !tlsTruststore.isEmpty()) {
          device.setTrustStoreURL(tlsTruststore);
          device.setTrustStoreType("PKCS12");
          device.setTrustStorePin(tlsTruststorePass);
        }

        // Enable TLS on this specific connection
        // Support TLSv1, TLSv1.1, TLSv1.2 and TLSv1.3 for maximum compatibility with medical devices
        tlsConnection.setTlsProtocols("TLSv1", "TLSv1.1", "TLSv1.2", "TLSv1.3");

        // Set cipher suites compatible with medical devices (Horos, iCAD, etc.)
        // Includes legacy weak ciphers for compatibility with older DICOM viewers
        // Ordered by preference (strongest first, but Java will negotiate with client)
        tlsConnection.setTlsCipherSuites(
            // TLSv1.3 ciphers (strongest, modern devices)
            "TLS_AES_256_GCM_SHA384",
            "TLS_AES_128_GCM_SHA256",

            // TLSv1.2 ECDHE ciphers (forward secrecy)
            "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384",
            "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256",
            "TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA384",
            "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256",
            "TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA",
            "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA",

            // TLSv1.2/1.0 RSA with AES (common in medical devices)
            "TLS_RSA_WITH_AES_256_GCM_SHA384",
            "TLS_RSA_WITH_AES_128_GCM_SHA256",
            "TLS_RSA_WITH_AES_256_CBC_SHA256",
            "TLS_RSA_WITH_AES_128_CBC_SHA256",
            "TLS_RSA_WITH_AES_256_CBC_SHA",
            "TLS_RSA_WITH_AES_128_CBC_SHA",

            // Diffie-Hellman variants (Horos compatibility)
            "TLS_DHE_RSA_WITH_AES_256_CBC_SHA",
            "TLS_DHE_RSA_WITH_AES_128_CBC_SHA",
            "TLS_DHE_DSS_WITH_AES_256_CBC_SHA",
            "TLS_DHE_DSS_WITH_AES_128_CBC_SHA",

            // Legacy 3DES ciphers (weak but required for old devices like Horos)
            "TLS_RSA_WITH_3DES_EDE_CBC_SHA",
            "TLS_DHE_RSA_WITH_3DES_EDE_CBC_SHA",
            "TLS_DHE_DSS_WITH_3DES_EDE_CBC_SHA"

            // NOTE: RC4, MD5, NULL and DES (non-3DES) ciphers are NOT included
            // as they are critically insecure and disabled in modern Java versions
        );

        tlsConnection.setTlsNeedClientAuth(tlsNeedClientAuth);

        device.addConnection(tlsConnection);
        applicationEntity.addConnection(tlsConnection);
      } catch (Exception e) {
        // Throw a runtime exception to halt startup if TLS config is invalid.
        throw new RuntimeException("Failed to configure TLS on port " + dicomTlsPort, e);
      }
    }

    // Define what this AET can accept (all SOP classes and transfer syntaxes)
    TransferCapability transferCapability =
        new TransferCapability(
            null /* commonName */,
            ALL_ALLOWED_SOP_CLASSES,
            TransferCapability.Role.SCP,
            ALL_ALLOWED_TRANSFER_SYNTAXES);
    applicationEntity.addTransferCapability(transferCapability);

    device.setDimseRQHandler(serviceRegistry);
    device.setScheduledExecutor(Executors.newSingleThreadScheduledExecutor());
    device.setExecutor(Executors.newCachedThreadPool());
    return device;
  }

  /**
   * Creates a DICOM client device containing the given Application Entity
   */
  public static Device createClientDevice(
      ApplicationEntity applicationEntity, Connection connection) {
    Device device = new Device("dicom-to-dicomweb-adapter-client");
    device.addConnection(connection);
    device.addApplicationEntity(applicationEntity);
    device.setScheduledExecutor(Executors.newSingleThreadScheduledExecutor());
    device.setExecutor(Executors.newCachedThreadPool());
    return device;
  }
}
