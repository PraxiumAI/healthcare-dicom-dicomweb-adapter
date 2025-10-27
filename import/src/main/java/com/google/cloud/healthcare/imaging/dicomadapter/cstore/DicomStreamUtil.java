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

package com.google.cloud.healthcare.imaging.dicomadapter.cstore;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.SequenceInputStream;
import org.dcm4che3.data.Attributes;
import org.dcm4che3.data.UID;
import org.dcm4che3.io.DicomOutputStream;
import org.dcm4che3.net.PDVInputStream;

/** Provides utilities for handling DICOM streams. */
public class DicomStreamUtil {

  // Adds the DICOM meta header to input stream.
  public static InputStream dicomStreamWithFileMetaHeader(
      String sopInstanceUID,
      String sopClassUID,
      String transferSyntax,
      InputStream inDicomStream) // PDVInputStream
      throws IOException {
    // File meta header (group 0002 tags), always in Explicit VR Little Endian.
    // http://dicom.nema.org/dicom/2013/output/chtml/part10/chapter_7.html
    ByteArrayOutputStream outBuffer = new ByteArrayOutputStream();
    DicomOutputStream fmiStream = new DicomOutputStream(outBuffer, UID.ExplicitVRLittleEndian);
    Attributes fmi =
        Attributes.createFileMetaInformation(sopInstanceUID, sopClassUID, transferSyntax);
    fmiStream.writeFileMetaInformation(fmi);

    // Add the file meta header + DICOM dataset (other groups) as a sequence of input streams.
    return new SequenceInputStream(
        new ByteArrayInputStream(outBuffer.toByteArray()), inDicomStream);
  }

  /**
   * TeeInputStream implementation that writes all data read from source
   * into a secondary output stream (like ByteArrayOutputStream for buffering).
   *
   * This allows reading metadata while simultaneously buffering it for later replay.
   * Used to solve the problem of reading DICOM attributes from network streams without
   * losing data when the stream needs to be forwarded/replayed.
   *
   * IMPORTANT: This class overrides markSupported() to return true to prevent
   * DicomInputStream from wrapping it in BufferedInputStream. DicomInputStream has
   * an ensureMarkSupported() method that automatically wraps streams in BufferedInputStream
   * if they don't support mark/reset. This would cause data loss because BufferedInputStream
   * reads ahead in chunks, and the buffered data doesn't get written to the sink.
   *
   * By claiming mark support (even though we don't actually implement it), we prevent
   * the automatic BufferedInputStream wrapping while maintaining the TeeInputStream behavior
   * of writing all read data to the sink.
   */
  public static class TeeInputStream extends InputStream {
    private final InputStream source;
    private final OutputStream sink;

    public TeeInputStream(InputStream source, OutputStream sink) {
      this.source = source;
      this.sink = sink;
    }

    @Override
    public int read() throws IOException {
      int b = source.read();
      if (b != -1) {
        sink.write(b);
      }
      return b;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      int n = source.read(b, off, len);
      if (n > 0) {
        sink.write(b, off, n);
      }
      return n;
    }

    @Override
    public boolean markSupported() {
      // Return true to prevent DicomInputStream from wrapping us in BufferedInputStream.
      // We don't actually implement mark/reset, but claiming support prevents the
      // automatic wrapping that would cause data loss.
      return true;
    }

    @Override
    public void mark(int readlimit) {
      // No-op: We claim mark support to prevent BufferedInputStream wrapping,
      // but we don't actually need to implement mark/reset functionality.
      // DicomInputStream only checks markSupported() but doesn't actually use mark/reset.
    }

    @Override
    public void reset() throws IOException {
      // Throw exception if reset is actually called (it shouldn't be).
      // This is a safety measure - DicomInputStream doesn't use reset, but if
      // something else tries to, we want to know about it.
      throw new IOException("mark/reset not supported on TeeInputStream");
    }

    @Override
    public void close() throws IOException {
      // Do not close the underlying source here; callers manage stream lifecycle.
      // Flushing sink ensures buffered bytes are written when used with ByteArrayOutputStream.
      try {
        sink.flush();
      } catch (IOException ignored) {
      }
    }
  }

  private DicomStreamUtil() {}
}
