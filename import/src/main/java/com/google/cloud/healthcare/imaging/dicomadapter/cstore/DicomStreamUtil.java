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
   * IMPORTANT: This class implements mark/reset support to prevent DicomInputStream
   * from wrapping it in BufferedInputStream. The mark/reset is implemented using a
   * small buffer that allows DicomInputStream.guessTransferSyntax() to peek at the
   * first few bytes, which is required during stream initialization.
   *
   * All data read (including marked/reset data) is written to the sink to ensure
   * complete data capture for later replay via SequenceInputStream.
   */
  public static class TeeInputStream extends InputStream {
    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(TeeInputStream.class);
    private final InputStream source;
    private final OutputStream sink;
    private byte[] markBuffer = null;
    private int markBufferPos = 0;
    private int markBufferLimit = 0;
    private int markReplayLimit = 0;  // The limit for replaying after reset() - frozen at reset() time
    private int markReadLimit = 0;
    private long totalBytesRead = 0;  // Track total bytes read from source
    private long markPosition = 0;    // Position when mark() was called
    private int markCount = 0;  // Debug: count mark() calls
    private int resetCount = 0;  // Debug: count reset() calls

    public TeeInputStream(InputStream source, OutputStream sink) {
      this.source = source;
      this.sink = sink;
    }

    @Override
    public int read() throws IOException {
      // If we have buffered data from a previous mark/reset, read from buffer first
      // IMPORTANT: Use markReplayLimit (frozen at reset time), not markBufferLimit (grows during buffering)
      if (markBuffer != null && markBufferPos < markReplayLimit) {
        int b = markBuffer[markBufferPos++] & 0xFF;
        // log.debug("Read from markBuffer: pos={}/{}, byte={}", markBufferPos-1, markReplayLimit, b);
        // Data from buffer was already written to sink during initial read, don't write again
        return b;
      }

      int b = source.read();
      if (b != -1) {
        sink.write(b);
        totalBytesRead++;
        // If mark is active, buffer this byte
        if (markReadLimit > 0 && markBuffer != null && markBufferLimit < markBuffer.length) {
          markBuffer[markBufferLimit++] = (byte) b;
          // log.debug("Read from source and buffered: totalRead={}, markBufferLimit={}/{}", totalBytesRead, markBufferLimit, markBuffer.length);
        } else {
          // log.debug("Read from source (no buffering): totalRead={}, byte={}", totalBytesRead, b);
        }
      } else {
        // log.debug("Read from source: EOF at totalRead={}", totalBytesRead);
      }
      return b;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      // If we have buffered data from a previous mark/reset, read from buffer first
      // IMPORTANT: Use markReplayLimit (frozen at reset time), not markBufferLimit (grows during buffering)
      if (markBuffer != null && markBufferPos < markReplayLimit) {
        int available = markReplayLimit - markBufferPos;
        int toRead = Math.min(len, available);
        System.arraycopy(markBuffer, markBufferPos, b, off, toRead);
        markBufferPos += toRead;
        // log.debug("Read {} bytes from markBuffer: pos={}/{}", toRead, markBufferPos, markReplayLimit);
        // Data from buffer was already written to sink during initial read, don't write again
        return toRead;
      }

      int n = source.read(b, off, len);
      if (n > 0) {
        sink.write(b, off, n);
        totalBytesRead += n;
        // If mark is active, buffer these bytes
        if (markReadLimit > 0 && markBuffer != null) {
          int toBuf = Math.min(n, markBuffer.length - markBufferLimit);
          if (toBuf > 0) {
            System.arraycopy(b, off, markBuffer, markBufferLimit, toBuf);
            markBufferLimit += toBuf;
            // log.debug("Read {} bytes from source, buffered {} bytes: totalRead={}, markBufferLimit={}/{}", n, toBuf, totalBytesRead, markBufferLimit, markBuffer.length);
          } else {
            // log.debug("Read {} bytes from source (mark buffer full): totalRead={}, markBufferLimit={}/{}", n, totalBytesRead, markBufferLimit, markBuffer.length);
          }
        } else {
          // log.debug("Read {} bytes from source (no mark active): totalRead={}", n, totalBytesRead);
        }
      } else if (n == 0) {
        // log.debug("Read 0 bytes from source: totalRead={}", totalBytesRead);
      } else {
        // log.debug("Read from source: EOF at totalRead={}", totalBytesRead);
      }
      return n;
    }

    @Override
    public boolean markSupported() {
      return true;
    }

    @Override
    public void mark(int readlimit) {
      markCount++;
      markPosition = totalBytesRead;
      // log.debug("mark() called #{}: readlimit={}, position={}, prev markBufferLimit={}", markCount, readlimit, markPosition, markBufferLimit);
      // Allocate buffer for mark/reset support
      // DicomInputStream typically needs only a small buffer for transfer syntax detection
      markReadLimit = readlimit;
      markBuffer = new byte[Math.max(readlimit, 1024)];
      markBufferPos = 0;
      markBufferLimit = 0;
      markReplayLimit = 0;  // No data to replay yet
    }

    @Override
    public void reset() throws IOException {
      resetCount++;
      if (markBuffer == null || markReadLimit == 0) {
        log.error("reset() called #{} but mark not set!", resetCount);
        throw new IOException("Mark not set");
      }
      // log.debug("reset() called #{}: rewinding to markPosition={}, markBufferLimit={}, currentPos={}", resetCount, markPosition, markBufferLimit, totalBytesRead);
      // Reset to beginning of mark buffer for re-reading
      // Don't clear markBuffer - it may be reset multiple times
      // IMPORTANT: Freeze markReplayLimit to current markBufferLimit
      // This prevents reading data that gets buffered AFTER reset()
      markReplayLimit = markBufferLimit;
      markBufferPos = 0;
    }

    @Override
    public void close() throws IOException {
      // log.debug("close() called: totalBytesRead={}, sink size={}", totalBytesRead,
      //     sink instanceof java.io.ByteArrayOutputStream ? ((java.io.ByteArrayOutputStream)sink).size() : "unknown");
      // log.debug("close() stacktrace:", new Exception("Stack trace for TeeInputStream.close()"));

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
