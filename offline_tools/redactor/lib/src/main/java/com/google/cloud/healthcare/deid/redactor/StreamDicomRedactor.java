/*
 * Copyright 2019 Google LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.healthcare.deid.redactor;

import com.google.cloud.healthcare.deid.redactor.protos.DicomConfigProtos.DicomConfig;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.dcm4che3.data.Attributes;
import org.dcm4che3.data.Tag;
import org.dcm4che3.data.UID;
import org.dcm4che3.data.VR;
import org.dcm4che3.imageio.codec.TransferSyntaxType;
import org.dcm4che3.io.BulkDataDescriptor;
import org.dcm4che3.io.DicomInputStream;
import org.dcm4che3.io.DicomInputStream.IncludeBulkData;
import org.dcm4che3.io.DicomOutputStream;

/**
 * StreamDicomRedactor implements basic DICOM redaction without fully reading input stream in one
 * pass
 */
public class StreamDicomRedactor extends AbstractDicomRedactor {

  private static final int BATCH_SIZE = 8192;

  /**
   * Constructs a StreamDicomRedactor for the provided config.
   *
   * @throws IllegalArgumentException if the configuration structure is invalid.
   */
  public StreamDicomRedactor(DicomConfig config) throws Exception {
    super(config);
  }

  /**
   * Constructs a StreamDicomRedactor for the provided config and prefix for UID replacement.
   *
   * @throws IllegalArgumentException if the configuration structure is invalid.
   */
  public StreamDicomRedactor(DicomConfig config, String prefix) throws Exception {
    super(config, prefix);
  }

  /**
   * Redact the given DICOM input stream, and write the result to the given output stream. Without
   * reading whole dataset in one go. Drawback: will parse last dataset twice.
   * Inputstream.available can return 0 instead of -1 for undeterminted length streams, so can't
   * relied upon.
   *
   * @throws IOException if the input stream cannot be read or the output stream cannot be written.
   * @throws IllegalArgumentException if there is an error redacting the object.
   */
  public void redact(InputStream inStream, OutputStream outStream)
      throws IOException, IllegalArgumentException {
    Attributes metadata, dataset;
    RedactVisitor visitor = new RedactVisitor();
    try (DicomInputStream dicomInputStream = new DicomInputStream(inStream)) {
      try (DicomOutputStream dicomOutputStream = new DicomOutputStream(outStream,
          UID.ExplicitVRLittleEndian)) {
        try {
          dicomInputStream.setIncludeBulkData(IncludeBulkData.YES);
          dicomInputStream.setBulkDataDescriptor(BulkDataDescriptor.PIXELDATA);
          metadata = dicomInputStream.getFileMetaInformation();

          // Update UID in metadata.
          regenUID(metadata, Tag.MediaStorageSOPInstanceUID);

          // Overwrite transfer syntax if PixelData is to be removed.
          String ts = metadata.getString(Tag.TransferSyntaxUID);
          if (shouldRemoveTag(toTagID("PixelData"))
              && (TransferSyntaxType.forUID(ts) != TransferSyntaxType.NATIVE)) {
            metadata.setString(Tag.TransferSyntaxUID, VR.UI, UID.ExplicitVRLittleEndian);
          }
        } catch (Exception e) {
          throw new IOException("Failed to initialize redactor", e);
        }

        boolean firstDataset = true;
        boolean lastDataset = false;
        while (!lastDataset) {
          try {
            dicomInputStream.mark(BATCH_SIZE);
            dataset = dicomInputStream.readDataset(BATCH_SIZE, -1);
          } catch (EOFException e) {
            dicomInputStream.reset();
            dataset = dicomInputStream.readDataset(-1, -1);
            lastDataset = true;
          }

          try {
            dataset.accept(visitor, false);
          } catch (Exception e) {
            throw new IllegalArgumentException("Failed to redact one or more tags", e);
          }

          if (firstDataset) {
            firstDataset = false;
            dicomOutputStream.writeDataset(metadata, dataset);
          } else {
            dicomOutputStream.writeDataset(null, dataset);
          }
        }
      }
    }
  }
}
