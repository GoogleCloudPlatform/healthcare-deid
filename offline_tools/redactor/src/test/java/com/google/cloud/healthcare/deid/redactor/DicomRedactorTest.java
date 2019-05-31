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
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.dcm4che3.data.Attributes;
import org.dcm4che3.data.Fragments;
import org.dcm4che3.data.Implementation;
import org.dcm4che3.data.Sequence;
import org.dcm4che3.data.Tag;
import org.dcm4che3.data.UID;
import org.dcm4che3.data.VR;
import org.dcm4che3.data.Value;
import org.dcm4che3.io.DicomOutputStream;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Test basic DICOM tag redaction. */
@RunWith(JUnit4.class)
public final class DicomRedactorTest {
  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  private void redactAndVerify(Attributes metadata, Attributes inData, Attributes expectedMetadata,
      Attributes expectedData, DicomConfig config) throws Exception {
    File inFile = folder.newFile("in.dcm");
    File outFile = folder.newFile("out.dcm");
    File expectedFile = folder.newFile("exp.dcm");

    writeDataset(inFile, metadata, inData);
    writeDataset(expectedFile, expectedMetadata, expectedData);

    InputStream is = new BufferedInputStream(new FileInputStream(inFile));
    OutputStream os = new BufferedOutputStream(new FileOutputStream(outFile));

    DicomRedactor redactor = new DicomRedactor(config);
    redactor.redact(is, os);
    Assert.assertTrue(FileUtils.contentEquals(outFile, expectedFile));
  }

  private Attributes getTestMetadataHeader(String transferSyntaxUID) {
    Attributes metadata = new Attributes();
    metadata.setBytes(Tag.FileMetaInformationVersion, VR.OB, new byte[]{0x00, 0x01});
    metadata.setString(Tag.MediaStorageSOPInstanceUID, VR.UI, "Test");
    metadata.setString(Tag.MediaStorageSOPClassUID, VR.UI, "Test");
    metadata.setString(Tag.TransferSyntaxUID, VR.UI, transferSyntaxUID);
    metadata.setString(Tag.ImplementationClassUID, VR.UI, Implementation.getClassUID());
    metadata.setString(Tag.ImplementationVersionName, VR.SH, Implementation.getVersionName());
    return metadata;
  }

  private void writeDataset(File f, Attributes md, Attributes dataset) throws Exception {
    try (DicomOutputStream os = new DicomOutputStream(
        new BufferedOutputStream(new FileOutputStream(f)), UID.ExplicitVRLittleEndian)) {
      os.writeDataset(md, dataset);
    }
  }

  @Test
  public void redactRemoveList() throws Exception {
    Attributes metadata = getTestMetadataHeader(UID.ExplicitVRLittleEndian);
    Attributes dataset = new Attributes();
    dataset.setString(Tag.IdentifyingComments, VR.LT, "Test String");
    dataset.setString(Tag.PatientName, VR.PN, "Person^Name");
    Attributes seq = new Attributes();
    seq.setString(Tag.PatientName, VR.PN, "Person^Name");
    Sequence items = dataset.newSequence(Tag.FailedSOPSequence, 1);
    items.add(seq);

    List<String> tags = new ArrayList<String>();
    tags.add("PatientName");
    DicomConfig config = DicomConfig.newBuilder().setRemoveList(
        DicomConfig.TagFilterList.newBuilder().addAllTags(tags)).build();

    Attributes expected = new Attributes();
    expected.setString(Tag.IdentifyingComments, VR.LT, "Test String");
    expected.setString(Tag.PatientName, VR.PN, "");
    Attributes expSeq = new Attributes();
    expSeq.setString(Tag.PatientName, VR.PN, "Person^Name");
    Sequence expectedItems = expected.newSequence(Tag.FailedSOPSequence, 1);
    expectedItems.add(expSeq);

    redactAndVerify(metadata, dataset, metadata, expected, config);
  }

  @Test
  public void redactRemoveListNumberedTag() throws Exception {
    Attributes metadata = getTestMetadataHeader(UID.ExplicitVRLittleEndian);
    Attributes dataset = new Attributes();
    dataset.setString(Tag.IdentifyingComments, VR.LT, "Test String");
    dataset.setString(Tag.PatientName, VR.PN, "Person^Name");
    Attributes seq = new Attributes();
    seq.setString(Tag.PatientName, VR.PN, "Person^Name");
    Sequence singleItemSequence = dataset.newSequence(Tag.FailedSOPSequence, 1);
    singleItemSequence.add(seq);

    List<String> tags = new ArrayList<String>();
    tags.add("00100010"); // PatientName
    DicomConfig config = DicomConfig.newBuilder().setRemoveList(
        DicomConfig.TagFilterList.newBuilder().addAllTags(tags)).build();

    Attributes expected = new Attributes();
    expected.setString(Tag.IdentifyingComments, VR.LT, "Test String");
    expected.setString(Tag.PatientName, VR.PN, "");
    Attributes expSeq = new Attributes();
    expSeq.setString(Tag.PatientName, VR.PN, "Person^Name");
    Sequence expectedItems = expected.newSequence(Tag.FailedSOPSequence, 1);
    expectedItems.add(expSeq);

    redactAndVerify(metadata, dataset, metadata, expected, config);
  }

  @Test
  public void redactRemoveListSequence() throws Exception {
    Attributes metadata = getTestMetadataHeader(UID.ExplicitVRLittleEndian);
    Attributes dataset = new Attributes();
    Attributes seq = new Attributes();
    seq.setString(Tag.PatientName, VR.PN, "Person^Name");
    Sequence singleItemSequence = dataset.newSequence(Tag.FailedSOPSequence, 1);
    singleItemSequence.add(seq);

    List<String> tags = new ArrayList<String>();
    tags.add("00081198"); // FailedSOPSequence
    DicomConfig config = DicomConfig.newBuilder().setRemoveList(
        DicomConfig.TagFilterList.newBuilder().addAllTags(tags)).build();

    Attributes expected = new Attributes();
    Sequence expectedItems = expected.newSequence(Tag.FailedSOPSequence, 0);

    redactAndVerify(metadata, dataset, metadata, expected, config);
  }

  @Test
  public void redactRemoveListEmpty() throws Exception {
     Attributes metadata = getTestMetadataHeader(UID.ExplicitVRLittleEndian);
    Attributes dataset = new Attributes();
    dataset.setString(Tag.PatientName, VR.PN, "Person^Name");
    Attributes seq = new Attributes();
    seq.setString(Tag.PatientName, VR.PN, "Person^Name");
    Sequence singleItemSequence = dataset.newSequence(Tag.FailedSOPSequence, 1);
    singleItemSequence.add(seq);

    List<String> tags = new ArrayList<String>();
    DicomConfig config = DicomConfig.newBuilder().setRemoveList(
        DicomConfig.TagFilterList.newBuilder().addAllTags(tags)).build();

    redactAndVerify(metadata, dataset, metadata, dataset, config);
  }

  @Test
  public void uncompressedPixelData() throws Exception {
    Attributes metadata = getTestMetadataHeader(UID.ExplicitVRLittleEndian);
    Attributes dataset = new Attributes();
    dataset.setBytes(Tag.PixelData, VR.OB,
        new byte[]{(byte) 0xAB, (byte) 0xCD, (byte) 0xEF, (byte) 0x01});

    List<String> tags = new ArrayList<String>();
    tags.add("PixelData");
    DicomConfig config = DicomConfig.newBuilder().setRemoveList(
        DicomConfig.TagFilterList.newBuilder().addAllTags(tags)).build();

    Attributes expectedMetadata = getTestMetadataHeader(UID.ExplicitVRLittleEndian);
    Attributes expected = new Attributes();
    expected.setBytes(Tag.PixelData, VR.OB, new byte[]{});

    redactAndVerify(metadata, dataset, expectedMetadata, expected, config);
  }

  @Test
  public void singleFragmentPixelData() throws Exception {
    Attributes metadata = getTestMetadataHeader(UID.JPEGBaseline1);
    Attributes dataset = new Attributes();
    Fragments fragments = dataset.newFragments(Tag.PixelData, VR.OB, 2);
    fragments.add(Value.NULL);
    fragments.add(new byte[]{(byte) 0xAB, (byte) 0xCD});

    List<String> tags = new ArrayList<String>();
    tags.add("PixelData");
    DicomConfig config = DicomConfig.newBuilder().setRemoveList(
        DicomConfig.TagFilterList.newBuilder().addAllTags(tags)).build();

    Attributes expectedMetadata = getTestMetadataHeader(UID.ExplicitVRLittleEndian);
    Attributes expected = new Attributes();
    expected.setBytes(Tag.PixelData, VR.OB, new byte[]{});

    redactAndVerify(metadata, dataset, expectedMetadata, expected, config);
  }

  @Test
  public void bigEndianPixelData() throws Exception {
    Attributes metadata = getTestMetadataHeader(UID.ExplicitVRBigEndianRetired);
    Attributes dataset = new Attributes();
    dataset.setBytes(Tag.PixelData, VR.OB,
        new byte[]{(byte) 0xAB, (byte) 0xCD, (byte) 0xEF, (byte) 0x01});

    List<String> tags = new ArrayList<String>();
    tags.add("PixelData");
    DicomConfig config = DicomConfig.newBuilder().setRemoveList(
        DicomConfig.TagFilterList.newBuilder().addAllTags(tags)).build();

    Attributes expected = new Attributes();
    expected.setBytes(Tag.PixelData, VR.OB, new byte[]{});

    redactAndVerify(metadata, dataset, metadata, expected, config);
  }

  @Test
  public void redactKeepList() throws Exception {
    Attributes metadata = getTestMetadataHeader(UID.ExplicitVRLittleEndian);
    Attributes dataset = new Attributes();
    dataset.setString(Tag.IdentifyingComments, VR.LT, "Test String");
    dataset.setString(Tag.PatientName, VR.PN, "Person^Name");
    Attributes seq = new Attributes();
    seq.setString(Tag.PatientName, VR.PN, "Person^Name");
    Sequence singleItemSequence = dataset.newSequence(Tag.FailedSOPSequence, 1);
    singleItemSequence.add(seq);

    List<String> tags = new ArrayList<String>();
    tags.add("PatientName");
    DicomConfig config = DicomConfig.newBuilder().setKeepList(
        DicomConfig.TagFilterList.newBuilder().addAllTags(tags)).build();

    Attributes expected = new Attributes();
    expected.setString(Tag.IdentifyingComments, VR.LT, "");
    expected.setString(Tag.PatientName, VR.PN, "Person^Name");
    Sequence expectedItems = expected.newSequence(Tag.FailedSOPSequence, 0);

    redactAndVerify(metadata, dataset, metadata, expected, config);
  }

  @Test
  public void redactKeepListEmpty() throws Exception {
    Attributes metadata = getTestMetadataHeader(UID.ExplicitVRLittleEndian);
    Attributes dataset = new Attributes();
    dataset.setString(Tag.PatientName, VR.PN, "Person^Name");
    Attributes seq = new Attributes();
    seq.setString(Tag.PatientName, VR.PN, "Person^Name");
    Sequence singleItemSequence = dataset.newSequence(Tag.FailedSOPSequence, 1);
    singleItemSequence.add(seq);

    List<String> tags = new ArrayList<String>();
    DicomConfig config = DicomConfig.newBuilder().setKeepList(
        DicomConfig.TagFilterList.newBuilder().addAllTags(tags)).build();

    Attributes expected = new Attributes();
    expected.setString(Tag.PatientName, VR.PN, "");
    Sequence expectedItems = expected.newSequence(Tag.FailedSOPSequence, 0);

    redactAndVerify(metadata, dataset, metadata, expected, config);
  }

  @Test
  public void littleEndianImplicitTransferSyntax() throws Exception {
    Attributes metadata = getTestMetadataHeader(UID.ImplicitVRLittleEndian);
    Attributes dataset = new Attributes();
    dataset.setString(Tag.PatientName, VR.PN, "Person^Name");

    List<String> tags = new ArrayList<String>();
    tags.add("PatientName");
    DicomConfig config = DicomConfig.newBuilder().setRemoveList(
        DicomConfig.TagFilterList.newBuilder().addAllTags(tags)).build();

    Attributes expected = new Attributes();
    expected.setString(Tag.PatientName, VR.PN, "");

    redactAndVerify(metadata, dataset, metadata, expected, config);
  }


  @Test(expected = IllegalArgumentException.class)
  public void invalidTag() throws Exception {
     Attributes metadata = getTestMetadataHeader(UID.ExplicitVRLittleEndian);
    Attributes dataset = new Attributes();
    dataset.setString(Tag.PatientName, VR.PN, "Person^Name");

    List<String> tags = new ArrayList<String>();
    tags.add("invalidTag123");
    DicomConfig config = DicomConfig.newBuilder().setRemoveList(
        DicomConfig.TagFilterList.newBuilder().addAllTags(tags)).build();

    redactAndVerify(metadata, dataset, metadata, dataset, config);
  }

  @Test(expected = IllegalArgumentException.class)
  public void emptyConfig() throws Exception {
     Attributes metadata = getTestMetadataHeader(UID.ExplicitVRLittleEndian);
    Attributes dataset = new Attributes();
    dataset.setString(Tag.PatientName, VR.PN, "Person^Name");

    DicomConfig config = DicomConfig.newBuilder().build();

    redactAndVerify(metadata, dataset, metadata, dataset, config);
  }

  @Test(expected = IOException.class)
  public void failRead() throws Exception {
     Attributes metadata = getTestMetadataHeader(UID.ExplicitVRLittleEndian);
    Attributes dataset = new Attributes();
    dataset.setString(Tag.PatientName, VR.PN, "Person^Name");

    List<String> tags = new ArrayList<String>();
    DicomConfig config = DicomConfig.newBuilder().setRemoveList(
        DicomConfig.TagFilterList.newBuilder().addAllTags(tags)).build();

    File inFile = folder.newFile("in.dcm");
    writeDataset(inFile, metadata, dataset);
    File outFile = folder.newFile("out.dcm");

    InputStream is = new BufferedInputStream(new FileInputStream(inFile));
    is.close();
    OutputStream os = new BufferedOutputStream(new FileOutputStream(outFile));

    DicomRedactor redactor = new DicomRedactor(config);
    redactor.redact(is, os);
  }

  @Test(expected = IOException.class)
  public void failWrite() throws Exception {
     Attributes metadata = getTestMetadataHeader(UID.ExplicitVRLittleEndian);
    Attributes dataset = new Attributes();
    dataset.setString(Tag.PatientName, VR.PN, "Person^Name");

    List<String> tags = new ArrayList<String>();
    DicomConfig config = DicomConfig.newBuilder().setRemoveList(
        DicomConfig.TagFilterList.newBuilder().addAllTags(tags)).build();

    File inFile = folder.newFile("in.dcm");
    writeDataset(inFile, metadata, dataset);
    File outFile = folder.newFile("out.dcm");

    InputStream is = new BufferedInputStream(new FileInputStream(inFile));
    OutputStream os = new BufferedOutputStream(new FileOutputStream(outFile));
    os.close();

    DicomRedactor redactor = new DicomRedactor(config);
    redactor.redact(is, os);
  }
}

