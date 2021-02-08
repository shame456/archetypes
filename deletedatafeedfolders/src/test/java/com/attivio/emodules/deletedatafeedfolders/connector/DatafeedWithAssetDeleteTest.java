/** Copyright 2021 Lucidworks Inc., All rights reserved. */
package com.attivio.emodules.deletedatafeedfolders.connector;

import com.attivio.sdk.esb.PlatformMessage;
import com.attivio.sdk.ingest.DocumentList;
import com.attivio.sdk.ingest.DocumentMode;
import com.attivio.sdk.ingest.IngestDocument;
import com.attivio.sdk.scanner.TestScannerRunner;
import com.attivio.sdk.schema.FieldNames;
import com.attivio.sdk.test.SdkTestUtils;
import com.attivio.util.FileUtils;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import org.junit.Assert;
import org.junit.Test;

/**
 * Run the incremental sample scanner using SdkTestUtils and test that the correct documents are
 * published.
 */
public class DatafeedWithAssetDeleteTest {
  private static final int FILE_COUNT = 5;

  private File dataDir;

  @Test
  public void testDirectory() throws Exception {
    doTest(createTestFiles(FILE_COUNT));
  }

  private void doTest(List<String> fileList) throws Exception {
    List<String> scannerFileList = Arrays.asList(new String[] {dataDir.getAbsolutePath()});
    try {
      DatafeedWithAssetDelete sampleScanner = new DatafeedWithAssetDelete();
      sampleScanner.setFileList(scannerFileList);
      sampleScanner.setIncrementalDeletes(
          false); // Existing documents will not be deleted if not seen during the scan
      sampleScanner.setIncrementalModeActivated(
          true); // Only new or modified documents will be published
      sampleScanner.setIncrementalDeletes(true); // Obsolete documents will be deleted
      sampleScanner.setRealm("acme");
      sampleScanner.setPrincipal("A-Team");
      TestScannerRunner scannerRunner =
          SdkTestUtils.createTestScannerRunner("sampleIncrementalScannerConnector", sampleScanner);

      // Test the initial scan
      scannerRunner.start();
      DocumentList documentList = (DocumentList) scannerRunner.getSentMessages().get(0);
      Assert.assertEquals("Wrong number of documents", FILE_COUNT, documentList.size());
      validateDocument(documentList, "some", "more");

      // Test that unchanged documents are not re-fed
      scannerRunner.start();
      List<PlatformMessage> message = scannerRunner.getSentMessages();
      documentList = message.size() > 0 ? (DocumentList) message.get(0) : new DocumentList();
      Assert.assertEquals(
          "Expected no documents since it's an incremental scanner and the data source was not modified",
          0,
          documentList.size());

      // Test the modified documents are fed
      List<String> fileToModify = new ArrayList<String>();
      fileToModify.add(fileList.get(0));
      fileToModify.add(fileList.get(1));
      modifyFiles(fileToModify);
      scannerRunner.start();
      documentList = (DocumentList) scannerRunner.getSentMessages().get(0);
      Assert.assertEquals("Expected only the modified documents", 2, documentList.size());
      validateDocument(documentList, "new");

      // Test that all the documents are re-fed after reset
      sampleScanner.reset("sampleIncrementalScannerConnector");
      scannerRunner.start();
      documentList = (DocumentList) scannerRunner.getSentMessages().get(0);
      Assert.assertEquals(
          "Wrong number of documents - all the documents should be there after reset",
          FILE_COUNT,
          documentList.size());

      // Test that obsolete documents are deleted
      new File(fileList.get(0)).delete();
      nap(); // On some systems the file deletion is not synchronic
      scannerRunner.start();
      documentList = (DocumentList) scannerRunner.getSentMessages().get(0);
      Assert.assertEquals("Only one deleted document is expected", 1, documentList.size());
      for (IngestDocument doc : documentList) {
        Assert.assertEquals(DocumentMode.DELETE, doc.getMode());
      }

    } finally {
      FileUtils.deleteDirectory(dataDir);
    }
  }

  private void nap() {
    try {
      Thread.sleep(100);
    } catch (Exception e) {
    }
  }

  private List<String> createTestFiles(int fileCount) throws IOException {
    if (fileCount < 0)
      throw new IOException(String.format("fileCount is %s - cannot be < )", fileCount));

    File baseDir = new File(System.getProperty("java.io.tmpdir"));
    dataDir = new File(baseDir, "d" + UUID.randomUUID());
    if (!dataDir.mkdir())
      throw new IOException("Failed to create firectory " + dataDir.getAbsolutePath());

    List<String> fileList = new ArrayList<String>();

    for (int i = 0; i < fileCount; i++) {
      File f = new File(dataDir, "d" + i);
      FileUtils.writeLines(f, Arrays.asList("some text" + i, "more text" + i));
      fileList.add(f.getAbsolutePath());
    }

    return fileList;
  }

  private void modifyFiles(List<String> fileList) throws IOException {
    for (String path : fileList) {
      FileUtils.writeLines(new File(path), Arrays.asList("new text ", "new text "));
    }
  }

  // All the documents should have this text
  private void validateDocument(DocumentList documentList, String... text) {

    for (IngestDocument doc : documentList) {
      Assert.assertNotNull(doc.getFirstValue(FieldNames.TITLE));
      Assert.assertNotNull(doc.getFirstValue(FieldNames.DATE));
      Assert.assertNotNull(
          "An ACL must be attached to the document since the scanner was configured for document security",
          doc.getAcl());
      for (String stringItem : text)
        Assert.assertTrue(
            "Expected text is missing",
            doc.getFirstValue(FieldNames.TEXT).stringValue().contains(stringItem));
    }
  }
}
