/** Copyright 2018 Attivio Inc., All rights reserved. */
package com.attivio.emodules.deletedatafeedfolders;

import com.attivio.sdk.AttivioException;
import com.attivio.sdk.ingest.DocumentList;
import com.attivio.sdk.ingest.IngestDocument;
import com.attivio.sdk.scanner.TestScannerRunner;
import com.attivio.sdk.schema.FieldNames;
import com.attivio.sdk.test.SdkTestUtils;
import org.junit.Assert;
import org.junit.Test;

/** Run a simple test scanner using SdkTestUtils */
public class SampleConcurrentDataSourceScannerTest {
  private static final String HELLO_WORLD = "Hello World";

  @Test
  public void test() throws AttivioException {
    SampleConcurrentDataSourceScanner sampleScanner = new SampleConcurrentDataSourceScanner();
    sampleScanner.setTestText(HELLO_WORLD);
    TestScannerRunner scannerRunner =
        SdkTestUtils.createTestScannerRunner("sampleScannerConnector", sampleScanner);
    scannerRunner.start();
    DocumentList documentList = (DocumentList) scannerRunner.getSentMessages().get(0);
    Assert.assertEquals(20, documentList.size());

    for (IngestDocument doc : documentList) {
      if (doc.getFirstValue(FieldNames.TEXT).stringValue().equals(HELLO_WORLD)) return;
    }
    Assert.fail("A document with 'Hello World' text was not published");
  }

  /** Test stopping the concurrent scanner */
  @Test
  public void testStop() throws AttivioException {
    SampleConcurrentDataSourceScanner sampleScanner = new SampleConcurrentDataSourceScanner();
    sampleScanner.setTestText(HELLO_WORLD);
    TestScannerRunner scannerRunner =
        SdkTestUtils.createTestScannerRunner("sampleScannerConnector", sampleScanner);

    // Stopping after 5 seconds
    new Thread() {
      @Override
      public void run() {
        nap(5000);
        scannerRunner.stop();
      }
    }.start();

    scannerRunner.start();

    DocumentList documentList = (DocumentList) scannerRunner.getSentMessages().get(0);
    int listSize = documentList.size();
    System.out.println(listSize + " published");
    // Since we stopped after 5 seconds not all the documents are published
    Assert.assertTrue(listSize < 20 && listSize > 0);

    for (IngestDocument doc : documentList) {
      if (doc.getFirstValue(FieldNames.TEXT).stringValue().equals(HELLO_WORLD)) return;
    }
    Assert.fail("A document with 'Hello World' text was not published");
  }

  private void nap(long millis) {
    try {
      Thread.sleep(millis);
    } catch (Exception e) {

    }
  }
}
