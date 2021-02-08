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

/** Tests SampleThrottlerAwareDataSourceScanner using SdkTestUtils */
public class SampleThrottlingDataSourceScannerTest {
  private static final String HELLO_WORLD = "Hello World";

  @Test
  public void test() throws AttivioException {
    SampleThrottlerAwareDataSourceScanner sampleScanner =
        new SampleThrottlerAwareDataSourceScanner();

    // Setting the upper limit of documents that will be fetched per 1000 millis (one second).
    sampleScanner.setDocumentRequestsUpperLimit(3);

    sampleScanner.setTestText(HELLO_WORLD);
    TestScannerRunner scannerRunner =
        SdkTestUtils.createTestScannerRunner("sampleScannerConnector", sampleScanner);

    long startTime = System.currentTimeMillis();
    scannerRunner.start();

    // Because of throttling scanning takes at least 5 seconds. Will be sub-second without
    // throttling
    Assert.assertTrue(System.currentTimeMillis() - startTime > 5000);

    DocumentList documentList = (DocumentList) scannerRunner.getSentMessages().get(0);
    Assert.assertEquals(20, documentList.size());

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
