/** Copyright 2019 Attivio Inc., All rights reserved. */
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
public class SampleDataSourceScannerTest {
  private static final String HELLO_WORLD = "Hello World";

  @Test
  public void test() throws AttivioException {
    SampleDataSourceScanner sampleScanner = new SampleDataSourceScanner();
    sampleScanner.setTestText(HELLO_WORLD);
    TestScannerRunner scannerRunner =
        SdkTestUtils.createTestScannerRunner("sampleScannerConnector", sampleScanner);
    scannerRunner.start();
    DocumentList documentList = (DocumentList) scannerRunner.getSentMessages().get(0);
    Assert.assertEquals(1, documentList.size());

    for (IngestDocument doc : documentList) {
      if (doc.getFirstValue(FieldNames.TEXT).stringValue().equals(HELLO_WORLD)) return;
    }
    Assert.fail("A document with 'Hello World' text was not published");
  }
}
