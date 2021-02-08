/** Copyright 2019 Attivio Inc., All rights reserved. */
package com.attivio.emodules.deletedatafeedfolders;

import com.attivio.sdk.AttivioException;
import com.attivio.sdk.ingest.DocumentList;
import com.attivio.sdk.ingest.IngestDocument;
import com.attivio.sdk.scanner.TestScannerRunner;
import com.attivio.sdk.test.SdkTestUtils;
import org.junit.Assert;
import org.junit.Test;

/** Run a SampleParentChildAssociationScanner using SdkTestUtils */
public class SampleParentChildAssociationScannerTest {
  private int numTests = 0;

  @Test
  public void test() throws AttivioException {
    SampleParentChildAssociationScanner sampleScanner = new SampleParentChildAssociationScanner();

    TestScannerRunner scannerRunner =
        SdkTestUtils.createTestScannerRunner("sampleScannerConnector", sampleScanner);
    scannerRunner.start();
    DocumentList documentList = (DocumentList) scannerRunner.getSentMessages().get(0);
    Assert.assertEquals(4, documentList.size());

    for (IngestDocument doc : documentList) {
      if (doc.getId().equals("1")) testRoot(doc);
      if (doc.getId().equals("1b")) testDad(doc);
      if (doc.getId().equals("1b1")) testGrandChild(doc);

      System.out.println(doc);
    }
    Assert.assertEquals(3, numTests);
  }

  private void testRoot(IngestDocument doc) {
    Assert.assertEquals(2, doc.getFirstValue("childcount").intValue());
    numTests++;
  }

  private void testDad(IngestDocument doc) {
    Assert.assertEquals(1, doc.getFirstValue("childcount").intValue());
    Assert.assertEquals("1", doc.getFirstValue("parentid").stringValue());
    Assert.assertEquals(2, doc.getFirstValue("childindex").intValue());
    Assert.assertEquals(1, doc.getField("ancestorids").size());
    numTests++;
  }

  private void testGrandChild(IngestDocument doc) {
    Assert.assertNull(doc.getFirstValue("childcount"));
    Assert.assertEquals(2, doc.getField("ancestorids").size());
    Assert.assertEquals(1, doc.getFirstValue("childindex").intValue());
    Assert.assertEquals("1b", doc.getFirstValue("parentid").stringValue());
    numTests++;
  }
}
