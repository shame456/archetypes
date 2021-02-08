/** Copyright 2018 Attivio Inc., All rights reserved. */
package com.attivio.emodules.deletedatafeedfolders;

import com.attivio.sdk.AttivioException;
import com.attivio.sdk.ingest.DocumentList;
import com.attivio.sdk.ingest.IngestDocument;
import com.attivio.sdk.scanner.TestScannerRunner;
import com.attivio.sdk.schema.FieldNames;
import com.attivio.sdk.test.SdkTestUtils;
import java.util.Arrays;
import org.junit.Assert;
import org.junit.Test;

/** Run a simple test scanner using SdkTestUtils */
public class SampleFilterAwareDataSourceScannerTest {
  private static final String TEXT1 = "Hello";
  private static final String TEXT2 = "World";
  private static final String TEXT3 = "Hello Crazy World";
  private static final String TEXT4 = "Universe";
  private static String[] TEXT_LIST = {TEXT1, TEXT2, TEXT3, TEXT4};

  private static final String INCLUDED_FOLDER = "/myDocuments";

  private static final String INCLUDE1 = ".*Hello.*";
  private static final String INCLUDE2 = ".*World.*";
  private static final String EXCLUDE1 = ".*Crazy.*";

  private static String[] INCLUDE_FOLDER_LIST_FOR_POSITIVE_TEST = {INCLUDED_FOLDER};
  private static String[] INCLUDE_FOLDER_LIST_FOR_NEGATIVE_TEST = {INCLUDED_FOLDER + "bogus"};
  private static String[] INCLUDE_LIST = {INCLUDE1, INCLUDE2};
  private static String[] EXCLUDE_LIST = {EXCLUDE1};

  @Test
  public void positiveTest() throws AttivioException {
    doTest(true);
  }

  @Test
  public void negativeTestTest() throws AttivioException {
    doTest(false);
  }

  private void doTest(boolean positive) throws AttivioException {
    SampleFilterAwareDataSourceScanner sampleScanner = new SampleFilterAwareDataSourceScanner();

    // Only documents under these top folders will get published
    sampleScanner.setInclusionFolder(
        Arrays.asList(
            positive
                ? INCLUDE_FOLDER_LIST_FOR_POSITIVE_TEST
                : INCLUDE_FOLDER_LIST_FOR_NEGATIVE_TEST));

    sampleScanner.setTestText(Arrays.asList(TEXT_LIST));
    sampleScanner.setTextInclude(Arrays.asList(INCLUDE_LIST));
    sampleScanner.setTextExclude(Arrays.asList(EXCLUDE_LIST));

    TestScannerRunner scannerRunner =
        SdkTestUtils.createTestScannerRunner("sampleScannerConnector", sampleScanner);
    scannerRunner.start();

    // No document were published in the negative test case since
    // INCLUDE_FOLDER_LIST_FOR_NEGATIVE_TEST included folder list will cause the scanner to avoid
    // publishing any documents
    if (!positive) {
      Assert.assertEquals(0, scannerRunner.getSentMessages().size());
      return;
    }

    DocumentList documentList = (DocumentList) scannerRunner.getSentMessages().get(0);
    Assert.assertEquals(2, documentList.size());

    for (IngestDocument doc : documentList) {
      String text = doc.getFirstValue(FieldNames.TEXT).stringValue();
      Assert.assertTrue(
          (text.indexOf(TEXT1) != -1 || text.indexOf(TEXT2) != -1) && text.indexOf("Crazy") == -1);
    }
  }
}
