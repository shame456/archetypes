/** Copyright 2019 Attivio Inc., All rights reserved. */
package com.attivio.emodules.deletedatafeedfolders;

import com.attivio.sdk.AttivioException;
import com.attivio.sdk.ingest.DocumentList;
import com.attivio.sdk.ingest.IngestDocument;
import com.attivio.sdk.ingest.IngestField;
import com.attivio.sdk.scanner.TestScannerRunner;
import com.attivio.sdk.test.SdkTestUtils;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;

/** Run SampleDataSourceScannerUI using SdkTestUtils */
public class SampleDataSourceScannerUITest {
  private static final String[] ITEM_LIST_ARRAY = new String[] {"first", "second", "third"};
  private static final List<String> ITEM_LIST = Arrays.asList(ITEM_LIST_ARRAY);
  public static final Map<String, String> ITEM_MAP =
      new HashMap<String, String>() {
        {
          put("field1_s", "value1");
          put("field2_s", "value2");
          put("field3_s", "value3");
        }
      };

  // Note that the _s postfix of the field name create a dynamic field - it doen't have to be
  // defines statically in the schema to be indexed
  private static final String LIST_FIELD = "somefield_s";

  @Test
  public void test() throws AttivioException {
    SampleDataSourceScannerUI sampleScanner = new SampleDataSourceScannerUI();
    sampleScanner.setListField(LIST_FIELD);
    sampleScanner.setListItems(ITEM_LIST);
    sampleScanner.setMapItems(ITEM_MAP);

    TestScannerRunner scannerRunner =
        SdkTestUtils.createTestScannerRunner("sampleScannerConnector", sampleScanner);
    scannerRunner.start();
    DocumentList documentList = (DocumentList) scannerRunner.getSentMessages().get(0);
    Assert.assertEquals(1, documentList.size());

    IngestDocument doc = documentList.get(0);
    validateList(doc, doc.getField(LIST_FIELD));
    validateItem(doc, "field1_s");
    validateItem(doc, "field2_s");
    validateItem(doc, "field3_s");
  }

  private void validateList(IngestDocument doc, IngestField field) {
    Assert.assertTrue(field.getValue(0).stringValue().equals(ITEM_LIST.get(0)));
    Assert.assertTrue(field.getValue(1).stringValue().equals(ITEM_LIST.get(1)));
    Assert.assertTrue(field.getValue(2).stringValue().equals(ITEM_LIST.get(2)));
  }

  private void validateItem(IngestDocument doc, String fieldName) {
    Assert.assertTrue(
        doc.getField(fieldName).getValue(0).stringValue().equals(ITEM_MAP.get(fieldName)));
  }
}
