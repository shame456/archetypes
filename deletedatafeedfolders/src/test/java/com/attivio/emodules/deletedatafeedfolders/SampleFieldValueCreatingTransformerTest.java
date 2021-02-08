/** Copyright 2019 Attivio Inc., All rights reserved. */
package com.attivio.emodules.deletedatafeedfolders;

import com.attivio.sdk.AttivioException;
import com.attivio.sdk.ingest.IngestDocument;
import com.attivio.sdk.ingest.IngestField;
import com.attivio.sdk.ingest.IngestFieldValue;
import com.attivio.sdk.test.DocumentAssert;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;

public class SampleFieldValueCreatingTransformerTest {

  private String inputField = "testin";
  private String outputField = "testout";

  @Test
  public void testTransformer() throws AttivioException {
    // Create my document transformer as a regular object
    SampleFieldValueCreatingTransformer xformer = new SampleFieldValueCreatingTransformer();

    // Field
    Map<String, String> tmpMap = new HashMap<>();
    tmpMap.put(inputField, outputField);
    xformer.setFieldMapping(tmpMap);
    IngestDocument doc = new IngestDocument("doc0001");
    doc.setField(inputField, "THIS IS A SAMPLE UPPER CASE DOCUMENT.");
    IngestField f = doc.getField(inputField);

    for (IngestFieldValue fv : f) {
      IngestFieldValue tmp = xformer.createMappedValue(inputField, fv);
      String outF = xformer.getFieldMapping().get(inputField);
      if (outF != null) {
        doc.addValue(outF, tmp);
      }
    }

    // print the document to see what it looks like
    System.err.println(doc.toString());

    // Assert that the document's text field is now all in lower case
    DocumentAssert.assertFieldValue(doc, outputField, "this is a sample upper case document.");
  }
}
