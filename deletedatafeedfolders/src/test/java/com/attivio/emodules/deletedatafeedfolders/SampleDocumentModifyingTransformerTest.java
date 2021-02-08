/** Copyright 2019 Attivio Inc., All rights reserved. */
package com.attivio.emodules.deletedatafeedfolders;

import com.attivio.sdk.AttivioException;
import com.attivio.sdk.ingest.IngestDocument;
import com.attivio.sdk.test.DocumentAssert;
import org.junit.Test;

/** The simplest unit test of a document transformer. */
public class SampleDocumentModifyingTransformerTest {

  @Test
  public void testTransformer() throws AttivioException {
    // Initialize a transformer object
    SampleDocumentModifyingTransformer xformer = new SampleDocumentModifyingTransformer();
    xformer.setField("foo");
    xformer.setValue("bar");

    IngestDocument doc = new IngestDocument("1234");
    xformer.processDocument(doc);

    DocumentAssert.assertFieldValue(doc, "foo", "bar");
  }
}
