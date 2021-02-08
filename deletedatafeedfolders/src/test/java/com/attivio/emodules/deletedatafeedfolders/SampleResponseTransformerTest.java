/** Copyright 2019 Attivio Inc., All rights reserved. */
package com.attivio.emodules.deletedatafeedfolders;

import com.attivio.sdk.AttivioException;
import com.attivio.sdk.search.QueryRequest;
import com.attivio.sdk.search.QueryResponse;
import com.attivio.sdk.search.SearchDocumentList;
import org.junit.Assert;
import org.junit.Test;

/** */
public class SampleResponseTransformerTest {

  @Test
  public void responseTest() throws AttivioException {
    SampleResponseTransformer rt = new SampleResponseTransformer();
    QueryResponse resp = new QueryResponse(new QueryRequest("foo"));

    // first check response info processing
    rt.processResponseInfo(resp);
    Assert.assertEquals(1, resp.getFeedback().size());

    // then check response document processing
    SearchDocumentList documents = new SearchDocumentList();
    rt.processResponseDocuments(resp, documents);
    Assert.assertEquals(1, documents.size());
    Assert.assertEquals(2, resp.getFeedback().size());
  }
}
