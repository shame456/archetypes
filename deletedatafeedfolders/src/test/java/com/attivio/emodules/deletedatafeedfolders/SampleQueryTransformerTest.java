/** Copyright 2019 Attivio Inc., All rights reserved. */
package com.attivio.emodules.deletedatafeedfolders;

import com.attivio.sdk.AttivioException;
import com.attivio.sdk.search.QueryFeedback;
import com.attivio.sdk.search.QueryRequest;
import com.attivio.sdk.search.query.PhraseQuery;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;

/** */
public class SampleQueryTransformerTest {

  @Test
  public void queryTest() throws AttivioException {
    SampleQueryTransformer qt = new SampleQueryTransformer();

    PhraseQuery input = new PhraseQuery("test");
    QueryRequest queryRequest = new QueryRequest(input);

    List<QueryFeedback> feedback = qt.processQuery(queryRequest);

    Assert.assertEquals(1, queryRequest.getFacets().size());
    Assert.assertEquals(1, feedback.size());
    Assert.assertEquals(qt.getFacetField(), queryRequest.getFacets().get(0).getField());
  }
}
