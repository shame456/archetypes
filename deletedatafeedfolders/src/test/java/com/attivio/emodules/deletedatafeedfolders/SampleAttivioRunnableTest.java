package com.attivio.emodules.deletedatafeedfolders;

import com.attivio.client.MockSearchClient;
import com.attivio.sdk.client.SearchClient;
import com.attivio.sdk.commandline.AttivioRunnable;
import com.attivio.sdk.search.SearchDocument;
import com.attivio.sdk.service.ServiceFactory;
import com.attivio.sdk.service.ServiceFactoryFactory;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import org.junit.Assert;
import org.junit.Test;

public class SampleAttivioRunnableTest {

  @Test
  public void runnableTest() throws Exception {

    // add a document to the MockSearchClient's cache so the runnable finds something
    String docId = "mydoc";
    ServiceFactory serviceFactory = ServiceFactoryFactory.get();
    MockSearchClient mockSearchClient =
        (MockSearchClient) serviceFactory.getService(SearchClient.class);
    mockSearchClient.add(new SearchDocument(docId));

    // redirect system out so we can test what is written
    PrintStream original = System.out;
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    PrintStream redirect = new PrintStream(out);
    System.setOut(redirect);

    // run the runnable
    SampleAttivioRunnable runnable = new SampleAttivioRunnable();
    Assert.assertEquals(AttivioRunnable.RETURN_CODE_OK, runnable.run());

    // check for the document
    String outString = out.toString();
    Assert.assertTrue(outString.startsWith("DOCUMENT: " + docId));

    // reset the standard out (so any subsequent tests are not affected)
    System.setOut(original);
  }
}
