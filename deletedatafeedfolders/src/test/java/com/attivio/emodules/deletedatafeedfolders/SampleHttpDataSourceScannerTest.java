/** Copyright 2019 Attivio Inc., All rights reserved. */
package com.attivio.emodules.deletedatafeedfolders;

import com.attivio.sdk.AttivioException;
import com.attivio.sdk.esb.PlatformMessage;
import com.attivio.sdk.ingest.DocumentList;
import com.attivio.sdk.ingest.IngestDocument;
import com.attivio.sdk.scanner.DataSourceScanner;
import com.attivio.sdk.scanner.TestScannerRunner;
import com.attivio.sdk.scanner.http.BasicAuthentication;
import com.attivio.sdk.schema.FieldNames;
import com.attivio.sdk.test.SdkTestUtils;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests the {@link SampleHttpDataSourceScanner}. The test retrieves the content from the Google web
 * site url and sets as text in the Attivio document. Since the Google site does not require
 * authentication the user/password and the other {@link BasicAuthentication} parameters are not
 * actually used. To actually test Basic authentication, this test can be modified by replacing the
 * Google url with a url of a site that is protected by Http Basic authentication. Then, modify the
 * {@code configureBasicAuthentication} method to set the correct parameters and observe the {@link
 * FieldNames.TEXT} value in the {@link IngestDocument}.
 */
public class SampleHttpDataSourceScannerTest {
  protected static final String TEST_URL = "http://www.google.com";
  private static final String TEXT_IN_GOOGLE_WEBSITE = "Search";

  @Test
  public void test() throws AttivioException {
    DataSourceScanner scanner = createScanner();
    TestScannerRunner scannerRunner =
        SdkTestUtils.createTestScannerRunner("testConnector", scanner);

    configureBasicAuthentication((BasicAuthentication) scanner);
    scannerRunner.start();
    List<PlatformMessage> publishedMessages = scannerRunner.getSentMessages();
    DocumentList documentList = (DocumentList) publishedMessages.get(0);
    Assert.assertEquals(1, documentList.size());

    for (IngestDocument doc : documentList) {
      if (doc.getFirstValue(FieldNames.TEXT).stringValue().contains(TEXT_IN_GOOGLE_WEBSITE)) return;
    }
    Assert.fail("A document with 'Search' text was not published");
  }

  protected DataSourceScanner createScanner() {
    DataSourceScanner scanner = new SampleHttpDataSourceScanner();
    ((SampleHttpDataSourceScanner) scanner).setContentUrl(TEST_URL);
    return scanner;
  }

  private void configureBasicAuthentication(BasicAuthentication scanner) {
    scanner.setBasicAuthLogin("user");
    scanner.setBasicAuthPassword("password");
    scanner.setBasicAuthHost("localhost");
    scanner.setBasicAuthPort(-1);
    scanner.setBasicAuthRealm("realm");
  }
}
