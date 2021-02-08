/** Copyright 2019 Attivio Inc., All rights reserved. */
package com.attivio.emodules.deletedatafeedfolders;

import com.attivio.sdk.AttivioException;
import com.attivio.sdk.scanner.DataSourceScanner;
import com.attivio.sdk.scanner.TestScannerRunner;
import com.attivio.sdk.scanner.http.BasicAuthentication;
import com.attivio.sdk.test.SdkTestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * This test demonstrates the running of the {@link SampleAPIErrorDescriptorsScanner} scanner. The
 * test shows how error handling is driven by the error definitions in the
 * SampleAPIErrorDescriptorsScanner_errors.json file.
 */
public class SampleAPIErrorDescriptorsScannerTest {
  protected static final String DOES_NOT_EXIST_URL = "http://www.somejunzzz.com";
  protected static final String BAD_PROTOCOL_URL = "xxx://www.somejunzzz.com";

  @Test
  public void testFatalError() throws AttivioException {
    try {
      doTest(DOES_NOT_EXIST_URL);
      Assert.fail(
          "Should not get here because we configured the java.net.UnknownHostException to be fatal and so it should throw an exception");
    } catch (Exception e) {
      Assert.assertTrue(e.getMessage().contains("Unknown host in the configured url"));
    }
  }

  @Test
  public void testBadProtocolError() throws AttivioException {
    try {
      doTest(BAD_PROTOCOL_URL);
    } catch (Exception e) {
      Assert.fail(
          "Should not get here because we configured the org.apache.http.client.ClientProtocolException to be non fatal and so it should not throw an exception");
    }
  }

  private void doTest(String url) throws AttivioException {
    DataSourceScanner scanner = createScanner(url);
    TestScannerRunner scannerRunner =
        SdkTestUtils.createTestScannerRunner("testConnector", scanner);

    configureBasicAuthentication((BasicAuthentication) scanner);
    scannerRunner.start();
  }

  protected DataSourceScanner createScanner(String url) {
    DataSourceScanner scanner = new SampleAPIErrorDescriptorsScanner();
    ((SampleAPIErrorDescriptorsScanner) scanner).setContentUrl(url);
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
