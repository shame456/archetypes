/** Copyright 2019 Attivio Inc., All rights reserved. */
package com.attivio.emodules.deletedatafeedfolders;

import com.attivio.sdk.scanner.DataSourceScanner;

public class SampleCustomHttpDataSourceScannerTest extends SampleHttpDataSourceScannerTest {
  @Override
  protected DataSourceScanner createScanner() {
    DataSourceScanner scanner = new SampleCustomHttpDataSourceScanner();
    ((SampleCustomHttpDataSourceScanner) scanner).setContentUrl(TEST_URL);
    return scanner;
  }
}
