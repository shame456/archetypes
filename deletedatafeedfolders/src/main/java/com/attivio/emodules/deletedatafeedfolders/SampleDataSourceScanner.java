package com.attivio.emodules.deletedatafeedfolders;

import com.attivio.sdk.AttivioException;
import com.attivio.sdk.connector.DocumentPublisher;
import com.attivio.sdk.error.ConnectorError;
import com.attivio.sdk.ingest.IngestDocument;
import com.attivio.sdk.scanner.DataSourceScanner;
import com.attivio.sdk.schema.FieldNames;
import com.attivio.sdk.server.annotation.ConfigurationOption;
import com.attivio.sdk.server.annotation.ConfigurationOptionInfo;
import com.attivio.sdk.server.annotation.ScannerInfo;

/**
 * A simple sample scanner that puts some configured test text in a document and feeds the document.
 */
@ScannerInfo(
    suggestedWorkflow =
        "ingest") // Use fileIngest for large documents and documents with complex formats such as
// PDF files
@ConfigurationOptionInfo(
    displayName = "Simple Sample Scanner",
    description = "An example for a simple scanner",
    groups = {
      @ConfigurationOptionInfo.Group(
          path = ConfigurationOptionInfo.SCANNER,
          propertyNames = {"testText"})
    })
public class SampleDataSourceScanner implements DataSourceScanner {
  private String testText;

  @ConfigurationOption(
      longOpt = "set-test-text",
      displayName = "Test Text",
      description = "Text to be placed in the document")
  public String getTestText() {
    return testText;
  }

  public void setTestText(String testText) {
    this.testText = testText;
  }

  @Override
  public void start(String name, DocumentPublisher publisher) throws AttivioException {
    IngestDocument doc = new IngestDocument("1");
    doc.addValue(FieldNames.TEXT, testText);
    publisher.feed(doc);
  }

  @Override
  public void validateConfiguration() throws AttivioException {
    if (testText == null)
      throw new AttivioException(
          ConnectorError.CONFIGURATION_ERROR, "Test text was not configured");
  }
}
