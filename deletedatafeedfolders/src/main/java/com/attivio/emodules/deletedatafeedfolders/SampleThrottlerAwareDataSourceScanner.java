package com.attivio.emodules.deletedatafeedfolders;

import com.attivio.sdk.AttivioException;
import com.attivio.sdk.connector.DocumentPublisher;
import com.attivio.sdk.error.ConnectorError;
import com.attivio.sdk.ingest.IngestDocument;
import com.attivio.sdk.scanner.DataSourceScanner;
import com.attivio.sdk.scanner.concurrency.ConcurrencyProvider;
import com.attivio.sdk.scanner.concurrency.ConcurrentScannerThrottlingAware;
import com.attivio.sdk.scanner.concurrency.ContentStoreAccess;
import com.attivio.sdk.scanner.concurrency.DocumentAndAcl;
import com.attivio.sdk.scanner.concurrency.DocumentFetchRequest;
import com.attivio.sdk.schema.FieldNames;
import com.attivio.sdk.server.annotation.ConfigurationOption;
import com.attivio.sdk.server.annotation.ConfigurationOptionInfo;
import com.attivio.sdk.server.annotation.ScannerInfo;

/**
 * A scanner that demonstrates the use of the concurrent scanner API with throttling. the purpose of
 * throttling is to limit the number of requests to the underlying data source. Failing to do that
 * might cause the blocking of the connector by the data source server (typically a cloud service).
 * Observe in {@link SampleThrottlingDataSourceScannerTest} how {@link
 * ConcurrentScannerThrottlingAware} is configured.
 */
@ScannerInfo(
    suggestedWorkflow =
        "ingest") // Use fileIngest for large documents and documents with complex formats such as
// PDF files
@ConfigurationOptionInfo(
    displayName = "Sample Throttling Scanner",
    description = "An example of a concurrent scanner with throttling",
    groups = {
      @ConfigurationOptionInfo.Group(
          path = ConfigurationOptionInfo.SCANNER,
          propertyNames = {"testText", "documentRequestsUpperLimit"})
    })
public class SampleThrottlerAwareDataSourceScanner
    implements DataSourceScanner, ConcurrentScannerThrottlingAware {
  private String testText;
  private boolean stopped = false;
  private ConcurrencyProvider concurrencyProvider;
  private long documentRequestsUpperLimit;

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

  /* (non-Javadoc)
   * @see com.attivio.sdk.scanner.DataSourceScanner#start(String, DocumentPublisher)
   */
  @Override
  public void start(String name, DocumentPublisher publisher) throws AttivioException {
    // When the concurrency provider is used, the scanner doesn't publish document directly but
    // rather queues a documentFetchRequest object.
    // The connector framework executes multiple fetch requests concurrently.
    for (int i = 0; i < 20; i++) {
      if (stopped) break;

      concurrencyProvider.queue(new SampleDocumentFetchRequest("doc" + i));
    }
    concurrencyProvider.doneQueuing();
  }

  @Override
  public void validateConfiguration() throws AttivioException {
    if (testText == null)
      throw new AttivioException(
          ConnectorError.CONFIGURATION_ERROR, "Test text was not configured");
  }

  @Override
  public void setConcurrencyProvider(ConcurrencyProvider concurrencyProvider) {
    this.concurrencyProvider = concurrencyProvider;
  }

  @Override
  public void stop() {
    stopped = true;
  }

  @Override
  public long getDocumentRequestsUpperLimit() {
    return this.documentRequestsUpperLimit;
  }

  @Override
  public void setDocumentRequestsUpperLimit(long documentRequestsUpperLimit) {
    this.documentRequestsUpperLimit = documentRequestsUpperLimit;
  }

  /**
   * This class demonstrates how to implement document fetching requests that are queued for
   * concurrent fetching.
   */
  private class SampleDocumentFetchRequest implements DocumentFetchRequest {
    private String docId;

    SampleDocumentFetchRequest(String docId) {
      this.docId = docId;
    }

    @Override
    public void setContentStoreAccess(ContentStoreAccess contentStoreAccess) {
      // Not using the content store in this example
    }

    @Override
    public DocumentAndAcl getDocument() throws AttivioException {
      IngestDocument doc = new IngestDocument(docId);
      doc.addValue(FieldNames.TEXT, testText);
      return new DocumentAndAcl(doc, null);
    }

    private void nap(long millis) {
      try {
        Thread.sleep(millis);
      } catch (Exception e) {
      }
    }

    @Override
    public String name() {
      return docId;
    }

    @Override
    public void cancel() {
      // Not implemented yet;
    }

    @Override
    public void published() {
      System.out.println(docId + " is published");
    }
  }
}
