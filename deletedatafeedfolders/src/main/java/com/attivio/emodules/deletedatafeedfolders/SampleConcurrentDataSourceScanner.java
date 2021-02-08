package com.attivio.emodules.deletedatafeedfolders;

import com.attivio.sdk.AttivioException;
import com.attivio.sdk.connector.DocumentPublisher;
import com.attivio.sdk.error.ConnectorError;
import com.attivio.sdk.ingest.IngestDocument;
import com.attivio.sdk.scanner.DataSourceScanner;
import com.attivio.sdk.scanner.concurrency.ConcurrencyProvider;
import com.attivio.sdk.scanner.concurrency.ConcurrentDocumentFetcherScanner;
import com.attivio.sdk.scanner.concurrency.ContentStoreAccess;
import com.attivio.sdk.scanner.concurrency.DocumentAndAcl;
import com.attivio.sdk.scanner.concurrency.DocumentFetchRequest;
import com.attivio.sdk.schema.FieldNames;
import com.attivio.sdk.server.annotation.ConfigurationOption;
import com.attivio.sdk.server.annotation.ConfigurationOptionInfo;
import com.attivio.sdk.server.annotation.ScannerInfo;

/**
 * A scanner that demonstrates the use of the concurrent scanner API. The {@link #start} method of
 * this sample doesn't publish document directly but rather creates {@link
 * SampleDocumentFetchRequest} objects and queues them at the {@link ConcurrencyProvider} object
 * passed to it by the connector framework.
 */
@ScannerInfo(
    suggestedWorkflow =
        "ingest") // Use fileIngest for large documents and documents with complex formats such as
// PDF files
@ConfigurationOptionInfo(
    displayName = "Concurrent Sample Scanner",
    description = "An example for a concurrent scanner",
    groups = {
      @ConfigurationOptionInfo.Group(
          path = ConfigurationOptionInfo.SCANNER,
          propertyNames = {"testText"})
    })
public class SampleConcurrentDataSourceScanner
    implements DataSourceScanner, ConcurrentDocumentFetcherScanner {
  private String testText;
  private boolean stopped = false;
  private ConcurrencyProvider concurrencyProvider;

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
   * @see com.attivio.sdk.scanner.DataSourceScanner#start(java.lang.String, com.attivio.sdk.connector.DocumentPublisher)
   */
  @Override
  public void start(String name, DocumentPublisher publisher) throws AttivioException {
    // When the concurrency provider is used, the scanner doesn't publish document directly but
    // rather queue a ocumentFetchRequest object.
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
      // Simulates the time it takes to fetch the content of a document
      nap(300);

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
