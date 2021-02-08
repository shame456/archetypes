package com.attivio.emodules.deletedatafeedfolders;

import com.attivio.sdk.AttivioException;
import com.attivio.sdk.connector.DocumentPublisher;
import com.attivio.sdk.ingest.IngestDocument;
import com.attivio.sdk.scanner.DataSourceScanner;
import com.attivio.sdk.scanner.ParentChildAssociationAware;
import com.attivio.sdk.scanner.ParentChildDocumentAssociation;
import com.attivio.sdk.server.annotation.ConfigurationOptionInfo;
import com.attivio.sdk.server.annotation.ScannerInfo;

/** An example for associating parent and child documents */
@ScannerInfo(
    suggestedWorkflow =
        "ingest") // Use fileIngest for large documents and documents with complex formats such as
// PDF files
@ConfigurationOptionInfo(
    displayName = "Parent Child Association Scanner",
    description = "A scanner that uses the SDK to associate parent and child documents",
    groups = {
      @ConfigurationOptionInfo.Group(
          path = ConfigurationOptionInfo.SCANNER,
          propertyNames = {"parentChildDocumentAssociation"})
    })
public class SampleParentChildAssociationScanner
    implements DataSourceScanner, ParentChildAssociationAware {
  private ParentChildDocumentAssociation parentChildDocumentAssociation;

  @Override
  public void setParentChildDocumentAssociation(
      ParentChildDocumentAssociation parentChildDocumentAssociation) {
    this.parentChildDocumentAssociation = parentChildDocumentAssociation;
  }

  @Override
  public void start(String name, DocumentPublisher publisher) throws AttivioException {
    IngestDocument root = new IngestDocument("1");
    IngestDocument child1 = new IngestDocument("1a");
    IngestDocument child2 = new IngestDocument("1b");
    IngestDocument grandChild = new IngestDocument("1b1");

    parentChildDocumentAssociation.noteChild(root, child1);
    parentChildDocumentAssociation.noteChild(root, child2);
    parentChildDocumentAssociation.noteChild(child2, grandChild);

    publisher.feed(root);
    publisher.feed(child1);
    publisher.feed(child2);
    publisher.feed(grandChild);
  }
}
