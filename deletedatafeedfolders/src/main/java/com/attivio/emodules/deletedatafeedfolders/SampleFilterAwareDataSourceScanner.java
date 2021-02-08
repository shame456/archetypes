package com.attivio.emodules.deletedatafeedfolders;

import com.attivio.sdk.AttivioException;
import com.attivio.sdk.connector.DocumentPublisher;
import com.attivio.sdk.error.ConnectorError;
import com.attivio.sdk.ingest.IngestDocument;
import com.attivio.sdk.scanner.DataSourceScanner;
import com.attivio.sdk.scanner.IncludeExcludeDocumentFilter;
import com.attivio.sdk.scanner.IngestDocumentFilterAware;
import com.attivio.sdk.schema.FieldNames;
import com.attivio.sdk.server.annotation.ConfigurationOption;
import com.attivio.sdk.server.annotation.ConfigurationOptionInfo;
import com.attivio.sdk.server.annotation.ScannerInfo;
import java.util.ArrayList;
import java.util.List;

/**
 * Demonstrates how to use the {@link IngestDocumentFilterAware} and {@link
 * IncludeExcludeDocumentFilter} interfaces to feed only included documents.
 */
@ScannerInfo(
    suggestedWorkflow =
        "ingest") // Use fileIngest for large documents and documents with complex formats such as
// PDF files
@ConfigurationOptionInfo(
    displayName = "Document filter Sample Scanner",
    description = "An example for an IngestDocumentFilterAware scanner",
    groups = {
      @ConfigurationOptionInfo.Group(
          path = ConfigurationOptionInfo.SCANNER,
          propertyNames = {"testText", "textInclude", "textExclude", "nameSpaceToInclude"})
    })
public class SampleFilterAwareDataSourceScanner
    implements DataSourceScanner, IngestDocumentFilterAware {
  private static final String FOLDER_FILTER_NAME = "folders-filter";

  private List<String> testText;

  private List<String> textInclude;

  private List<String> textExclude;

  private List<String> inclusionFolders;

  private IncludeExcludeDocumentFilter docTextFilter;

  @ConfigurationOption(
      longOpt = "folders",
      displayName = "Inclusion folders",
      description = "Only documents from these top folders will get included",
      formEntryClass = ConfigurationOption.STRING_LIST)
  public List<String> getInclusionFolder() {
    return inclusionFolders;
  }

  public void setInclusionFolder(List<String> inclusionFolders) {
    this.inclusionFolders = inclusionFolders;
  }

  @ConfigurationOption(
      longOpt = "get-include",
      displayName = "Regex Inclusions",
      description = "Get a list of regex inclusions",
      formEntryClass = ConfigurationOption.STRING_LIST)
  public List<String> getTextInclude() {
    return textInclude;
  }

  public void setTextInclude(List<String> textInclude) {
    this.textInclude = textInclude;
  }

  @ConfigurationOption(
      longOpt = "get-exclude",
      displayName = "Regex Exclusions",
      description = "Get a list of regex exclusions",
      formEntryClass = ConfigurationOption.STRING_LIST)
  public List<String> getTextExclude() {
    return textExclude;
  }

  public void setTextExclude(List<String> textExclude) {
    this.textExclude = textExclude;
  }

  @ConfigurationOption(
      longOpt = "set-test-text",
      displayName = "Test Text",
      description = "Text to be placed in the document",
      formEntryClass = ConfigurationOption.STRING_LIST)
  public List<String> getTestText() {
    return testText;
  }

  public void setTestText(List<String> testText) {
    this.testText = testText;
  }

  @Override
  public void setFilter(IncludeExcludeDocumentFilter filter) {
    this.docTextFilter = filter;
    filter.addFilter(FieldNames.TEXT, textInclude, textExclude, false);

    // Create regex to include all the folders and documents under the top folder
    List<String> folderRegexList = new ArrayList<String>();
    for (String folder : inclusionFolders) folderRegexList.add(folder + "/.*");

    filter.addFilter(FOLDER_FILTER_NAME, folderRegexList, null, false);
  }

  @Override
  public void start(String name, DocumentPublisher publisher) throws AttivioException {
    int docId = 0;
    String folderName = "/myDocuments/archive"; // We pretend that these documents come from the
    // "/myDocuments/archive" folder
    for (String textItem : testText) {

      // Is this document coming from a folder we should include?
      if (!docTextFilter.isIncluded(folderName, FOLDER_FILTER_NAME)) continue;

      IngestDocument doc = new IngestDocument(new Integer(docId++).toString());
      doc.addValue(FieldNames.TEXT, textItem);
      if (docTextFilter.isIncluded(doc, FieldNames.TEXT)) publisher.feed(doc);
    }
  }

  @Override
  public void validateConfiguration() throws AttivioException {
    if (testText == null)
      throw new AttivioException(
          ConnectorError.CONFIGURATION_ERROR, "Test text was not configured");
  }
}
