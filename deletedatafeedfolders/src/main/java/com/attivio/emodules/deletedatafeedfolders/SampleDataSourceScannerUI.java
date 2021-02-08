package com.attivio.emodules.deletedatafeedfolders;

import com.attivio.sdk.AttivioException;
import com.attivio.sdk.connector.DocumentPublisher;
import com.attivio.sdk.error.ConnectorError;
import com.attivio.sdk.ingest.IngestDocument;
import com.attivio.sdk.scanner.DataSourceScanner;
import com.attivio.sdk.server.annotation.ConfigurationOption;
import com.attivio.sdk.server.annotation.ConfigurationOptionInfo;
import com.attivio.sdk.server.annotation.ScannerInfo;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * An example for configuring a scanner with lists and maps. The configured values are added to a
 * document to demonstrate multiple field values and dynamic fields.
 */
@ScannerInfo(
    suggestedWorkflow =
        "ingest") // Use fileIngest for large documents and documents with complex formats such as
// PDF files
@ConfigurationOptionInfo(
    displayName = "Scanner UI Configuration",
    description =
        "An example for list and map configuration items, multiple value fiedls and dynamic fields",
    groups = {
      @ConfigurationOptionInfo.Group(
          path = ConfigurationOptionInfo.SCANNER,
          propertyNames = {"listItems", "mapItems"})
    })
public class SampleDataSourceScannerUI implements DataSourceScanner {
  private String listField;
  private List<String> listItems;
  private Map<String, String> mapItems;

  @ConfigurationOption(
      longOpt = "list-items",
      displayName = "List Items",
      description = "A list of items to be put in an Attivio document field",
      formEntryClass = ConfigurationOption.STRING_LIST)
  public List<String> getListItems() {
    return listItems;
  }

  public void setListItems(List<String> listItems) {
    this.listItems = listItems;
  }

  @ConfigurationOption(
      longOpt = "map-items",
      displayName = "Map Items",
      description = "A field-value map to be put in the Attivio document",
      formEntryClass = ConfigurationOption.STRING_TO_STRING_MAP)
  public Map<String, String> getMapItems() {
    return mapItems;
  }

  public void setMapItems(Map<String, String> mapItems) {
    this.mapItems = mapItems;
  }

  @ConfigurationOption(
      longOpt = "list-field",
      displayName = "List Field",
      description = "A field to populate with list items")
  public String getListField() {
    return listField;
  }

  public void setListField(String listField) {
    this.listField = listField;
  }

  @Override
  public void start(String name, DocumentPublisher publisher) throws AttivioException {
    IngestDocument doc = new IngestDocument("1");

    populateListItems(doc, "somefield_s");
    populateMapItems(doc);

    publisher.feed(doc);
  }

  @Override
  public void validateConfiguration() throws AttivioException {
    if (listField == null)
      throw new AttivioException(
          ConnectorError.CONFIGURATION_ERROR, "List field was not configured");
  }

  private void populateListItems(IngestDocument doc, String fieldName) {
    if (listItems != null) {
      for (String item : listItems) {
        doc.addValue(fieldName, item);
      }
    }
  }

  private void populateMapItems(IngestDocument doc) {
    if (mapItems != null) {

      for (Entry<String, String> entry : mapItems.entrySet()) {
        doc.addValue(entry.getKey(), entry.getValue());
      }
    }
  }
}
