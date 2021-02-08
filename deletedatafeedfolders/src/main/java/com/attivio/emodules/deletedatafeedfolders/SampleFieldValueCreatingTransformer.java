/** Copyright 2019 Attivio Inc., All rights reserved. */
package com.attivio.emodules.deletedatafeedfolders;

import com.attivio.sdk.AttivioException;
import com.attivio.sdk.error.IndexWorkflowError;
import com.attivio.sdk.ingest.IngestFieldValue;
import com.attivio.sdk.server.annotation.ConfigurationOptionInfo;
import com.attivio.sdk.server.component.ingest.FieldValueCreatingTransformer;
import java.util.Map;

/**
 * Sample document transformer demonstrating implementation of a FieldValueCreatingTransformer. In
 * this example, a new field value is created that will contain the lower-case version of the input
 * field's value.
 */
@ConfigurationOptionInfo(
    displayName = "Sample Document Transformer",
    description = "Advanced transformer sample code provided by the SDK",
    groups = {
      @ConfigurationOptionInfo.Group(
          path = ConfigurationOptionInfo.PLATFORM_COMPONENT,
          propertyNames = {"fieldMapping"})
    })
public class SampleFieldValueCreatingTransformer implements FieldValueCreatingTransformer<String> {

  private Map<String, String> fieldMapping = null;

  @Override
  public IngestFieldValue createMappedValue(String inputFieldName, IngestFieldValue fv)
      throws AttivioException {
    if (fv.getValue() instanceof String) {
      String tmp = fv.stringValue();
      tmp = tmp.toLowerCase();
      return new IngestFieldValue(tmp);
    } else {
      // throw an exception. this will cause the document to get a processing result of FAIL.
      throw new AttivioException(
          IndexWorkflowError.INVALID_FIELD_TYPE, "Field %s is not a String field", inputFieldName);
    }
  }

  @Override
  public Map<String, String> getFieldMapping() {
    return fieldMapping;
  }

  @Override
  public void setFieldMapping(Map<String, String> value) {
    fieldMapping = value;
  }
}
