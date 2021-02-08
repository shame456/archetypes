/** Copyright 2021 Lucidworks Inc., All rights reserved. */
package com.attivio.emodules.deletedatafeedfolders.transformer.ingest.document;

import com.attivio.sdk.AttivioException;
import com.attivio.sdk.client.DocumentOutputClient;
import com.attivio.sdk.error.IndexWorkflowError;
import com.attivio.sdk.error.PlatformError;
import com.attivio.sdk.ingest.ContentPointer;
import com.attivio.sdk.ingest.IngestDocument;
import com.attivio.sdk.ingest.IngestField;
import com.attivio.sdk.ingest.IngestFieldValue;
import com.attivio.sdk.schema.FieldNames;
import com.attivio.sdk.server.annotation.ConfigurationOption;
import com.attivio.sdk.server.annotation.ConfigurationOptionInfo;
import com.attivio.sdk.server.component.configuration.AieLoggerAware;
import com.attivio.sdk.server.component.configuration.HasInputListProperty;
import com.attivio.sdk.server.component.ingest.MultiOutputDocumentTransformer;
import com.attivio.sdk.server.component.ingest.ProcessingFeedbackHandler;
import com.attivio.sdk.server.component.ingest.ProvidesProcessingFeedback;
import com.attivio.sdk.server.component.lifecycle.Startable;
import com.attivio.sdk.server.component.lifecycle.Stoppable;
import com.attivio.sdk.server.util.AieLogger;
import com.attivio.util.IOUtils;
import com.attivio.util.ObjectUtils;
import com.attivio.emodules.deletedatafeedfolders.connector.DatafeedWithAssetDelete;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.file.Path; 
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import org.jaxen.JaxenException;
import org.jaxen.SimpleNamespaceContext;
import org.jaxen.XPath;
import org.jaxen.dom.DOMXPath;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.apache.commons.io.FilenameUtils;

/**
 * Takes XML in a single field and splits it into sub-parts based on XPath expressions.
 *
 * <p>For example: if configured with:
 *
 * <pre>
 * &lt;map name="rules"&gt;
 *   &lt;property name="/docs/doc" value="@id" /&gt;
 * &lt;/map&gt;
 * </pre>
 *
 * will split a document that looks like:
 *
 * <pre class="code-xml">
 *  &lt;docs&gt;
 *    &lt;doc id=&quot;1&quot;&gt;&lt;title&gt;doc 1&lt;/title&gt;&lt;/doc&gt;
 *    &lt;doc id=&quot;2&quot;&gt;&lt;title&gt;doc 2&lt;/title&gt;&lt;/doc&gt;
 *    &lt;doc id=&quot;3&quot;&gt;&lt;title&gt;doc 3&lt;/title&gt;&lt;/doc&gt;
 *  &lt;/docs&gt;
 * </pre>
 *
 * Into three separate documents each with the XML for their node:
 *
 * <pre class="code-xml">
 *  &lt;doc id=&quot;1&quot;&gt;&lt;title&gt;doc 1&lt;/title&gt;&lt;/doc&gt;
 * </pre>
 */
@ConfigurationOptionInfo(
    description = "Split an XML document based on XPath rules.",
    componentGroups = ConfigurationOptionInfo.COMPONENT_GROUP_XML,
    groups =
        @ConfigurationOptionInfo.Group(
            path = {ConfigurationOptionInfo.PLATFORM_COMPONENT},
            propertyNames = {
              "rules",
              "copyParentFields",
              "validate",
              "throwErrorOnMissingXML",
              "autonumberChildDocIds",
              "dropParentDocument",
              "namespaces",
              "deleteFlag",
              "deleteFile",
              "closeLogFileAfterWrite"
            }))
public class SplitXmlSetLineage
    implements MultiOutputDocumentTransformer,
        HasInputListProperty,
        ProvidesProcessingFeedback,
        Startable,
        Stoppable,
        AieLoggerAware {

  private Map<String, String> rules = new LinkedHashMap<String, String>();
  private Map<String, String> namespaces = new HashMap<String, String>();

  private boolean copyParentFields = true;

  private boolean autonumberChildDocIds = false;

  private boolean dropParentDocument = true;

  private boolean throwErrorOnMissingXML = true;
  private List<String> input = ObjectUtils.newList(FieldNames.XML_DOM);

  private DocumentBuilder docBuilder;

  protected ProcessingFeedbackHandler feedbackHandler = null;

  private final Map<XPath, XPath> compiledRules = new HashMap<>();
  
  private Map<String, String> deleteFlag = new LinkedHashMap<String, String>();
  
  private final Map<XPath, XPath> compiledDeleteConf = new HashMap<>();
  
  private final boolean isWindows = IOUtils.getOSFamily().equals(IOUtils.WINDOWS);
  
  private boolean isOpen = false;
  
  private boolean closeLogFileAfterWrite = false;
  
  private String deleteFile = null;
  
  private BufferedWriter fileWriter = null;
  
  private String orQuery;
  
  private AieLogger log = null;

  @Override
  public void startComponent() throws AttivioException {
    SimpleNamespaceContext ns = new SimpleNamespaceContext(getNamespaces());
    for (Map.Entry<String, String> entry : rules.entrySet()) {
      try {
        XPath expDoc = new DOMXPath(entry.getKey());
        expDoc.setNamespaceContext(ns);
        XPath expId = new DOMXPath(entry.getValue());
        expId.setNamespaceContext(ns);
        compiledRules.put(expDoc, expId);
      } catch (JaxenException je) {
        throw new AttivioException(
            IndexWorkflowError.XML_HANDLING_ERROR, je, "Invalid xpath: %s", entry.getKey());
      }
    }
    for (Map.Entry<String, String> entry : deleteFlag.entrySet()) {
        try {
          XPath expDoc = new DOMXPath(entry.getKey());
          expDoc.setNamespaceContext(ns);
          XPath expDeleteFlag = new DOMXPath(entry.getValue());
          expDeleteFlag.setNamespaceContext(ns);
          compiledDeleteConf.put(expDoc, expDeleteFlag);
        } catch (JaxenException je) {
          throw new AttivioException(
              IndexWorkflowError.XML_HANDLING_ERROR, je, "Invalid xpath: %s", entry.getKey());
        }
      }

    try {
      docBuilder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
    } catch (ParserConfigurationException e) {
      throw new AttivioException(
          PlatformError.UNHANDLED_EXCEPTION, e, "Invalid JRE XML configuration");
    }
  }
  /**
   * *****************************************************************************************************************
   */
  /**
   * Flushes and closes the writer.
   *
   * @throws AttivioException
   */
  @Override
  public void stopComponent() throws AttivioException {
    if (isOpen) {
      try {
        if (closeLogFileAfterWrite && isWindows) {
          openWriter();
        }
        closeWriter();
        isOpen = false;
      } catch (IOException ex) {
        throw new AttivioException(
            PlatformError.LIFECYCLE_ERROR, ex, "Exception while attempting to flush FileWriter.");
      }
    }
  }

  /**
   * *****************************************************************************************************************
   */
  /** {@inheritDoc} */
  @Override
  public void process(IngestDocument doc, DocumentOutputClient out) throws AttivioException {
    if (throwErrorOnMissingXML) {
      boolean found = false;
      for (String fieldName : input) {
        if (doc.getField(fieldName) != null) {
          found = true;
          break;
        }
      }

      if (!found) {
        if (feedbackHandler != null) {
          feedbackHandler.warn(
              doc, null, "No XML found in fields %s of document %s", input, doc.getId());
        }
        // don't send this document to the ingest client.  Just return.
        return;
      }
    }

    for (String fieldName : input) {
      final IngestField f = doc.getField(fieldName);
      if (f != null) {
        for (IngestFieldValue value : f) {
          processXML(value.xmlValue().getDocumentElement(), doc, out);
        }
      }
    }
  }

  /**
   * ***************************************************************************************************************
   */

  /**
   * Splits the incoming document's XML and add all of the parent document field values to the child
   * documents. Also sets the
   *
   * @link PROP_PARENT_DOC_ID for the document.
   */
  @SuppressWarnings("unchecked")
  private void processXML(Element elem, IngestDocument doc, DocumentOutputClient out)
      throws AttivioException {

    if (!this.dropParentDocument) {
      out.feed(doc);
    }
    
    try {
    	for (Entry<XPath, XPath> entry : compiledDeleteConf.entrySet()) {
    		XPath xpathToDoc = entry.getKey();
    		XPath xpathToDelete = entry.getValue();
    		List<Element> nodes = xpathToDoc.selectNodes(elem);
    		for (Element node : nodes) {
    			Node deleteNode = (Node) xpathToDelete.selectSingleNode(node);
    			if (deleteNode != null) {
    				if(isFolderToDelete(node.getAttribute("id"))) {
    					writeFolderToDelete(node.getAttribute("id"));
    				}
    			}
    		}
    	}
    } catch (JaxenException je) {
    	throw new AttivioException(
    			IndexWorkflowError.XML_HANDLING_ERROR, je, "Error evaluating xpath expression for delete flag");
    }
    
    try {
      for (Entry<XPath, XPath> entry : compiledRules.entrySet()) {
        XPath xpathToDoc = entry.getKey();
        XPath xpathToId = entry.getValue();
        List<Element> nodes = xpathToDoc.selectNodes(elem);
        int childId = 0;
        for (Element node : nodes) {
          Node idNode = (Node) xpathToId.selectSingleNode(node);
          if (idNode == null) {
            throw new AttivioException(
                IndexWorkflowError.XML_HANDLING_ERROR,
                "xpathToId %s not found for document %s",
                xpathToId,
                doc.getId());
          }
          String id = idNode.getTextContent();
          if (autonumberChildDocIds) {
            id = id + "-" + childId++;
          }
          IngestDocument newDoc = new IngestDocument(id);
          if (copyParentFields) {
            // Copy all parent fields (except content pointers and input fields) - see PLAT-30347
            for (IngestField srcField : doc) {
              if (!isInputField(srcField) && !isContentPointerField(srcField)) {
                // it isn't an input field or a content pointer field
                newDoc.setField(srcField.clone());
              }
            }
          }

          newDoc.setId(id);
          //docBuilder.reset();
          Document d = docBuilder.newDocument();
          // the following line of code is silly but required
          d.appendChild(d.importNode(node.cloneNode(true), true));
          newDoc.removeField(FieldNames.XML_DOM); // overwrite this
          newDoc.removeField(FieldNames.PARENT_ID); // overwrite this
          newDoc.addValue(FieldNames.XML_DOM, d);
          newDoc.addValue(FieldNames.PARENT_ID, doc.getId());
          Path filePath = Paths.get(id);
          List<String> sl = generateLineageIDS(filePath);
          for(String val : sl) {
        	  newDoc.addValue(FieldNames.LINEAGE_IDS, val);  
          }
          out.feed(newDoc);
        }
      }
      deleteFoldersAndChildAssets(deleteFile);
    } catch (JaxenException je) {
      throw new AttivioException(
          IndexWorkflowError.XML_HANDLING_ERROR, je, "Error evaluating xpath expression");
    }
    
    
  }
  
  /**
   * *****************************************************************************************************************
   */
  private void openWriter() throws IOException {
	  // TODO: check if file exists
    FileOutputStream fos = new FileOutputStream(new File(deleteFile), true);
    fileWriter = new BufferedWriter(new OutputStreamWriter(fos, IOUtils.DEFAULT_ENCODING));
  }
  /**
   * *****************************************************************************************************************
   */
  private void closeWriter() throws IOException {
    if (fileWriter == null) {
      return;
    }

    fileWriter.flush();
    fileWriter.close();
    fileWriter = null;
  }  

  /**
   * *****************************************************************************************************************
   */
  private void writeFolderToDelete(String folderName) throws AttivioException {
	  try {
		  if (closeLogFileAfterWrite && isWindows) {
			  openWriter();
		  }
		  fileWriter.write(folderName);
		  fileWriter.write("\n"); // using \n instead of newline because that is what serializer does.
		  fileWriter.flush();
		  if (closeLogFileAfterWrite && isWindows) {
			  closeWriter();
		  }
	  } catch (IOException ioe) {
		  throw new AttivioException(IndexWorkflowError.FAILED_WRITE, ioe, "Error writing doc to file");
	  }
  }
  /**
   * *****************************************************************************************************************
   */
  private List<String> generateLineageIDS(Path filePath) {
	// Creating an iterator 
      Iterator<Path> elements = filePath.iterator(); 
      List<String> lineage = new ArrayList<>();
      StringBuilder sb = new StringBuilder();
      while (elements.hasNext()) { 
          String tmp = elements.next().toString();
          sb.append("/").append(tmp);
          lineage.add(sb.toString());
      } 
      
	  return lineage;
	  
  }
  /**
   * *****************************************************************************************************************
   */
  private boolean isContentPointerField(IngestField field) {
	  if (field != null) {
		  for (IngestFieldValue value : field) {
			  if (value.getValue() instanceof ContentPointer) {
				  return true;
			  }
		  }
	  }
	  return false;
  }
  /**
   * *****************************************************************************************************************
   */  
  private boolean isFolderToDelete(String s) {
	  String ext1 = FilenameUtils.getExtension(s);
	  if(ext1.isEmpty()) {
		  return true;
	  }
	return false;
  }
  /**
   * *****************************************************************************************************************
   */
  private boolean isInputField(IngestField field) {
    if (field == null) {
      return false;
    }

    for (String inputName : input) {
      if (inputName.equals(field.getName())) {
        return true;
      }
    }

    return false;
  }
  /**
   * ***************************************************************************************************************
   */  
  private void deleteFoldersAndChildAssets(String path) {
	DatafeedWithAssetDelete df = new DatafeedWithAssetDelete();
    try {
		df.deleteDocsUsingLineageIds(path);
	} catch (AttivioException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
  }

  /**
   * ***************************************************************************************************************
   */
  /**
   * The namespace map is a map of namespace prefix to uri mappings that can be used for xpath
   * expressions.
   *
   * <p>For example a map containing the prefix 'm' mapped to 'http://www.acme.com/myNamespace'
   * would then allow for xpath expressions of the form <code>/m:my-element/m:my-sub-element</code>.
   *
   * <p>See <a href="http://www.w3schools.com/xml/xml_namespaces.asp">
   * http://www.w3schools.com/xml/xml_namespaces.asp</a> for more information on namespace prefixes.
   */
  @ConfigurationOption(
      displayName = "Namespaces",
      description = "Prefix to URI namespace map",
      formEntryClass = ConfigurationOption.STRING_TO_STRING_MAP)
  public Map<String, String> getNamespaces() {
    return namespaces;
  }

  /**
   * ***************************************************************************************************************
   */
  public void setNamespaces(Map<String, String> namespaces) {
    this.namespaces = namespaces;
  }

  /**
   * ***************************************************************************************************************
   */
  @ConfigurationOption(
      displayName = "Splitting Rules",
      labels = {"XPath to Document", "XPath to ID"},
      description = "Rules for splitting incoming XML document into sub documents.",
      formEntryClass = ConfigurationOption.STRING_TO_STRING_MAP,
      required = true)
  public Map<String, String> getRules() {
    return rules;
  }

  /**
   * ***************************************************************************************************************
   */
  public void setRules(Map<String, String> r) {
    this.rules = r;
  }

  /**
   * ***************************************************************************************************************
   */
  @ConfigurationOption(
      displayName = "Copy Parent Fields",
      description =
          "Copies the parent documents fields to all new child documents (except for ContentPointer fields and input fields).")
  public boolean isCopyParentFields() {
    return copyParentFields;
  }

  /**
   * ***************************************************************************************************************
   */
  public void setCopyParentFields(boolean copyParentFields) {
    this.copyParentFields = copyParentFields;
  }

  /**
   * ***************************************************************************************************************
   */
  @ConfigurationOption(
      displayName = "Auto Number Child Document Ids",
      description =
          "Sets the document id to the value of the xpathToId + an incrementing number for each parent record.")
  public boolean isAutonumberChildDocIds() {
    return autonumberChildDocIds;
  }

  /**
   * ***************************************************************************************************************
   */
  public void setAutonumberChildDocIds(boolean autonumberChildDocIds) {
    this.autonumberChildDocIds = autonumberChildDocIds;
  }

  /**
   * ***************************************************************************************************************
   */
  @ConfigurationOption(
      displayName = "Drop parent Document",
      description = "Drops the source/parent document after all sub documents have been extracted.")
  public boolean isDropParentDocument() {
    return dropParentDocument;
  }

  /**
   * ***************************************************************************************************************
   */
  public void setDropParentDocument(boolean dropParentDocument) {
    this.dropParentDocument = dropParentDocument;
  }

  /**
   * ***************************************************************************************************************
   */
  @ConfigurationOption(
      displayName = "Throw Error on Missing XML",
      description = "Throws an exception if document is missing XML content")
  public boolean isThrowErrorOnMissingXML() {
    return throwErrorOnMissingXML;
  }

  /**
   * ****************************************************************************************************************
   */
  public void setThrowErrorOnMissingXML(boolean throwErrorOnMissingXML) {
    this.throwErrorOnMissingXML = throwErrorOnMissingXML;
  }

  /**
   * ****************************************************************************************************************
   */
  @Override
  @ConfigurationOption(
      displayName = "Input Fields",
      description = "List of fields containing XML",
      formEntryClass = ConfigurationOption.STRING_LIST)
  public List<String> getInput() {
    return input;
  }

  /**
   * ****************************************************************************************************************
   */
  @Override
  public void setInput(List<String> input) {
    this.input = input;
  }

  /**
   * ****************************************************************************************************************
   */
  @Override
  public void setProcessingFeedbackHandler(ProcessingFeedbackHandler feedbackHandler) {

    this.feedbackHandler = feedbackHandler;
  }
  
  /**
   * ****************************************************************************************************************
   */
  @ConfigurationOption(
	      displayName = "Delete Flag rule",
	      labels = {"XPath to Delete Flag", "XPath to Delete Attribute"},
	      description = "Rules for identifying the documents to be deleted.",
	      formEntryClass = ConfigurationOption.STRING_TO_STRING_MAP,
	      required = true)
  public Map<String, String> getDeleteFlag() {
	  return deleteFlag;
  }

  /**
   * ****************************************************************************************************************
   */
  public void setDeleteFlag(Map<String, String> deleteFlag) {
	  this.deleteFlag = deleteFlag;
  }
  @ConfigurationOption(
	      displayName = "File with delete paths",
	      labels = {"Filename"},
	      description = "File that stores the field paths to delete")
  /**
   * ****************************************************************************************************************
   */
  public String getDeleteFile() {
	  return deleteFile;
  }
  /**
   * ****************************************************************************************************************
   */
  public void setDeleteFile(String deleteFile) {
	  this.deleteFile = deleteFile;
  }
/**
   * ****************************************************************************************************************
   */
  @Override
  public void setAieLogger(AieLogger log) {
	  this.log = log;
  }
  /**
   * *****************************************************************************************************************
   */
  /**
   * On Windows, close the log file after each document write to allow modification of the log file
   * by another program
   */
  @ConfigurationOption(
		  displayName = "Close log after write",
		  description = "On Windows, close the log file after each document write.")
  public boolean isCloseLogFileAfterWrite() {
	  return closeLogFileAfterWrite;
  }
  /**
   * *****************************************************************************************************************
   */
  /**
   * On Windows, close the log file after each document write to allow modification of the log file
   * by another program
   */
  public void setCloseLogFileAfterWrite(boolean closeLogFileAfterWrite) {
	  this.closeLogFileAfterWrite = closeLogFileAfterWrite;
  }

}