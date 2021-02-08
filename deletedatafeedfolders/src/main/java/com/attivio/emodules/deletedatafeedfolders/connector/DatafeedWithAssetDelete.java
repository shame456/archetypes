/** Copyright 2021 Lucidworks Inc., All rights reserved. */
package com.attivio.emodules.deletedatafeedfolders.connector;

import com.attivio.sdk.AttivioException;
import com.attivio.sdk.client.IndexCommitter;
import com.attivio.sdk.client.IngestionHistoryApi;
import com.attivio.sdk.connector.DocumentPublisher;
import com.attivio.sdk.error.ConnectorError;
import com.attivio.sdk.ingest.IngestDocument;
import com.attivio.sdk.ingest.IngestFieldValue;
import com.attivio.sdk.scanner.DataSourceScanner;
import com.attivio.sdk.scanner.IncrementalDataSourceScanner;
import com.attivio.sdk.scanner.IndexCommitterAware;
import com.attivio.sdk.schema.FieldNames;
import com.attivio.sdk.security.AttivioAcl;
import com.attivio.sdk.security.AttivioAclEntry;
import com.attivio.sdk.security.AttivioPermission;
import com.attivio.sdk.security.AttivioPrincipal;
import com.attivio.sdk.security.AttivioPrincipal.PrincipalType;
import com.attivio.sdk.server.annotation.ConfigurationOption;
import com.attivio.sdk.server.annotation.ConfigurationOptionInfo;
import com.attivio.sdk.server.annotation.ScannerInfo;
import com.attivio.sdk.service.ServiceFactoryFactory;
import com.attivio.util.FileUtils;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;

/**
 * An example for an incremental file system scanner. The sample demonstrates:
 *
 * <p>1) The creation of a document with text content and meta data.
 *
 * <p>2) Only new or modified documents are fed.
 *
 * <p>3) Obsolete documents are deleted
 *
 * <p>4) The scanner can be configured to secure the document with an authorized principal.
 *
 * <p>5) Storing large documents in the content store.
 *
 * <p>6) The creation of {@link AttivioException} using {@link ConnectorError}
 *
 * <p>7) The use of {@link IndexCommitterAware}
 */
@ScannerInfo(suggestedWorkflow = "fileIngest")
@ConfigurationOptionInfo(
    displayName = "Incremental Sample Scanner",
    description = "An example for an incremental scanner",
    groups = {
      @ConfigurationOptionInfo.Group(
          path = ConfigurationOptionInfo.SCANNER,
          propertyNames = {"fileList"}),
      @ConfigurationOptionInfo.Group(
          path = {ConfigurationOptionInfo.SCANNER, ConfigurationOptionInfo.INCREMENTAL},
          propertyNames = {"incrementalModeActivated", "incrementalDeletes"}),
      @ConfigurationOptionInfo.Group(
          path = {ConfigurationOptionInfo.SECURITY},
          propertyNames = {"realm", "principal"})
    })
public class DatafeedWithAssetDelete
    implements DataSourceScanner, IncrementalDataSourceScanner, IndexCommitterAware {
  private static int CONTENT_STORE_THRESHOLD_KB = 64000;

  private List<String> fileList;

  private DocumentPublisher publisher;

  private IngestionHistoryApi history;

  private String connectorName;

  private boolean incrementalModeActivated;

  private boolean incrementalDelete;

  private String realm;

  private String principal;

  private IndexCommitter indexCommitter;

  @ConfigurationOption(
      longOpt = "file-list",
      displayName = "File and Directory List",
      description = "List of files and directories to scan",
      formEntryClass = ConfigurationOption.STRING_LIST)
  public List<String> getFileList() {
    return fileList;
  }

  public void setFileList(List<String> fileList) {
    this.fileList = fileList;
  }

  @Override
  public void start(String connectorName, DocumentPublisher publisher) throws AttivioException {
    this.connectorName = connectorName;
    this.publisher = publisher;
    if (incrementalModeActivated && !publisher.isInTestMode()) {
      history = ServiceFactoryFactory.get().getService(IngestionHistoryApi.class);
      history.startSession(connectorName);
    }
    filesAndDirectoriesToDocuments(fileList);

    if (incrementalDelete) deleteObsoleteDocuments();
  }

  private void deleteObsoleteDocuments() throws AttivioException {
    if (publisher.isInTestMode()) return;

    for (String docId : history.getUnvisited(connectorName)) {
      publisher.delete(docId);
    }
  }

  @Override
  public void validateConfiguration() throws AttivioException {
    if (fileList == null)
      throw new AttivioException(ConnectorError.CONFIGURATION_ERROR, "fileList was not configured");
    if (realm != null && principal == null || realm == null && principal != null)
      throw new AttivioException(
          ConnectorError.CONFIGURATION_ERROR,
          "Both the realm Id and the principal name must be set or none of them");
  }

  @Override
  public boolean isIncrementalDeletes() {
    return incrementalDelete;
  }

  @Override
  public void setIncrementalDeletes(boolean incrementalDelete) {
    this.incrementalDelete = incrementalDelete;
  }

  @Override
  public void reset(String connectorName) throws AttivioException {
    if (incrementalModeActivated)
      ServiceFactoryFactory.get().getService(IngestionHistoryApi.class).clear(connectorName);
  }

  @Override
  public boolean isIncrementalModeActivated() {
    return this.incrementalModeActivated;
  }

  @Override
  public void setIncrementalModeActivated(boolean incrementalModeActivated) {
    this.incrementalModeActivated = incrementalModeActivated;
  }

  @ConfigurationOption(
      longOpt = "security-realm",
      displayName = "Security Realm",
      description = "Realm Id of authorized principal")
  public String getRealm() {
    return realm;
  }

  public void setRealm(String realm) {
    this.realm = realm;
  }

  @ConfigurationOption(
      longOpt = "authorized-principal",
      displayName = "Authorized Principal",
      description = "Principal authorized to read the document")
  public String getPrincipal() {
    return principal;
  }

  public void setPrincipal(String principal) {
    this.principal = principal;
  }

  @ConfigurationOption(hidden = true)
  @Override
  public void setIndexCommitter(IndexCommitter indexCommitter) {
    this.indexCommitter = indexCommitter;
  }

  /**
   * @return An ACL that allows only a certain security group to read the document. In this example
   *     we assume that the principal name and the principal Id are identical. We also assume that
   *     the principal is of type {@link PrincipalType.GROUP}
   */
  private AttivioAcl createACL() {
    if (principal == null) return null;

    AttivioPrincipal p = new AttivioPrincipal(realm, principal, principal, PrincipalType.GROUP);

    AttivioAcl acl = new AttivioAcl("SampleACL");
    List<AttivioPermission> perms = new ArrayList<AttivioPermission>();
    perms.add(AttivioPermission.READ);
    acl.addEntry(new AttivioAclEntry(p, perms, false));
    return acl;
  }

  private void feedDocument(IngestDocument doc) throws AttivioException {

    if (shouldFeedThisDoc(doc)) {
      AttivioAcl acl = createACL();
      if (acl == null) publisher.feed(doc);
      else publisher.feed(doc, acl);
    }

    if (incrementalModeActivated && !publisher.isInTestMode())
      history.visit(connectorName, doc.getId(), getDocSignature(doc).getBytes());
  }
  
  public void deleteDocsUsingLineageIds(String path) throws AttivioException {
	  try {
		FileInputStream fis = new FileInputStream(path);
	} catch (FileNotFoundException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
  }

  private String getDocSignature(IngestDocument doc) {
    IngestFieldValue ifv = doc.getFirstValue(FieldNames.TEXT);
    return ifv == null ? "" : ifv.stringValue();
  }

  private boolean shouldFeedThisDoc(IngestDocument doc) throws AttivioException {
    if (!incrementalModeActivated || publisher.isInTestMode()) return true;

    byte[] sBytes = history.getSignature(connectorName, doc.getId());
    String prevSignature = sBytes != null ? new String(sBytes) : null;
    return prevSignature == null || !prevSignature.equals(getDocSignature(doc));
  }

  private void scanDirectory(File dir) throws AttivioException {
    List<String> children = new ArrayList<String>();
    for (File f : dir.listFiles()) {
      children.add(f.getAbsolutePath());
    }
    filesAndDirectoriesToDocuments(children);
  }

  private void createAttivioDocument(File file) throws AttivioException {

    try {

      String docText = "";
      for (String line : FileUtils.readLines(file)) {
        docText += line;
      }
      IngestDocument doc = new IngestDocument(file.getAbsolutePath());

      // This demonstrates the use of indexCommitter. It is should be used only when an explicit
      // commit is required because of
      // some scanner condition. More typically, the publisher will manage index commits.
      if (docText.contains("some trigger text")) indexCommitter.commit("default");

      addMetadata(file, doc, docText);
      addContent(doc, docText);

      feedDocument(doc);

    } catch (IOException e) {
      throw new AttivioException(ConnectorError.CRAWL_FAILED, e, "Crawl failed");
    }
  }

  private void addMetadata(File file, IngestDocument doc, String docText) throws AttivioException {
    doc.addValue(FieldNames.TITLE, file.getName());
    doc.addValue(FieldNames.DATE, new Date(file.lastModified()));
  }

  private void addContent(IngestDocument doc, String docText) throws AttivioException {
    if (docText.length() > CONTENT_STORE_THRESHOLD_KB) {
      // This code demonstrates the storing of the content in the content store and setting a
      // pointer in the document. Note that using this
      // technique is most efficient when the document is large and the content can be streamed out
      // rather than be stored first in memory.
      publisher.put(
          doc,
          FieldNames.CONTENT_POINTER,
          UUID.randomUUID().toString(),
          new ByteArrayInputStream(docText.getBytes()));
    } else doc.addValue(FieldNames.TEXT, docText);
  }

  private void filesAndDirectoriesToDocuments(List<String> fileList) throws AttivioException {

    for (String path : fileList) {
      File file = new File(path);
      if (file.isDirectory()) scanDirectory(file);
      else createAttivioDocument(file);
    }
  }
}
