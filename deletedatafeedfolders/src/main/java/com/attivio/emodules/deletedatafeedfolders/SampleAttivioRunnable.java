package com.attivio.emodules.deletedatafeedfolders;

import com.attivio.sdk.AttivioException;
import com.attivio.sdk.client.SearchClient;
import com.attivio.sdk.commandline.AttivioRunnable;
import com.attivio.sdk.search.QueryResponse;
import com.attivio.sdk.search.SearchDocument;
import com.attivio.sdk.server.annotation.ConfigurationOption;
import com.attivio.sdk.server.annotation.ConfigurationOption.OptionLevel;
import com.attivio.sdk.service.ServiceFactory;
import com.attivio.sdk.service.ServiceFactoryFactory;

public class SampleAttivioRunnable implements AttivioRunnable {

  private String projectName;
  private String projectEnv = "default";
  private String zkConnectString = "localhost:16980";

  private String query = "*:*";

  @Override
  public int run() throws AttivioException {

    // get the ServiceFactory - need to getRemote because this runs outside of the node
    ServiceFactory sf = ServiceFactoryFactory.getRemote(projectName, projectEnv, zkConnectString);

    // get the search client service and query the index
    SearchClient sc = sf.getService(SearchClient.class);
    QueryResponse qr = sc.search(query);

    // print out documents
    for (SearchDocument doc : qr.getDocuments()) {
      StringBuilder sb = new StringBuilder();
      sb.append("DOCUMENT: ").append(doc.getId()).append("  FIELDS: ");
      boolean start = true;
      for (String field : doc.getFieldNames()) {
        if (!start) {
          sb.append(", ");
        }
        sb.append(field).append(" -> ").append(doc.getFirstValue(field).stringValue());
        start = false;
      }
      System.out.println(sb);
    }

    return AttivioRunnable.RETURN_CODE_OK;
  }

  @ConfigurationOption(
      optionLevel = OptionLevel.Required,
      shortOpt = "n",
      description = "The name of the project against which to search")
  public String getProjectName() {
    return projectName;
  }

  public void setProjectName(String projectName) {
    this.projectName = projectName;
  }

  @ConfigurationOption(
      shortOpt = "e",
      description = "The environment of the project against which to search")
  public String getProjectEnv() {
    return projectEnv;
  }

  public void setProjectEnv(String projectEnv) {
    this.projectEnv = projectEnv;
  }

  @ConfigurationOption(
      shortOpt = "z",
      description = "The connection string <host>:<port> to use to connect to zookeeper")
  public String getZkConnectString() {
    return zkConnectString;
  }

  public void setQuery(String query) {
    this.query = query;
  }

  @ConfigurationOption(shortOpt = "q", description = "The query to search the attivio index with")
  public String getQuery() {
    return query;
  }

  public void setZkConnectString(String zkConnectString) {
    this.zkConnectString = zkConnectString;
  }
}
