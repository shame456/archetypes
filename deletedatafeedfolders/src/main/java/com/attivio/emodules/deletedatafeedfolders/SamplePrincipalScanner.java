/** Copyright 2019 Attivio Inc., All rights reserved. */
package com.attivio.emodules.deletedatafeedfolders;

import com.attivio.sdk.AttivioException;
import com.attivio.sdk.connector.DocumentPublisher;
import com.attivio.sdk.error.ConnectorError;
import com.attivio.sdk.scanner.DataSourceScanner;
import com.attivio.sdk.security.AttivioGroupMembership;
import com.attivio.sdk.security.AttivioPrincipal;
import com.attivio.sdk.security.AttivioPrincipal.PrincipalType;
import com.attivio.sdk.security.AttivioPrincipalKey;
import com.attivio.sdk.server.annotation.ConfigurationOption;
import com.attivio.sdk.server.annotation.ConfigurationOptionInfo;
import com.attivio.sdk.server.annotation.ScannerInfo;
import com.attivio.sdk.server.annotation.ScannerInfo.ScannerPurpose;
import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import org.apache.commons.io.FileUtils;

/** Demonstrates the creation and loading of Attivio users and groups. */
@ScannerInfo(purpose = ScannerPurpose.PrincipalLoading, suggestedWorkflow = "ingestPrincipals")
@ConfigurationOptionInfo(
    displayName = "Sample Principal Loading",
    description =
        "Demonstrates the reading of principal info from files and loading into the Attivio index",
    groups = {
      @ConfigurationOptionInfo.Group(
          path = ConfigurationOptionInfo.SCANNER,
          propertyNames = {"principalsFile", "membershipsFile"})
    })
public class SamplePrincipalScanner implements DataSourceScanner {
  private File principalsFile;

  private File membershipsFile;

  private Map<String, AttivioPrincipal> principals = new HashMap<String, AttivioPrincipal>();

  private Map<String, List<String>> memberships = new HashMap<String, List<String>>();

  @Override
  public void start(String connectorName, DocumentPublisher publisher) throws AttivioException {
    try {
      getMemberships();
      feed(publisher);
    } catch (Exception e) {
      if (e instanceof AttivioException) throw (AttivioException) e;
      else
        throw new AttivioException(
            ConnectorError.CRAWL_FAILED, e, "Connector %s run failed", connectorName);
    }
  }

  private void getMemberships() throws Exception {

    for (String line : FileUtils.readLines(membershipsFile)) {
      StringTokenizer tokenizer = new StringTokenizer(line, ",", false);
      String memberId = tokenizer.nextToken();
      String groupId = tokenizer.nextToken();
      List<String> members = memberships.get(groupId);
      if (members == null) {
        members = new ArrayList<String>();
        memberships.put(groupId, members);
      }
      members.add(memberId);
    }
  }

  private void feed(DocumentPublisher publisher) throws Exception {
    List<String> principalLines = FileUtils.readLines(principalsFile);
    for (String line : principalLines) {
      AttivioPrincipal principal = createPrincipal(line);
      publisher.feed(principal);
    }
  }

  private AttivioPrincipal createPrincipal(String line) {
    StringTokenizer tokenizer = new StringTokenizer(line, ",", false);
    String domain = tokenizer.nextToken();
    String pId = tokenizer.nextToken();
    String pName = tokenizer.nextToken();
    PrincipalType pType =
        tokenizer.nextToken().equals("user") ? PrincipalType.USER : PrincipalType.GROUP;

    AttivioPrincipal principal = new AttivioPrincipal(domain, pId, pName, pType);
    if (pType.equals(PrincipalType.GROUP) && memberships.containsKey(pId)) {

      for (String memberId : memberships.get(pId)) {
        AttivioPrincipal member = principals.get(memberId);
        addMemebership(principal, member.getRealmId(), memberId);
      }
    }
    principals.put(principal.getPrincipalId(), principal);
    return principal;
  }

  private void addMemebership(AttivioPrincipal group, String memberRealm, String memberId) {
    AttivioPrincipalKey groupKey =
        new AttivioPrincipalKey(group.getRealmId(), group.getPrincipalId());
    AttivioPrincipalKey memberKey = new AttivioPrincipalKey(memberRealm, memberId);

    // We assume here the member's realm is the same as the group realm - it's not always the case
    group.addGroupMembership(new AttivioGroupMembership(memberKey, groupKey));
  }

  @ConfigurationOption(
      longOpt = "principals-file",
      displayName = "Principals File",
      description = "The path to a CSV file the contains users and groups")
  public String getPrincipalsFile() {
    return principalsFile.getAbsolutePath();
  }

  public void setPrincipalsFile(String principalsFile) {
    this.principalsFile = new File(principalsFile);
  }

  @ConfigurationOption(
      longOpt = "memberships-file",
      displayName = "Memberships File",
      description = "The path to a CSV file the contains membership associations")
  public String getMembershipsFile() {
    return membershipsFile.getAbsolutePath();
  }

  public void setMembershipsFile(String membershipsFile) {
    this.membershipsFile = new File(membershipsFile);
  }

  @Override
  public void validateConfiguration() throws AttivioException {
    if (principalsFile != null && !principalsFile.exists())
      throw new AttivioException(
          ConnectorError.CONFIGURATION_ERROR, "%s doesn't exist", principalsFile.getAbsolutePath());

    if (membershipsFile != null && !membershipsFile.exists())
      throw new AttivioException(
          ConnectorError.CONFIGURATION_ERROR,
          "%s doesn't exist",
          membershipsFile.getAbsolutePath());
  }
}
