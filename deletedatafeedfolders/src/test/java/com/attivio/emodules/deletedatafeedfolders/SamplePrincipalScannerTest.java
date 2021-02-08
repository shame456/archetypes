/** Copyright 2019 Attivio Inc., All rights reserved. */
package com.attivio.emodules.deletedatafeedfolders;

import com.attivio.sdk.ingest.DocumentList;
import com.attivio.sdk.ingest.IngestDocument;
import com.attivio.sdk.scanner.TestScannerRunner;
import com.attivio.sdk.security.AttivioGroupMembership;
import com.attivio.sdk.security.AttivioPrincipal;
import com.attivio.sdk.test.SdkTestUtils;
import com.attivio.util.FileUtils;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.junit.Assert;
import org.junit.Test;

/** Run a simple test scanner using SdkTestUtils */
public class SamplePrincipalScannerTest {

  private static final String[][] PRINCIPALS = {
    {"acme-domain", "id01", "user1", "user"},
    {"acme-domain", "id046", "user2", " user"},
    {"acme-domain", "id077", "user3", " user"},
    {"acme-domain", "id0999", "group1", " group"},
    {"acme-domain", "id0888", "group2", " group"}
  };

  private static final String[][] MEMBERSHPS = {
    {"id01", "id0999"},
    {"id046", "id0999"},
    {"id077", "id0888"}
  };

  @Test
  public void test() throws Exception {
    File principalsFile = null;
    File membershipsFile = null;
    try {
      principalsFile = writeLines(dataToLines(PRINCIPALS));
      membershipsFile = writeLines(dataToLines(MEMBERSHPS));

      SamplePrincipalScanner sampleScanner = new SamplePrincipalScanner();
      sampleScanner.setPrincipalsFile(principalsFile.getAbsolutePath());
      sampleScanner.setMembershipsFile(membershipsFile.getAbsolutePath());

      TestScannerRunner scannerRunner =
          SdkTestUtils.createTestScannerRunner("sampleScannerConnector", sampleScanner);
      scannerRunner.start();
      DocumentList documentList = (DocumentList) scannerRunner.getSentMessages().get(0);
      Assert.assertEquals(5, documentList.size());

      for (IngestDocument doc : documentList) {
        AttivioPrincipal principal = doc.getPrincipal();
        Assert.assertNotNull(principal);
        Set<AttivioGroupMembership> memberships = principal.getGroupMemberships();
        Assert.assertNotNull(memberships);
        if (principal.getPrincipalId().equals("id0999")) Assert.assertEquals(2, memberships.size());
        else if (principal.getPrincipalId().equals("id0888"))
          Assert.assertEquals(1, memberships.size());
        else Assert.assertEquals(0, memberships.size());
      }

    } finally {
      principalsFile.delete();
      membershipsFile.delete();
    }
  }

  private List<String> dataToLines(String[][] rowData) {
    List<String> lines = new ArrayList<String>();
    for (String[] row : rowData) {
      StringBuilder sb = new StringBuilder();
      for (String item : row) {
        if (sb.length() > 0) sb.append(',');
        sb.append(item);
      }
      lines.add(sb.toString());
    }

    return lines;
  }

  private File writeLines(List<String> lines) throws IOException {
    File f = File.createTempFile("testdata", null);
    FileUtils.writeLines(f, lines);
    return f;
  }

  // FileUtils.writeLines(f, Arrays.asList("some text" + i, "more text" + i));
}
