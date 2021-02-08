/** Copyright 2020 Lucidworks Inc., All rights reserved. */
package com.attivio.emodules.deletedatafeedfolders.transformer.ingest.document;

import com.attivio.emodules.deletedatafeedfolders.transformer.ingest.document.SplitXmlSetLineage;
import com.attivio.sdk.AttivioException;
import com.attivio.sdk.ingest.IngestDocument;
import com.attivio.sdk.schema.FieldNames;
import com.attivio.sdk.test.MockIngestClient;
import com.attivio.sdk.test.SdkTestUtils;
import com.attivio.util.XMLUtils;
import java.util.HashMap;

import org.junit.Test;

public class SplitXmlSetLineageTest {
	
	  @Test
	  public void testDeletefolder() throws Exception {
	    IngestDocument ad = new IngestDocument("1");
	    ad.setField(FieldNames.XML_DOM, XMLUtils.parseAsW3c("<feed>"
	    		+ "<item id='/fragments/videos/strategies/high-yield-market-fed-and-market-characteristics.pdf' delete='true'>1</item>"
	    		+ "<item id='/fragments/videos/strategies/tax-efficient-investing1.doc'>2</item>"
	    		+ "<item id='/fragments/videos/strategies/tax-efficient-investing1.doc'>2</item>"
	    		+ "<item id='/assets/documents/foldertest1' delete='true'>3</item>"
	    		+ "<item id='/assets/documents/foldertest2' delete='true'>3</item>"
	    		+ "</feed>"));
	    SplitXmlSetLineage s = new SplitXmlSetLineage();
	    s.setDeleteFile("C:\\temp\\deletePaths.txt");
	    s.startComponent();
	    HashMap<String, String> rules = new HashMap<String, String>();
	    rules.put("/feed/item", "@id");
	    s.setRules(rules);
	    HashMap<String, String> deleteFlag = new HashMap<String, String>();
	    deleteFlag.put("/feed/item", "@delete");
	    s.setDeleteFlag(deleteFlag);
	    s.setCloseLogFileAfterWrite(true);
	    SdkTestUtils.startTransformer(s);
	    MockIngestClient mock = new MockIngestClient();
	    s.process(ad, mock);
	  }
	  
	  @Test
	  public void simpleTest() throws Exception {
	    IngestDocument ad = new IngestDocument("1");
	    ad.setField(FieldNames.XML_DOM, XMLUtils.parseAsW3c("<docs><doc id='/test/folder'>1</doc></docs>"));
	    SplitXmlSetLineage s = new SplitXmlSetLineage();
	    s.startComponent();
	    HashMap<String, String> rules = new HashMap<String, String>();
	    rules.put("/docs/doc", "@id");
	    s.setRules(rules);
	    SdkTestUtils.startTransformer(s);
	    MockIngestClient mock = new MockIngestClient();
	    s.process(ad, mock);
	  }
	  
	  @Test(expected = AttivioException.class)
	  public void testPLAT21918() throws Exception {
	    IngestDocument ad = new IngestDocument("1");
	    ad.setField(FieldNames.XML_DOM, XMLUtils.parseAsW3c("<docs><doc>1</doc></docs>"));
	    SplitXmlSetLineage s = new SplitXmlSetLineage();
	    HashMap<String, String> rules = new HashMap<String, String>();
	    rules.put("/docs/doc", "@id");
	    s.setRules(rules);
	    SdkTestUtils.startTransformer(s);
	    MockIngestClient mock = new MockIngestClient();
	    s.process(ad, mock);
	  }
}
