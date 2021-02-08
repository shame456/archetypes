/** Copyright 2019 Attivio Inc., All rights reserved. */
package com.attivio.emodules.deletedatafeedfolders;

import com.attivio.sdk.AttivioException;
import com.attivio.sdk.connector.DocumentPublisher;
import com.attivio.sdk.error.ConnectorError;
import com.attivio.sdk.ingest.IngestDocument;
import com.attivio.sdk.scanner.DataSourceScanner;
import com.attivio.sdk.scanner.http.BasicAuthentication;
import com.attivio.sdk.scanner.http.DigestAuthentication;
import com.attivio.sdk.scanner.http.HttpClientProvider;
import com.attivio.sdk.scanner.http.HttpConnectionConfig;
import com.attivio.sdk.scanner.http.HttpDataSourceScanner;
import com.attivio.sdk.schema.FieldNames;
import com.attivio.sdk.server.annotation.ConfigurationOptionInfo;
import com.attivio.sdk.server.annotation.ScannerInfo;
import java.io.IOException;
import org.apache.commons.io.IOUtils;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;

/**
 * This sample scanner demonstrates how to use the {@link HttpDataSourceScanner} interface to take
 * advantage of the Attivio provided implementations of Http client authentication protocols. in
 * this example, the scanner implements the {@link BasicAuthentication} interface. Once that
 * interface is implemented, Attivio will create and configure for the scanner a {@link
 * HttpClientProvider} object. If the scanner implements more than a single authentication
 * interface, say {@link BasicAuthentication} and {@link DigestAuthentication}, Attivio will
 * configure the one that was configured. That allows the scanner to support multiple Http
 * protocols.
 */
@ScannerInfo(
    suggestedWorkflow =
        "ingest") // Use fileIngest for large documents and documents with complex formats such as
// PDF files
@ConfigurationOptionInfo(
    displayName = "Http Data Source Scanner",
    description = "An example for using the Http Data Source interfaces",
    groups = {
      @ConfigurationOptionInfo.Group(
          path = {ConfigurationOptionInfo.SCANNER, "BasicAuthentication"},
          propertyNames = {
            "basicAuthLogin",
            "basicAuthPassword",
            "basicAuthRealm",
            "basicAuthPort",
            "basicAuthHost"
          }),
      @ConfigurationOptionInfo.Group(
          path = {ConfigurationOptionInfo.SCANNER, "HttpConnConfig"},
          propertyNames = {"maxConnTotal", "maxConnPerRoute"}),
      @ConfigurationOptionInfo.Group(
          path = ConfigurationOptionInfo.SCANNER,
          propertyNames = {"contentUrl"})
    })
public class SampleHttpDataSourceScanner
    implements DataSourceScanner, HttpDataSourceScanner, BasicAuthentication, HttpConnectionConfig {
  private HttpClientProvider httpProvider;

  private String contentUrl;

  private String basicAuthLogin = "";

  private String basicAuthPassword = "";

  private String basicAuthRealm = "";

  private int basicAuthPort = -1;

  private String basicAuthHost = "";

  private int maxConnTotal = 10;

  private int maxConnPerRoute = 2;

  @Override
  public void start(String connectorName, DocumentPublisher publisher) throws AttivioException {
    try {

      HttpGet get = new HttpGet(contentUrl);
      CloseableHttpClient httpClient = httpProvider.prepareHttpClient(contentUrl);
      CloseableHttpResponse response = httpClient.execute(get);
      int status = response.getStatusLine().getStatusCode();
      if (response.getStatusLine().getStatusCode() != 200)
        throw new AttivioException(
            ConnectorError.CRAWL_FAILED, "Didn't get OK status from %s - %s", contentUrl, status);

      String content = new String(IOUtils.toByteArray(response.getEntity().getContent()));
      IngestDocument doc = new IngestDocument("1");
      doc.addValue(FieldNames.TEXT, content);
      publisher.feed(doc);

    } catch (IOException e) {
      throw new AttivioException(
          ConnectorError.CRAWL_FAILED, e, "Failed to retrieve content from %s", contentUrl);
    } finally {
      if (httpProvider != null) IOUtils.closeQuietly(httpProvider);
    }
  }

  public String getContentUrl() {
    return contentUrl;
  }

  public void setContentUrl(String contentUrl) {
    this.contentUrl = contentUrl;
  }

  public HttpClientProvider getHttpProvider() {
    return httpProvider;
  }

  @Override
  public void setHttpProvider(HttpClientProvider provider) {
    this.httpProvider = provider;
  }

  @Override
  public String getBasicAuthLogin() {
    return basicAuthLogin;
  }

  @Override
  public void setBasicAuthLogin(String basicAuthLogin) {
    this.basicAuthLogin = basicAuthLogin;
  }

  @Override
  public String getBasicAuthPassword() {
    return basicAuthPassword;
  }

  @Override
  public void setBasicAuthPassword(String basicAuthPassword) {
    this.basicAuthPassword = basicAuthPassword;
  }

  @Override
  public String getBasicAuthRealm() {
    return basicAuthRealm;
  }

  @Override
  public void setBasicAuthRealm(String basicAuthRealm) {
    this.basicAuthRealm = basicAuthRealm;
  }

  @Override
  public int getBasicAuthPort() {
    return basicAuthPort;
  }

  @Override
  public void setBasicAuthPort(int basicAuthPort) {
    this.basicAuthPort = basicAuthPort;
  }

  @Override
  public String getBasicAuthHost() {
    return basicAuthHost;
  }

  @Override
  public void setBasicAuthHost(String basicAuthHost) {
    this.basicAuthHost = basicAuthHost;
  }

  @Override
  public int getMaxConnTotal() {
    return maxConnTotal;
  }

  @Override
  public void setMaxConnTotal(int maxConnTotal) {
    this.maxConnTotal = maxConnTotal;
  }

  @Override
  public int getMaxConnPerRoute() {
    return maxConnPerRoute;
  }

  @Override
  public void setMaxConnPerRoute(int maxConnPerRoute) {
    this.maxConnPerRoute = maxConnPerRoute;
  }
}
