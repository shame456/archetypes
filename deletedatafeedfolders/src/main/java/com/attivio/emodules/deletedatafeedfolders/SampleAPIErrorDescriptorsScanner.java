/** Copyright 2019 Attivio Inc., All rights reserved. */
package com.attivio.emodules.deletedatafeedfolders;

import com.attivio.sdk.AttivioException;
import com.attivio.sdk.connector.DocumentPublisher;
import com.attivio.sdk.error.ConnectorError;
import com.attivio.sdk.scanner.DataSourceAPIErrorDescriptor;
import com.attivio.sdk.scanner.DataSourceAPIErrorDescriptors;
import com.attivio.sdk.scanner.DataSourceAPIErrorDescriptorsAware;
import com.attivio.sdk.scanner.DataSourceScanner;
import com.attivio.sdk.scanner.http.BasicAuthentication;
import com.attivio.sdk.scanner.http.HttpClientProvider;
import com.attivio.sdk.scanner.http.HttpDataSourceScanner;
import com.attivio.sdk.server.annotation.ConfigurationOptionInfo;
import com.attivio.sdk.server.annotation.ScannerInfo;
import java.io.IOException;
import org.apache.commons.io.IOUtils;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;

/**
 * This sample scanner shows how to define data source errors in a json file. The file is then
 * placed on the classpath of the Attivio node. The Attivio framework creates a {@link
 * DataSourceAPIErrorDescriptors} object the scanner uses to obtain information about handling
 * errors encountered when the underlying data source is accessed. The advantages of using this
 * mechanism are:
 *
 * <p>1) A unified way to describe common error handling issues: Retry, logging, system events etc.
 *
 * <p>2) A single human readable description of error handling.
 *
 * <p>3) Supportability: Error handling parameters can be modified without a need for a patch
 *
 * <p>To use this mechanism the scanner must:
 *
 * <p>1) Implement the {@link DataSourceAPIErrorDescriptorsAware} interface
 *
 * <p>2) Define the errors in a json file called <scanner-class-name>_errors.json
 * SampleAPIErrorDescriptorsScanner_errors.json in this example.
 *
 * <p>3) Place the json file on the Attivio node classpath. The simplest way is to put the json file
 * in the same package as the scanner.
 *
 * <p>4) The scanner can name the json file differently and place it anywhere on the classpath if it
 * overrides the {@link DataSourceAPIErrorDescriptorsAware#getErrorDescriptorsJsonFileName()}
 */
@ScannerInfo(suggestedWorkflow = "ingest")
@ConfigurationOptionInfo(
    displayName = "Error Descriptors Scanner",
    description =
        "An example for defining data source API calls in a json file using teh DataSourceAPIErrorDescriptorsAware framework",
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
          path = ConfigurationOptionInfo.SCANNER,
          propertyNames = {"contentUrl"})
    })
public class SampleAPIErrorDescriptorsScanner
    implements DataSourceScanner,
        HttpDataSourceScanner,
        BasicAuthentication,
        DataSourceAPIErrorDescriptorsAware {
  private HttpClientProvider httpProvider;

  private String contentUrl;

  private String basicAuthLogin = "";

  private String basicAuthPassword = "";

  private String basicAuthRealm = "";

  private int basicAuthPort = -1;

  private String basicAuthHost = "";

  // errorDescriptors is generated from the SampleAPIErrorDescriptorsScanner_errors.json file
  // located in the same directory
  // as this scanner. See the use of it to get information about handling errors.
  private DataSourceAPIErrorDescriptors errorDescriptors;

  @Override
  public void setErrorDescriptors(DataSourceAPIErrorDescriptors errorDescriptors) {
    this.errorDescriptors = errorDescriptors;
  }

  @Override
  public void start(String connectorName, DocumentPublisher publisher) throws AttivioException {
    try {

      HttpGet get = new HttpGet(contentUrl);
      CloseableHttpClient httpClient = httpProvider.prepareHttpClient(contentUrl);
      httpClient.execute(get);

      // For this example httpClient.execute always throws an exception

    } catch (IOException e) {
      DataSourceAPIErrorDescriptor errorDesc =
          errorDescriptors.getErrorDescriptor(e.getClass().getName());
      // Demonstrates the use of DataSourceAPIErrorDescriptors properties
      boolean useStackTrace =
          Boolean.parseBoolean(
              errorDesc.getErrorProperties().getProperty("use_stacktrace", "false"));

      if (errorDesc.isFatal()) {

        AttivioException ae =
            useStackTrace
                ? new AttivioException(
                    ConnectorError.CRAWL_FAILED,
                    e,
                    errorDesc.getErrorMessage(),
                    contentUrl,
                    this.getClass().getName())
                : new AttivioException(
                    ConnectorError.CRAWL_FAILED,
                    errorDesc.getErrorMessage(),
                    contentUrl,
                    this.getClass().getName());
        throw ae;
      } else {
        System.err.println(
            String.format(errorDesc.getErrorMessage(), contentUrl, this.getClass().getName()));
        if (useStackTrace) e.printStackTrace();
      }

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
}
