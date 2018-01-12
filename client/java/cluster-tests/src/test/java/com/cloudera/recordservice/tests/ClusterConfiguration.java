// Copyright 2014 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.cloudera.recordservice.tests;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.Authenticator;
import java.net.MalformedURLException;
import java.net.PasswordAuthentication;
import java.net.URL;
import java.net.URLConnection;
import java.util.Random;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class interacts with a cluster running Cloudera Manager and is
 * responsible for getting the information needed to run RecordService jobs on
 * the cluster of the given host. This class, given a hostname, using the CM web
 * api, gets the cluster configurations.
 */
public class ClusterConfiguration {
  public final int cmPort_ = 7180;
  public final String apiVersionString_ = "v10";

  private static final Logger LOGGER = LoggerFactory
      .getLogger(ClusterConfiguration.class);

  private Random rand_;
  private String hostname_;
  private String urlBase_;
  private String configServiceName_;
  private String hadoopConfDirPath_;
  private String username_;
  private String password_;

  public ClusterConfiguration(String hostname, String username, String password)
      throws MalformedURLException, JSONException, IOException {
    rand_ = new Random();
    hostname_ = hostname;
    username_ = username;
    password_ = password;
    urlBase_ = "http://" + hostname + ":" + cmPort_ + "/api/" + apiVersionString_ + "/";
    if (!getClusterConfPath()) {
      throw new FileNotFoundException("Unable to access the cluster's cm web api");
    }
  }

  /**
   * This method sends a get request to CM using the given URL. The return value
   * of that request is returned in the method in string form.
   */
  public String clusterGetRequest(URL url) throws IOException {
    Authenticator.setDefault(new Authenticator() {
      protected PasswordAuthentication getPasswordAuthentication() {
        return new PasswordAuthentication(username_, password_.toCharArray());
      }
    });
    StringBuilder ret = new StringBuilder();
    BufferedReader br = null;
    try {
      URLConnection cc = url.openConnection();
      br = new BufferedReader(new InputStreamReader(cc.getInputStream()));
      String line;
      while ((line = br.readLine()) != null) {
        ret.append(line);
      }
    } finally {
      if (br != null) {
        br.close();
      }
    }
    return ret.toString();
  }

  public String getHadoopConfDir() {
    return hadoopConfDirPath_;
  }

  /**
   * This method does the setup for the ClusterConfiguration object. This method
   * gets the HADOOP_CONF_DIR from the cluster machine and creates a local copy,
   * setting the object's hadoopConfDirPath variable.
   */
  private boolean getClusterConfPath() throws MalformedURLException, JSONException,
      IOException {
    String url = urlBase_ + "clusters/";
    String clusterName = extractClusterName(clusterGetRequest(new URL(url)), 0);
    if (clusterName == null) {
      return false;
    }
    // Format name for http requests
    clusterName = clusterName.replace(" ", "%20");
    url += clusterName + "/services/";
    configServiceName_ = extractConfigServiceName(clusterGetRequest(new URL(url)));
    LOGGER.debug("configServiceName: " + configServiceName_);
    if (configServiceName_ == null) {
      return false;
    }
    url += configServiceName_ + "/clientConfig/";
    hadoopConfDirPath_ = unzipConf(getClusterConfiguration(new URL(url)));
    LOGGER.debug("Final path: " + hadoopConfDirPath_);
    return hadoopConfDirPath_ != null;
  }

  /**
   * This method gets a cluster configuration and write the configuration to a
   * zipfile. The method returns the path of the zipfile.
   */
  private String getClusterConfiguration(URL url) throws IOException {
    // This Authenticator object sets the user and password field, similar to
    // using the --user username:password flag with the linux curl command
    Authenticator.setDefault(new Authenticator() {
      protected PasswordAuthentication getPasswordAuthentication() {
        return new PasswordAuthentication(username_, password_.toCharArray());
      }
    });

    int num = rand_.nextInt(10000000);
    String confZipFileName = "/tmp/" + "clusterConf" + "_" + num + ".zip";
    InputStream is = null;
    FileOutputStream fos = null;
    try {
      URLConnection conn = url.openConnection();
      is = conn.getInputStream();
      fos = new FileOutputStream(confZipFileName);
      byte[] buffer = new byte[4096];
      int len;
      while ((len = is.read(buffer)) > 0) {
        fos.write(buffer, 0, len);
      }
    } finally {
      try {
        if (is != null) {
          is.close();
        }
      } finally {
        if (fos != null) {
          fos.close();
        }
      }
    }
    return confZipFileName;
  }

  /**
   * This method unzips a conf dir return through the CM api and returns the
   * path of the unzipped directory as a string
   */
  private String unzipConf(String zipFileName) throws IOException {
    int num = rand_.nextInt(10000000);
    String outDir = "/tmp/" + "confDir" + "_" + num + "/";
    byte[] buffer = new byte[4096];
    FileInputStream fis;
    String filename = "";
    fis = new FileInputStream(zipFileName);
    ZipInputStream zis = new ZipInputStream(fis);
    ZipEntry ze = zis.getNextEntry();
    while (ze != null) {
      filename = ze.getName();
      File unzippedFile = new File(outDir + filename);
      // Ensure that parent directories exist
      new File(unzippedFile.getParent()).mkdirs();
      FileOutputStream fos = new FileOutputStream(unzippedFile);
      int len;
      while ((len = zis.read(buffer)) > 0) {
        fos.write(buffer, 0, len);
      }
      fos.close();
      zis.closeEntry();
      ze = zis.getNextEntry();
    }
    zis.closeEntry();
    zis.close();
    fis.close();
    return outDir + "recordservice-conf/";
  }

  /**
   * CM uses a hierarchy to determine which service's conf directory serves as
   * the system's hadoop configuration directory. This method takes as its input
   * a String in JSON returned from the CM api that lists the running services
   * on the cluster and returns the name of the relevant service that sets the
   * HADOOP_CONF_DIR
   */
  private String extractConfigServiceName(String input) throws JSONException {
    JSONObject obj = new JSONObject(input);
    JSONArray items = obj.getJSONArray("items");
    String rsName = "";
    String yarnName = "";
    String hdfsName = "";
    System.out.println(input);
    for (int i = 0; i < items.length(); i++) {
      String type = items.getJSONObject(i).getString("type");
      if (type.equals("RECORD_SERVICE")
          && items.getJSONObject(i).get("serviceState").equals("STARTED")) {
        LOGGER.debug("RS EXISTS");
        rsName = items.getJSONObject(i).getString("name");
      } else if (type.equals("YARN")
          && items.getJSONObject(i).get("serviceState").equals("STARTED")) {
        LOGGER.debug("YARN EXISTS");
        yarnName = items.getJSONObject(i).getString("name");
      } else if (type.equals("HDFS")
          && items.getJSONObject(i).get("serviceState").equals("STARTED")) {
        LOGGER.debug("HDFS EXISTS");
        hdfsName = items.getJSONObject(i).getString("name");
      }
    }
    if (!rsName.equals("")) {
      return rsName;
    } else if (!yarnName.equals("")) {
      return yarnName;
    } else if (!hdfsName.equals("")) {
      return hdfsName;
    } else {
      return null;
    }
  }

  /**
   * CM can control multiple clusters. This code extracts the cluster name given
   * its position in a JSON array
   */
  private String extractClusterName(String input, int clusterListNumber)
      throws JSONException {
    JSONObject obj = new JSONObject(input);
    JSONArray items = obj.getJSONArray("items");
    return items.getJSONObject(clusterListNumber).getString("name");
  }
}
