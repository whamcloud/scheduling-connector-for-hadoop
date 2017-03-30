// Copyright (c) 2017 Intel Corporation. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package org.apache.hadoop.yarn.hpc.records;

public class ApplicationMasterRegisterRequest {
  private String host;
  private int port;
  private String trackingUrl;

  public ApplicationMasterRegisterRequest(String host, int port,
      String trackingUrl) {
    this.host = host;
    this.port = port;
    this.trackingUrl = trackingUrl;
  }

  public String getHost() {
    return host;
  }

  public int getPort() {
    return port;
  }

  public String getTrackingUrl() {
    return trackingUrl;
  }
}