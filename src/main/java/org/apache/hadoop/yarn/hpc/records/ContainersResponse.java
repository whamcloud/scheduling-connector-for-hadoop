// Copyright (c) 2017 DDN. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package org.apache.hadoop.yarn.hpc.records;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.SerializedException;

public class ContainersResponse {
  private Map<String, ByteBuffer> servicesMetaData;
  private List<ContainerId> succeededContainers;
  private Map<ContainerId, SerializedException> failedContainers;

  public Map<String, ByteBuffer> getServicesMetaData() {
    return servicesMetaData;
  }

  public void setServicesMetaData(Map<String, ByteBuffer> servicesMetaData) {
    this.servicesMetaData = servicesMetaData;
  }

  public List<ContainerId> getSucceededContainers() {
    return succeededContainers;
  }

  public void setSucceededContainers(List<ContainerId> succeededContainers) {
    this.succeededContainers = succeededContainers;
  }

  public Map<ContainerId, SerializedException> getFailedContainers() {
    return failedContainers;
  }

  public void setFailedContainers(
      Map<ContainerId, SerializedException> failedContainers) {
    this.failedContainers = failedContainers;
  }
}
