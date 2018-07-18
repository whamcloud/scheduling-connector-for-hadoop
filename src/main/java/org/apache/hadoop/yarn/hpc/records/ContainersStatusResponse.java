// Copyright (c) 2017 DDN. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package org.apache.hadoop.yarn.hpc.records;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.SerializedException;

public class ContainersStatusResponse {

  private List<ContainerStatus> statuses;
  private Map<ContainerId, SerializedException> failedRequests;

  public List<ContainerStatus> getStatuses() {
    return statuses;
  }

  public void setStatuses(List<ContainerStatus> statuses) {
    this.statuses = statuses;
  }

  public Map<ContainerId, SerializedException> getFailedRequests() {
    return failedRequests;
  }

  public void setFailedRequests(
      Map<ContainerId, SerializedException> failedRequests) {
    this.failedRequests = failedRequests;
  }
}
