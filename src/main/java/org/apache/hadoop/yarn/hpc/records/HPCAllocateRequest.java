// Copyright (c) 2017 DDN. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package org.apache.hadoop.yarn.hpc.records;

import java.util.List;

import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ResourceBlacklistRequest;
import org.apache.hadoop.yarn.api.records.ResourceRequest;

public class HPCAllocateRequest {
  private int responseID;
  private float appProgress;
  private List<ResourceRequest> resourceAsk;
  private List<ContainerId> containersToBeReleased;
  private ResourceBlacklistRequest resourceBlacklistRequest;

  public int getResponseID() {
    return responseID;
  }

  public void setResponseID(int responseID) {
    this.responseID = responseID;
  }

  public float getAppProgress() {
    return appProgress;
  }

  public void setAppProgress(float appProgress) {
    this.appProgress = appProgress;
  }

  public List<ResourceRequest> getResourceAsk() {
    return resourceAsk;
  }

  public void setResourceAsk(List<ResourceRequest> resourceAsk) {
    this.resourceAsk = resourceAsk;
  }

  public List<ContainerId> getContainersToBeReleased() {
    return containersToBeReleased;
  }

  public void setContainersToBeReleased(List<ContainerId> containersToBeReleased) {
    this.containersToBeReleased = containersToBeReleased;
  }

  public ResourceBlacklistRequest getResourceBlacklistRequest() {
    return resourceBlacklistRequest;
  }

  public void setResourceBlacklistRequest(
      ResourceBlacklistRequest resourceBlacklistRequest) {
    this.resourceBlacklistRequest = resourceBlacklistRequest;
  }
}
