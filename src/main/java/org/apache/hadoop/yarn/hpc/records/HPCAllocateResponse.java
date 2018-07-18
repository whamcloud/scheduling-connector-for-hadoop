// Copyright (c) 2017 DDN. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package org.apache.hadoop.yarn.hpc.records;

import java.util.List;

import org.apache.hadoop.yarn.api.records.AMCommand;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NMToken;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.PreemptionMessage;
import org.apache.hadoop.yarn.api.records.Resource;

public class HPCAllocateResponse {
  private int responseId;
  private List<ContainerStatus> completedContainers;
  private List<Container> allocatedContainers;
  private List<NodeReport> updatedNodes;
  private Resource availResources;
  private AMCommand command;
  private int numClusterNodes;
  private PreemptionMessage preempt;
  private List<NMToken> nmTokens;

  public int getResponseId() {
    return responseId;
  }

  public void setResponseId(int responseId) {
    this.responseId = responseId;
  }

  public List<ContainerStatus> getCompletedContainers() {
    return completedContainers;
  }

  public void setCompletedContainers(List<ContainerStatus> completedContainers) {
    this.completedContainers = completedContainers;
  }

  public List<Container> getAllocatedContainers() {
    return allocatedContainers;
  }

  public void setAllocatedContainers(List<Container> allocatedContainers) {
    this.allocatedContainers = allocatedContainers;
  }

  public List<NodeReport> getUpdatedNodes() {
    return updatedNodes;
  }

  public void setUpdatedNodes(List<NodeReport> updatedNodes) {
    this.updatedNodes = updatedNodes;
  }

  public Resource getAvailResources() {
    return availResources;
  }

  public void setAvailResources(Resource availResources) {
    this.availResources = availResources;
  }

  public AMCommand getCommand() {
    return command;
  }

  public void setCommand(AMCommand command) {
    this.command = command;
  }

  public int getNumClusterNodes() {
    return numClusterNodes;
  }

  public void setNumClusterNodes(int numClusterNodes) {
    this.numClusterNodes = numClusterNodes;
  }

  public PreemptionMessage getPreempt() {
    return preempt;
  }

  public void setPreempt(PreemptionMessage preempt) {
    this.preempt = preempt;
  }

  public List<NMToken> getNmTokens() {
    return nmTokens;
  }

  public void setNmTokens(List<NMToken> nmTokens) {
    this.nmTokens = nmTokens;
  }
}
