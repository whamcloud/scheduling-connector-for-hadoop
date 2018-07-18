// Copyright (c) 2018 DDN. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package org.apache.hadoop.yarn.hpc.records;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.yarn.api.records.ContainerId;

/**
 * This class shares the information between application master protocol and
 * container manager protocol.
 * 
 */
public class AllocatedContainersInfo {
  private ConcurrentHashMap<ContainerId, Integer> containerIdVsTaskId = 
      new ConcurrentHashMap<ContainerId, Integer>();
  private static AllocatedContainersInfo instance = 
      new AllocatedContainersInfo();

  private AllocatedContainersInfo() {
  }

  public static AllocatedContainersInfo getInstance() {
    return instance;
  }

  public void put(ContainerId containerId, int taskId) {
    containerIdVsTaskId.put(containerId, taskId);
  }

  public int get(ContainerId containerId) {
    return containerIdVsTaskId.get(containerId);
  }

  public void remove(ContainerId containerId) {
    containerIdVsTaskId.remove(containerId);
  }
  
  public boolean contains(ContainerId containerId) {
    return containerIdVsTaskId.containsKey(containerId);
  }

  public Collection<Integer> getTaskIds() {
    return containerIdVsTaskId.values();
  }

  @Override
  public String toString() {
    return containerIdVsTaskId.toString();
  }
}
