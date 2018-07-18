// Copyright (c) 2017 DDN. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package org.apache.hadoop.yarn.hpc.pbs.util;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.yarn.api.records.ContainerId;

public class LaunchedPBSJobs {
  private static Map<Integer, Integer> pbsJobIdVsPriority = new HashMap<Integer, Integer>();
  private static Map<ContainerId, Integer> runningContainers = new HashMap<ContainerId, Integer>();

  public static void put(Integer pbsJobId, Integer priority) {
    pbsJobIdVsPriority.put(pbsJobId, priority);
  }

  public static boolean contains(Integer pbsJobId) {
    return pbsJobIdVsPriority.containsKey(pbsJobId);
  }

  public static Integer getPriority(Integer pbsJobId) {
    return pbsJobIdVsPriority.get(pbsJobId);
  }

  public static void addRunningContainer(ContainerId containerId,
      Integer pbsJobId) {
    runningContainers.put(containerId, pbsJobId);
  }

  public static void removeRunningContainer(ContainerId containerId) {
    runningContainers.remove(containerId);
  }

  public static Map<ContainerId, Integer> getRunningContainers() {
    return runningContainers;
  }
}
