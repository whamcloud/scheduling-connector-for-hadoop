// Copyright (c) 2018 DDN. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package org.apache.hadoop.yarn.hpc.pbs.util;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class ContainerResponses {
  private static Map<Integer, ContainerResponse> responses =
      new ConcurrentHashMap<Integer, ContainerResponse>();

  public static Set<Integer> createdPbsJobIds = new HashSet<Integer>();
  
  public static void put(int pbsJobId, ContainerResponse response) {
    responses.put(pbsJobId, response);
  }

  public static Collection<ContainerResponse> getResponses() {
    return responses.values();
  }

  public static ContainerResponse getResponse(int jobId) {
    return responses.get(jobId);
  }
  
  public static void remove(int jobId) {
    responses.get(jobId);
  }
}
