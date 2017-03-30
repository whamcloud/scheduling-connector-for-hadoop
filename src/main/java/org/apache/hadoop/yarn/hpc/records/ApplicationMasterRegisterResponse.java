// Copyright (c) 2017 Intel Corporation. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package org.apache.hadoop.yarn.hpc.records;

import org.apache.hadoop.yarn.api.records.Resource;

public class ApplicationMasterRegisterResponse {
  private Resource maxCapability;
  private String queue;

  public Resource getMaxCapability() {
    return maxCapability;
  }

  public void setMaxCapability(Resource maxCapability) {
    this.maxCapability = maxCapability;
  }

  public String getQueue() {
    return queue;
  }

  public void setQueue(String queue) {
    this.queue = queue;
  }
}