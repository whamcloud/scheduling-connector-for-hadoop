// Copyright (c) 2018 DDN. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package org.apache.hadoop.yarn.hpc.records;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Resource;

public class NewApplicationResponse {
  private ApplicationId applicationId;
  private Resource minCapability;
  private Resource maxCapability;

  public ApplicationId getApplicationId() {
    return applicationId;
  }

  public void setApplicationId(ApplicationId applicationId) {
    this.applicationId = applicationId;
  }

  public Resource getMinCapability() {
    return minCapability;
  }

  public void setMinCapability(Resource minCapability) {
    this.minCapability = minCapability;
  }

  public Resource getMaxCapability() {
    return maxCapability;
  }

  public void setMaxCapability(Resource maxCapability) {
    this.maxCapability = maxCapability;
  }
}
