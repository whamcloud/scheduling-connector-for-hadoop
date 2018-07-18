// Copyright (c) 2017 DDN. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package org.apache.hadoop.yarn.hpc.records;

public class ApplicationMasterFinishResponse {
  private boolean isUnregistered;

  public ApplicationMasterFinishResponse(boolean isUnregistered) {
    this.isUnregistered = isUnregistered;
  }

  public boolean isUnregistered() {
    return isUnregistered;
  }
}
