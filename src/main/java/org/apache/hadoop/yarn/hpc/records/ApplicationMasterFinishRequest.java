// Copyright (c) 2017 DDN. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package org.apache.hadoop.yarn.hpc.records;

import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;

public class ApplicationMasterFinishRequest {
  private FinalApplicationStatus finalAppStatus;
  private String diagnostics;
  private String url;

  public ApplicationMasterFinishRequest(FinalApplicationStatus finalAppStatus,
      String diagnostics, String url) {
    this.finalAppStatus = finalAppStatus;
    this.diagnostics = diagnostics;
    this.url = url;
  }

  public FinalApplicationStatus getFinalAppStatus() {
    return finalAppStatus;
  }

  public String getDiagnostics() {
    return diagnostics;
  }

  public String getUrl() {
    return url;
  }
}
