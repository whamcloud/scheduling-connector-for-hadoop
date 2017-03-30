// Copyright (c) 2017 Intel Corporation. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package org.apache.hadoop.yarn.hpc.localization;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;

public class LocalizationResource {
  private Path resourcePath;
  private LocalResourceVisibility visibility;
  private LocalResourceType type;
  private String pattern;
  private String targetName;

  public LocalizationResource(String resource, String targetName,
      LocalResourceVisibility visibility, String resourceType, String pattern) {
    this.targetName = targetName;
    this.type = LocalResourceType.valueOf(resourceType);
    this.pattern = pattern;
    this.resourcePath = new Path(resource);
    this.visibility = visibility;
  }

  public LocalResourceVisibility getVisibility() {
    return visibility;
  }

  public void setVisibility(LocalResourceVisibility visibility) {
    this.visibility = visibility;
  }

  public LocalResourceType getType() {
    return type;
  }

  public Path getResource() {
    return resourcePath;
  }

  public String getPattern() {
    return pattern;
  }

  public String getTargetName() {
    return targetName;
  }
}
