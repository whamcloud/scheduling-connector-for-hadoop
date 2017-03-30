// Copyright (c) 2017 Intel Corporation. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package org.apache.hadoop.yarn.hpc.localization;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

public class LocalizationService {

  private String localizationDir;
  private Collection<LocalizationResource> resources;
  private int localizeThreads;

  public LocalizationService(String localizationDir, int localizeThreads,
      Collection<LocalizationResource> resources) {
    this.localizationDir = localizationDir;
    this.localizeThreads = localizeThreads;
    this.resources = resources;
  }

  public void localizeFiles() throws IOException {
    ThreadFactory tf = new ThreadFactoryBuilder()
        .setNameFormat("Localizer #%d").build();
    ExecutorService newFixedThreadPool = Executors.newFixedThreadPool(
        localizeThreads, tf);
    for (LocalizationResource resource : resources) {
      UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
      Configuration conf = new Configuration();
      FSDownload fsDownload = new FSDownload(
          FileContext.getLocalFSFileContext(), ugi, conf, new Path(
              localizationDir), resource);
      newFixedThreadPool.submit(fsDownload);
    }
    newFixedThreadPool.shutdown();
  }
}
