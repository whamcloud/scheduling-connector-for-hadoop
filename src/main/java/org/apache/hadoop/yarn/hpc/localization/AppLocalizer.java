// Copyright (c) 2017 DDN. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package org.apache.hadoop.yarn.hpc.localization;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Collection;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.hpc.conf.HPCConfiguration;

public class AppLocalizer {

  private static final Log LOG = LogFactory.getLog(AppLocalizer.class);

  public static void main(String[] args) throws UnsupportedEncodingException {
    // It should get at least one file to localize
    if (args == null || args.length < 6) {
      LOG.info("There are no files to localize.");
      System.exit(0);
    }
    LOG.info("Container localization process started.");
    // arg[0] - localization directory
    String localizationDir = args[0];
    try {
      // arg[1] - No of localization threads
      int localizeThreads = Integer.parseInt(args[1].trim());
      Collection<LocalizationResource> resources = new ArrayList<LocalizationResource>();
      for (int i = 2; i < args.length; i = i + 5) {
        // args[i] - localization resource
        // args[i+1] - resource target name
        // args[i+2] - visibility
        // args[i+3] - resource type
        // args[i+4] - pattern
        LocalizationResource resource = new LocalizationResource(args[i],
            args[i + 1], LocalResourceVisibility.valueOf(args[i + 2]),
            args[i + 3], URLDecoder.decode(args[i + 4],
                HPCConfiguration.CHAR_ENCODING));
        resources.add(resource);
      }
      LocalizationService service = new LocalizationService(localizationDir,
          localizeThreads, resources);
      service.localizeFiles();
      LOG.info("Container localization process completed.");
    } catch (Throwable e) {
      LOG.error("Exception occured during container localization process.", e);
    }
  }
}
