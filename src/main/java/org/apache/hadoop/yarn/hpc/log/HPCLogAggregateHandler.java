// Copyright (c) 2017 DDN. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package org.apache.hadoop.yarn.hpc.log;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.util.ShutdownHookManager;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.hpc.conf.HPCConfiguration;
import org.apache.hadoop.yarn.logaggregation.AggregatedLogFormat.LogKey;
import org.apache.hadoop.yarn.logaggregation.AggregatedLogFormat.LogValue;
import org.apache.hadoop.yarn.logaggregation.AggregatedLogFormat.LogWriter;
import org.apache.hadoop.yarn.logaggregation.LogAggregationUtils;
import org.apache.hadoop.yarn.util.ConverterUtils;

public class HPCLogAggregateHandler extends CompositeService {
  private static FsPermission APP_DIR_PERMISSIONS = FsPermission
      .createImmutable((short) 0770);
  private static final Log LOG = LogFactory
      .getLog(HPCLogAggregateHandler.class);
  public static final int SHUTDOWN_HOOK_PRIORITY = 30;
  private Path remoteNodeTmpLogFileForApp;
  private Configuration conf;
  private String applicationId;
  private String user;
  private String hostName;
  private FileContext lfs;

  public HPCLogAggregateHandler(String applicationId, String user) {
    super(HPCLogAggregateHandler.class.getName());
    this.applicationId = applicationId;
    this.user = user;
    try {
      this.lfs = FileContext.getLocalFSFileContext();
    } catch (UnsupportedFileSystemException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void serviceInit(Configuration conf) throws Exception {
    this.conf = conf;
    remoteNodeTmpLogFileForApp = new Path(
        conf.get(YarnConfiguration.NM_REMOTE_APP_LOG_DIR));
    hostName = System.getenv("HOSTNAME");
    super.serviceInit(conf);
  }

  @Override
  public void serviceStart() throws Exception {
    try {
      // Get user's FileSystem credentials
      final UserGroupInformation userUgi = UserGroupInformation
          .createRemoteUser(user);

      createAppDir(user, applicationId, userUgi, conf,
          remoteNodeTmpLogFileForApp);

      Path remoteNodeLogFileForApp = getRemoteNodeLogFileForApp(conf,
          remoteNodeTmpLogFileForApp,
          ConverterUtils.toApplicationId(applicationId), user);
      LogWriter writer = new LogWriter(conf, remoteNodeLogFileForApp, userUgi);
      List<ContainerId> containers = getAllContainers(applicationId, conf);
      LOG.info("Starting Log aggregation for containers : " + containers);
      String[] hpcLogDir = HPCConfiguration.getHPCLogDirs(conf);
      List<String> logDirs = Arrays.asList(hpcLogDir);
      for (ContainerId containerId : containers) {
        LogKey logKey = new LogKey(containerId);
        LogValue logValue = new LogValue(logDirs, containerId,
            userUgi.getShortUserName());
        writer.append(logKey, logValue);
      }
      writer.close();
      LOG.info("Log aggregation has completed.");

      // Remove the log files from local dir's
      delete(applicationId, hpcLogDir);

      // Clean up container work dirs
      delete(applicationId, HPCConfiguration.getHPCLocalDirs(conf));

    } catch (Throwable e) {
      throw new RuntimeException("Failed to complete aggregation on "
          + hostName + "for application " + applicationId, e);
    }
    super.serviceStart();
  }

  public void delete(String subDir, String... baseDirs) throws IOException,
      InterruptedException {
    if (baseDirs == null || baseDirs.length == 0) {
      LOG.info("Deleting absolute path : " + subDir);
      if (!lfs.delete(new Path(subDir), true)) {
        LOG.warn("delete returned false for path: [" + subDir + "]");
      }
      return;
    }
    for (String baseDir : baseDirs) {
      Path del = subDir == null ? new Path(baseDir) : new Path(baseDir, subDir);
      LOG.info("Deleting path : " + del);
      if (!lfs.delete(del, true)) {
        LOG.warn("delete returned false for path: [" + del + "]");
      }
    }
  }

  @Override
  public void serviceStop() throws Exception {
    super.serviceStop();
  }

  public static void main(String[] args) {
    try {
      Configuration conf = new YarnConfiguration();
      String appId = args[0];
      String appUser = args[1];
      HPCLogAggregateHandler aggregateHandler = new HPCLogAggregateHandler(
          appId, appUser);
      initAndStartAggregation(conf, appUser, aggregateHandler);
      ShutdownHookManager.get().addShutdownHook(
          new CompositeServiceShutdownHook(aggregateHandler),
          SHUTDOWN_HOOK_PRIORITY);
    } catch (Throwable t) {
      LOG.fatal("Error starting Log Aggregation", t);
      ExitUtil.terminate(1, t);
    }
  }

  private static void initAndStartAggregation(final Configuration conf,
      String appUser, final HPCLogAggregateHandler aggregateHandler)
      throws IOException, InterruptedException {
    UserGroupInformation logAggregatorUgi = UserGroupInformation
        .createRemoteUser(appUser);
    logAggregatorUgi.doAs(new PrivilegedExceptionAction<Object>() {
      @Override
      public Object run() throws Exception {
        aggregateHandler.init(conf);
        aggregateHandler.start();
        return null;
      }
    });
  }

  private Path getRemoteNodeLogFileForApp(Configuration conf,
      Path remoteNodeTmpLogFileForApp, ApplicationId applicationId, String user) {
    return LogAggregationUtils.getRemoteNodeLogFileForApp(
        remoteNodeTmpLogFileForApp, applicationId, user, NodeId.newInstance(
            hostName, 0), conf.get(
            YarnConfiguration.NM_REMOTE_APP_LOG_DIR_SUFFIX,
            YarnConfiguration.DEFAULT_NM_REMOTE_APP_LOG_DIR_SUFFIX));
  }

  private Path createAppDir(String user, String applicationId,
      UserGroupInformation userUgi, Configuration conf,
      Path remoteNodeTmpLogFileForApp) throws IOException {
    FileSystem remoteFS = FileSystem.get(conf);

    // Only creating directories if they are missing to avoid
    // unnecessary load on the filesystem from all of the nodes
    String remoteRootLogDirSuffix = conf.get(
        YarnConfiguration.NM_REMOTE_APP_LOG_DIR_SUFFIX,
        YarnConfiguration.DEFAULT_NM_REMOTE_APP_LOG_DIR_SUFFIX);
    Path appDir = LogAggregationUtils.getRemoteAppLogDir(
        remoteNodeTmpLogFileForApp,
        ConverterUtils.toApplicationId(applicationId), user,
        remoteRootLogDirSuffix);
    appDir = appDir.makeQualified(remoteFS.getUri(),
        remoteFS.getWorkingDirectory());

    if (!checkExists(remoteFS, appDir, APP_DIR_PERMISSIONS)) {
      Path suffixDir = LogAggregationUtils.getRemoteLogSuffixedDir(
          remoteNodeTmpLogFileForApp, user, remoteRootLogDirSuffix);
      suffixDir = suffixDir.makeQualified(remoteFS.getUri(),
          remoteFS.getWorkingDirectory());

      if (!checkExists(remoteFS, suffixDir, APP_DIR_PERMISSIONS)) {
        Path userDir = LogAggregationUtils.getRemoteLogUserDir(
            remoteNodeTmpLogFileForApp, user);
        userDir = userDir.makeQualified(remoteFS.getUri(),
            remoteFS.getWorkingDirectory());

        if (!checkExists(remoteFS, userDir, APP_DIR_PERMISSIONS)) {
          createDir(remoteFS, userDir, APP_DIR_PERMISSIONS);
        }

        createDir(remoteFS, suffixDir, APP_DIR_PERMISSIONS);
      }

      createDir(remoteFS, appDir, APP_DIR_PERMISSIONS);
    }
    return appDir;
  }

  private void createDir(FileSystem fs, Path path, FsPermission fsPerm)
      throws IOException {
    FsPermission dirPerm = new FsPermission(fsPerm);
    fs.mkdirs(path, dirPerm);
    FsPermission umask = FsPermission.getUMask(fs.getConf());
    if (!dirPerm.equals(dirPerm.applyUMask(umask))) {
      fs.setPermission(path, new FsPermission(fsPerm));
    }
  }

  private boolean checkExists(FileSystem fs, Path path, FsPermission fsPerm)
      throws IOException {
    boolean exists = true;
    try {
      FileStatus appDirStatus = fs.getFileStatus(path);
      if (!APP_DIR_PERMISSIONS.equals(appDirStatus.getPermission())) {
        fs.setPermission(path, APP_DIR_PERMISSIONS);
      }
    } catch (FileNotFoundException fnfe) {
      exists = false;
    }
    return exists;
  }

  public List<ContainerId> getAllContainers(String applicationId,
      Configuration conf) {
    List<ContainerId> containers = new ArrayList<ContainerId>();
    String[] logDirs = HPCConfiguration.getHPCLogDirs(conf);
    for (String logDir : logDirs) {
      File appDir = new File(logDir, applicationId);
      if (appDir.exists()) {
        String[] containerDirs = appDir.list();
        if (containerDirs != null) {
          for (String containerDir : containerDirs) {
            try {
              ContainerId containerId = ConverterUtils
                  .toContainerId(containerDir);
              containers.add(containerId);
            } catch (Exception e) {
              // Skip the non container dir's
              LOG.debug("Skipping non container dir : " + containerDir, e);
            }
          }
        }
      }
    }
    return containers;
  }
}
