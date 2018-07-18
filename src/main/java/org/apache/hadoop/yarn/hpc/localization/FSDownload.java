// Copyright (c) 2017 DDN. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package org.apache.hadoop.yarn.hpc.localization;

import java.io.File;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.concurrent.Callable;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.RunJar;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;

public class FSDownload implements Callable<Path> {

  private static final Log LOG = LogFactory.getLog(FSDownload.class);

  private FileContext files;
  private final UserGroupInformation userUgi;
  private Configuration conf;
  private LocalizationResource resource;

  private Path destDirPath;

  private static final FsPermission cachePerms = new FsPermission((short) 0755);
  static final FsPermission PUBLIC_FILE_PERMS = new FsPermission((short) 0555);
  static final FsPermission PRIVATE_FILE_PERMS = new FsPermission((short) 0500);
  static final FsPermission PUBLIC_DIR_PERMS = new FsPermission((short) 0755);
  static final FsPermission PRIVATE_DIR_PERMS = new FsPermission((short) 0700);

  public FSDownload(FileContext files, UserGroupInformation ugi,
      Configuration conf, Path destDirPath, LocalizationResource resource) {
    this.conf = conf;
    this.destDirPath = destDirPath;
    this.files = files;
    this.userUgi = ugi;
    this.resource = resource;
  }

  private void createDir(Path path, FsPermission perm) throws IOException {
    files.mkdir(path, perm, false);
    if (!perm.equals(files.getUMask().applyUMask(perm))) {
      files.setPermission(path, perm);
    }
  }

  private Path copy(Path sCopy, Path dstdir) throws IOException {
    FileSystem sourceFs = sCopy.getFileSystem(conf);
    Path dCopy = new Path(dstdir, "tmp_" + sCopy.getName());
    FileStatus sStat = sourceFs.getFileStatus(sCopy);
    FileUtil.copy(sourceFs, sStat, FileSystem.getLocal(conf), dCopy, false,
        true, conf);
    return dCopy;
  }

  private long unpack(File localrsrc, File dst) throws IOException {
    switch (resource.getType()) {
    case ARCHIVE: {
      String lowerDst = dst.getName().toLowerCase();
      if (lowerDst.endsWith(".jar")) {
        RunJar.unJar(localrsrc, dst);
      } else if (lowerDst.endsWith(".zip")) {
        FileUtil.unZip(localrsrc, dst);
      } else if (lowerDst.endsWith(".tar.gz") || lowerDst.endsWith(".tgz")
          || lowerDst.endsWith(".tar")) {
        FileUtil.unTar(localrsrc, dst);
      } else {
        LOG.warn("Cannot unpack " + localrsrc);
        if (!localrsrc.renameTo(dst)) {
          throw new IOException("Unable to rename file: [" + localrsrc
              + "] to [" + dst + "]");
        }
      }
    }
      break;
    case PATTERN: {
      String lowerDst = dst.getName().toLowerCase();
      if (lowerDst.endsWith(".jar")) {
        String p = resource.getPattern();
        RunJar.unJar(localrsrc, dst,
            p == null ? RunJar.MATCH_ANY : Pattern.compile(p));
        File newDst = new File(dst, dst.getName());
        if (!dst.exists() && !dst.mkdir()) {
          throw new IOException("Unable to create directory: [" + dst + "]");
        }
        if (!localrsrc.renameTo(newDst)) {
          throw new IOException("Unable to rename file: [" + localrsrc
              + "] to [" + newDst + "]");
        }
      } else if (lowerDst.endsWith(".zip")) {
        LOG.warn("Treating [" + localrsrc + "] as an archive even though it "
            + "was specified as PATTERN");
        FileUtil.unZip(localrsrc, dst);
      } else if (lowerDst.endsWith(".tar.gz") || lowerDst.endsWith(".tgz")
          || lowerDst.endsWith(".tar")) {
        LOG.warn("Treating [" + localrsrc + "] as an archive even though it "
            + "was specified as PATTERN");
        FileUtil.unTar(localrsrc, dst);
      } else {
        LOG.warn("Cannot unpack " + localrsrc);
        if (!localrsrc.renameTo(dst)) {
          throw new IOException("Unable to rename file: [" + localrsrc
              + "] to [" + dst + "]");
        }
      }
    }
      break;
    case FILE:
    default:
      if (!localrsrc.renameTo(dst)) {
        throw new IOException("Unable to rename file: [" + localrsrc + "] to ["
            + dst + "]");
      }
      break;
    }
    if (localrsrc.isFile()) {
      try {
        files.delete(new Path(localrsrc.toString()), false);
      } catch (IOException ignore) {
      }
    }
    return 0;
  }

  @Override
  public Path call() throws Exception {
    final Path sCopy = resource.getResource();
    createDir(destDirPath, cachePerms);
    final Path dst_work = destDirPath;
    createDir(dst_work, cachePerms);
    Path dFinal = files.makeQualified(new Path(dst_work, resource
        .getTargetName()));
    try {
      Path dTmp = null == userUgi ? files.makeQualified(copy(sCopy, dst_work))
          : userUgi.doAs(new PrivilegedExceptionAction<Path>() {
            public Path run() throws Exception {
              return files.makeQualified(copy(sCopy, dst_work));
            };
          });
      unpack(new File(dTmp.toUri()), new File(dFinal.toUri()));
      changePermissions(dFinal.getFileSystem(conf), dFinal);
    } catch (Exception e) {
      throw e;
    } finally {
      conf = null;
      resource = null;
    }
    return files.makeQualified(new Path(destDirPath, sCopy.getName()));
  }

  private void changePermissions(FileSystem fs, final Path path)
      throws IOException, InterruptedException {
    File f = new File(path.toUri());
    if (FileUtils.isSymlink(f)) {
      // avoid following symlinks when changing permissions
      return;
    }
    boolean isDir = f.isDirectory();
    FsPermission perm = cachePerms;
    // set public perms as 755 or 555 based on dir or file
    if (resource.getVisibility() == LocalResourceVisibility.PUBLIC) {
      perm = isDir ? PUBLIC_DIR_PERMS : PUBLIC_FILE_PERMS;
    }
    // set private perms as 700 or 500
    else {
      // PRIVATE:
      // APPLICATION:
      perm = isDir ? PRIVATE_DIR_PERMS : PRIVATE_FILE_PERMS;
    }
    LOG.debug("Changing permissions for path " + path + " to perm " + perm);
    final FsPermission fPerm = perm;
    if (null == userUgi) {
      files.setPermission(path, perm);
    } else {
      userUgi.doAs(new PrivilegedExceptionAction<Void>() {
        public Void run() throws Exception {
          files.setPermission(path, fPerm);
          return null;
        }
      });
    }
    if (isDir) {
      FileStatus[] statuses = fs.listStatus(path);
      for (FileStatus status : statuses) {
        changePermissions(fs, status.getPath());
      }
    }
  }
}
