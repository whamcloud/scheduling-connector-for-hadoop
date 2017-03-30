// Copyright (c) 2017 Intel Corporation. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package org.apache.hadoop.yarn.hpc.util;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class UnzipUtil {
  private static final Log LOG = LogFactory.getLog(UnzipUtil.class);
  private static final int BUFFER_SIZE = 4096;

  public static void main(String[] args) throws IOException {
    UnzipUtil unZip = new UnzipUtil();
    unZip.unzip(args[0], args[1]);
  }

  public void unzip(String zipFile, String destDirectory) throws IOException {
    File destDir = new File(destDirectory);
    if (!destDir.exists()) {
      if (destDir.mkdir() == false) {
        LOG.warn("Failed to create directory : " + destDir);
      }
    }
    FileInputStream inStream = null;
    ZipInputStream zipIn = null;
    IOException exception = null;
    try {
      inStream = new FileInputStream(zipFile);
      zipIn = new ZipInputStream(inStream);
      ZipEntry entry = zipIn.getNextEntry();
      while (entry != null) {
        String filePath = destDirectory + File.separator + entry.getName();
        if (!entry.isDirectory()) {
          extractFile(zipIn, filePath);
        } else {
          File dir = new File(filePath);
          if (dir.mkdir() == false) {
            LOG.warn("Failed to create directory : " + dir);
          }
        }
        zipIn.closeEntry();
        entry = zipIn.getNextEntry();
      }
    } catch (IOException e) {
      exception = e;
    } finally {
      if (zipIn != null) {
        try {
          zipIn.close();
        } catch (IOException e) {
          if (exception == null) {
            exception = e;
          }
        }
      }
      if (inStream != null) {
        try {
          inStream.close();
        } catch (IOException e) {
          if (exception == null) {
            exception = e;
          }
        }
      }
      if (exception != null) {
        throw exception;
      }
    }
  }

  private void extractFile(ZipInputStream zipIn, String filePath)
      throws IOException {
    //check and create parent dir's if doesn't exist
    File file = new File(filePath);
    File parentFile = file.getParentFile();
    if (parentFile.exists() == false) {
      if (parentFile.mkdirs() == false) {
        LOG.warn("Failed to create directory : " + parentFile);
      }
    }
    FileOutputStream fileOutStream = null;
    BufferedOutputStream bos = null;
    try {
      fileOutStream = new FileOutputStream(filePath);
      bos = new BufferedOutputStream(fileOutStream);
      byte[] bytesIn = new byte[BUFFER_SIZE];
      int read = 0;
      while ((read = zipIn.read(bytesIn)) != -1) {
        bos.write(bytesIn, 0, read);
      }
    } finally {
      if (bos != null) {
        bos.close();
      }
      if (fileOutStream != null) {
        fileOutStream.close();
      }
    }
  }
}