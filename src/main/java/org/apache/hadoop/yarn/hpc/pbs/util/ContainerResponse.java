// Copyright (c) 2017 DDN. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package org.apache.hadoop.yarn.hpc.pbs.util;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class ContainerResponse {
  private static final Log LOG = LogFactory.getLog(ContainerResponse.class);
  private boolean assigned;
  private int pbsJobId;
  private int priority;
  private int memory;
  private int cores;
  private DataOutputStream out;
  private Socket accept;
  private boolean cmdSent;
  private String containerHostName;

  public ContainerResponse(int pbsJobId, int priority, int memory, int cores,
      Socket accept, DataOutputStream out, String containerHostName) {
    this.pbsJobId = pbsJobId;
    this.priority = priority;
    this.memory = memory;
    this.cores = cores;
    this.accept = accept;
    this.out = out;
    this.containerHostName = containerHostName;
  }

  public boolean isAssigned() {
    return assigned;
  }

  public void setAssigned(boolean assigned) {
    this.assigned = assigned;
  }

  public int getPriority() {
    return priority;
  }

  public int getMemory() {
    return memory;
  }

  public int getCores() {
    return cores;
  }

  public int getPbsJobId() {
    return pbsJobId;
  }

  public void writeChildCommand(String childCommand) {
    LOG.info("Sending command to container launcher");
    LOG.debug("Container launch Command : " + childCommand);
    try {
      out.writeUTF(childCommand);
      cmdSent = true;
    } catch (IOException e) {
      LOG.error("Failed to write child command to container.", e);
    }
  }
  
  public void writeShutDownCommand() {
    if (!cmdSent) {
      LOG.info("Sending shutdown command to container launcher");
      try {
        out.writeUTF("####SHUTDOWN####");
      } catch (IOException e) {
        LOG.error("Failed to write shutdown command to container.", e);
      }
    }
  }

  public void close() {
    if (out != null) {
      try {
        out.close();
      } catch (IOException e) {
        // Ignore
        LOG.error("Error during closing out.", e);
      }
    }

    if (accept != null) {
      try {
        accept.close();
      } catch (IOException e) {
        // Ignore
        LOG.error("Error during closing socket.", e);
      }
    }
  }

  public String getContainerHostName() {
    return containerHostName;
  }
}
