// Copyright (c) 2017 DDN. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package org.apache.hadoop.yarn.hpc.pbs.util;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class SocketWrapper {
  private static final Log LOG = LogFactory.getLog(SocketWrapper.class);
  private ServerSocket serverSocket;
  private int port;
  private String hostName;
  private boolean isReady;
  private Object readyLock = new Object();
  private Object commandLock = new Object();
  private String childCommand;
  private boolean assigned;
  private int priority;
  private int memory;
  private int cores;
  private int pbsJobId;
  private String containerHostName;

  public void initialize() throws IOException {
    serverSocket = new ServerSocket(0);
    this.port = serverSocket.getLocalPort();
//    this.hostName = serverSocket.getInetAddress().getHostAddress();
    this.hostName = InetAddress.getLocalHost().getHostName();
    Thread t = new Thread() {

      @Override
      public void run() {
        Socket accept;
        try {
          accept = serverSocket.accept();
          DataInputStream in = new DataInputStream(accept.getInputStream());
          DataOutputStream out = new DataOutputStream(accept.getOutputStream());
          while (true) {
            if (in.available() > 0) {
              String reqString = in.readUTF();
              if ("LAUNCH_SCRIPT".equals(reqString)) {
                pbsJobId = in.readInt();
                priority = in.readInt();
                memory = in.readInt();
                cores = in.readInt();
                containerHostName = in.readUTF();
                synchronized (readyLock) {
                  isReady = true;
                  readyLock.notify();
                }
                break;
              } else {
                LOG.warn("Invalid message received : " + reqString);
              }
            }
            try {
              Thread.sleep(100);
            } catch (InterruptedException e) {
              e.printStackTrace();
            }
          }
          // Send the command script for launching child task
          if (childCommand == null) {
            synchronized (commandLock) {
              while (childCommand == null) {
                try {
                  commandLock.wait();
                } catch (InterruptedException e) {
                  LOG.warn(
                      "InterruptedException while waiting for child command.",
                      e);
                }
              }
            }
          }
//          LOG.info("Writing child command to Task : " + childCommand);
          out.writeUTF(childCommand);
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    };
    t.start();
  }

  public int getPort() {
    return port;
  }

  public String getHostName() {
    return hostName;
  }

  public boolean isReady() {
    return isReady;
  }

  public boolean waitForReady() {
    if (isReady) {
      return true;
    }
    synchronized (readyLock) {
      try {
        readyLock.wait();
      } catch (InterruptedException e) {
        LOG.warn("InterruptedException while waiting for socket to be ready.",
            e);
      }
    }
    return isReady;
  }

  public boolean waitForReady(int timeout) {
    if (isReady) {
      return true;
    }
    synchronized (readyLock) {
      try {
        readyLock.wait(timeout);
      } catch (InterruptedException e) {
        LOG.warn("InterruptedException while waiting for socket to be ready.",
            e);
      }
    }
    return isReady;
  }

  public void setChildCommand(String childCommand) {
    synchronized (commandLock) {
      this.childCommand = childCommand;
      commandLock.notify();
    }
  }
  
  public void setAssigned(boolean assigned) {
    this.assigned = assigned;
  }
  
  public boolean isAssigned() {
    return assigned;
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
  
  public void close() {
    if (serverSocket != null) {
      try {
        serverSocket.close();
      } catch (IOException e) {
        LOG.warn("Failed to close socket.", e);
      }
    }
  }

  public String getContainerHostName() {
    return containerHostName;
  }
}
