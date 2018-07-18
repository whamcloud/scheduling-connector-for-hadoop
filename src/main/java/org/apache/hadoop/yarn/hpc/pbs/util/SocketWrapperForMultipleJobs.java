// Copyright (c) 2018 DDN. All rights reserved.
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

public class SocketWrapperForMultipleJobs {
  private static final Log LOG = LogFactory
      .getLog(SocketWrapperForMultipleJobs.class);
  private ServerSocket serverSocket;
  private int port;
  private String hostName;
  private boolean closed;

  public void initialize() throws IOException {
    serverSocket = new ServerSocket(0);
    this.port = serverSocket.getLocalPort();
    this.hostName = InetAddress.getLocalHost().getHostName();
    Thread t = new Thread() {

      @Override
      public void run() {
        while (!isClosed()) {
          Socket accept;
          try {
            accept = serverSocket.accept();
            DataInputStream in = new DataInputStream(accept.getInputStream());
            DataOutputStream out = new DataOutputStream(
                accept.getOutputStream());
            while (true) {
              if (in.available() > 0) {
                String reqString = in.readUTF();
                if ("LAUNCH_SCRIPT".equals(reqString)) {
                  int pbsJobId = in.readInt();
                  int priority = in.readInt();
                  int memory = in.readInt();
                  int cores = in.readInt();
                  String containerHostName = in.readUTF();
                  ContainerResponse containerResponse = new ContainerResponse(
                      pbsJobId, priority, memory, cores, accept, out, containerHostName);
                  ContainerResponses.put(pbsJobId, containerResponse);
                  break;
                } else {
                  LOG.warn("Invalid message received : " + reqString);
                }
              }
              try {
                Thread.sleep(100);
              } catch (InterruptedException e) {
                LOG.warn("InterruptedException occured.", e);
              }
            }
          } catch (IOException e) {
            if (!isClosed()) {
              LOG.error("IOException occured.", e);
            }
            break;
          }
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

  public void close() {
    closed = true;
    if (serverSocket != null) {
      try {
        serverSocket.close();
      } catch (IOException e) {
        LOG.warn("Failed to close socket.", e);
      }
    }
  }

  public boolean isClosed() {
    return closed;
  }
}
