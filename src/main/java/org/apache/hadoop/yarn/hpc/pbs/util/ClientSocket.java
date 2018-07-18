// Copyright (c) 2018 DDN. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package org.apache.hadoop.yarn.hpc.pbs.util;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;

public class ClientSocket {
  private String hostName;
  private int port;
  private boolean isCmdReady;
  private String cmdString;
  private int priority;
  private int memory;
  private int cores;
  private int pbsJobId;

  public ClientSocket(int pbsJobId, String hostName, int port, int priority, int memory,
      int cores) {
    this.pbsJobId = pbsJobId;
    this.hostName = hostName;
    this.port = port;
    this.priority = priority;
    this.memory = memory;
    this.cores = cores;
  }

  public void initialize() throws IOException {
    final Socket client = new Socket(hostName, port);
    final DataInputStream in = new DataInputStream(client.getInputStream());
    DataOutputStream out = new DataOutputStream(client.getOutputStream());
    out.writeUTF("LAUNCH_SCRIPT");
    out.writeInt(pbsJobId);
    out.writeInt(priority);
    out.writeInt(memory);
    out.writeInt(cores);
    out.writeUTF(InetAddress.getLocalHost().getHostName());
    Thread t = new Thread() {

      @Override
      public void run() {
        try {
          while (true) {
            if (in.available() > 0) {
              cmdString = in.readUTF();
              isCmdReady = true;
              break;
            }
            try {
              //TODO Need to handle it better
              Thread.sleep(10);
            } catch (InterruptedException e) {
              e.printStackTrace();
            }
          }
          client.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    };
    t.start();
  }

  public boolean isCmdReady() {
    return isCmdReady;
  }

  public String getCmdString() {
    return cmdString;
  }
}
