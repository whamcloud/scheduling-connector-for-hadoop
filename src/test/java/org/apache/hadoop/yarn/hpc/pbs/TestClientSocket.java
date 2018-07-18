// Copyright (c) 2018 DDN. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package org.apache.hadoop.yarn.hpc.pbs;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

import org.apache.hadoop.yarn.hpc.pbs.util.ClientSocket;
import org.junit.Assert;
import org.junit.Test;

public class TestClientSocket {

  private ServerSocket serverSocket;

  @Test
  public void testClientSocket() throws IOException, InterruptedException {
    final Result result = new Result();
    serverSocket = new ServerSocket(0);
    String hostName = serverSocket.getInetAddress().getHostName();
    int port = serverSocket.getLocalPort();
    new Thread() {
      public void run() {
        try {
          Socket socket = serverSocket.accept();
          DataInputStream in = new DataInputStream(socket.getInputStream());
          DataOutputStream out = new DataOutputStream(socket.getOutputStream());
          while (true) {
            if (in.available() > 0) {
              String cmd = in.readUTF();
              Assert.assertEquals("LAUNCH_SCRIPT", cmd);
              out.writeUTF("This is test command script for child task");
              result.sentCmd = true;
              break;
            }
          }
        } catch (IOException e) {
          e.printStackTrace();
          Assert.fail();
        }
      }
    }.start();

    ClientSocket clientSocket = new ClientSocket(port, hostName, port, port, port, port);
    clientSocket.initialize();
    while (true) {
      if (clientSocket.isCmdReady()) {
        String cmdString = clientSocket.getCmdString();
        Assert.assertEquals("This is test command script for child task",
            cmdString);
        result.receivedCmd = true;
        break;
      }
      Thread.sleep(100);
    }
    Assert.assertEquals(result.sentCmd, true);
    Assert.assertEquals(result.receivedCmd, true);
  }

  class Result {
    boolean sentCmd;
    boolean receivedCmd;
  }
}
