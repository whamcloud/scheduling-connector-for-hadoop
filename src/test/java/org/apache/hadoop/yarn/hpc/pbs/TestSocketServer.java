// Copyright (c) 2018 DDN. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package org.apache.hadoop.yarn.hpc.pbs;

import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

public class TestSocketServer {
  public static void main(String[] args) throws IOException {
    ServerSocket ss = null;
    Socket accept = null;
    DataInputStream in = null;
    DataOutputStream out = null;
    try {
      ss = new ServerSocket(0);
      System.out.println("Before accept : " + ss.getLocalPort());
      accept = ss.accept();
      System.out.println("After accept");
      in = new DataInputStream(accept.getInputStream());
      out = new DataOutputStream(accept.getOutputStream());
      String command = in.readUTF();
      if ("LAUNCH_SCRIPT".equals(command)) {
        out.writeUTF("This is scipt for launch");
      }
    } catch (IOException ex) {
      throw ex;
    } finally {
      close(out);
      close(in);
      close(accept);
      close(ss);
    }
  }

  private static void close(Closeable closeable) {
    if (closeable != null) {
      try {
        closeable.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }
}
