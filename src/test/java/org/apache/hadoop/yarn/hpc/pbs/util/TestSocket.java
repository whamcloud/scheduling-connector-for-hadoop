// Copyright (c) 2018 DDN. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package org.apache.hadoop.yarn.hpc.pbs.util;

import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;

public class TestSocket {
  public static void main(String[] args) throws IOException {
    Socket socket = null;
    DataInputStream in = null;
    DataOutputStream out = null;
    try {
      socket = new Socket("0.0.0.0", 50784);
      in = new DataInputStream(socket.getInputStream());
      out = new DataOutputStream(socket.getOutputStream());
      out.writeUTF("LAUNCH_SCRIPT");
      String sciptToLaunch = in.readUTF();
      System.out.println(sciptToLaunch);
    } finally {
      close(out);
      close(in);
      close(socket);
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
