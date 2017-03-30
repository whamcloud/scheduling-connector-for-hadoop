// Copyright (c) 2017 Intel Corporation. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package org.apache.hadoop.yarn.hpc.pbs.util;

import java.io.IOException;

import org.apache.hadoop.util.Shell;
import org.apache.hadoop.yarn.hpc.util.HPCCommandExecutor.StdInputThread;

public class ContainerLauncher {
  public static void main(String[] args) throws InterruptedException,
      IOException {
    if (args != null && args.length >= 3) {
      System.out.println("PBS Job ID : " + args[0] + ", Host Name :" + args[1]
          + ", Port :" + args[2]);
    } else {
      System.err.println("Invalid Number of arguments : " + args.length);
      System.exit(-1);
    }

    ClientSocket clientSocket = new ClientSocket(Integer.parseInt(args[0].substring(0, args[0].indexOf("."))), args[1],
        Integer.parseInt(args[2]), Integer.parseInt(args[3]),
        Integer.parseInt(args[4]), Integer.parseInt(args[5]));
    clientSocket.initialize();
    while (clientSocket.isCmdReady() == false) {
      Thread.sleep(100);
    }
    String cmdString = clientSocket.getCmdString();
    if ("####SHUTDOWN####".equals(cmdString)) {
      System.out
          .println("Received shutdown command from master, Shutting down.");
      System.exit(0);
    }
    Shell.ShellCommandExecutor shell = new Shell.ShellCommandExecutor(
        new String[] { "sh" });
    StdInputThread exec = new StdInputThread(shell, cmdString);
    exec.start();
    shell.execute();
    exec.checkException();
  }
}
