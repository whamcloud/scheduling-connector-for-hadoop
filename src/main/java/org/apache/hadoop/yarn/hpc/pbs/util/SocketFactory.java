// Copyright (c) 2017 DDN. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package org.apache.hadoop.yarn.hpc.pbs.util;

import java.io.IOException;

public class SocketFactory {
  public static SocketWrapper createSocket() throws IOException {
    SocketWrapper socketWrapper = new SocketWrapper();
    socketWrapper.initialize();
    return socketWrapper;
  }
}
