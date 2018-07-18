// Copyright (c) 2018 DDN. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package org.apache.hadoop.yarn.hpc;

import static org.junit.Assert.assertTrue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.api.ContainerManagementProtocol;
import org.junit.Test;

public class TestHadoopYarnHPCRPC {

  @Test
  public void testGetProxyForApplicationClientProtocol() {
    HadoopYarnHPCRPC hpcRPC = new HadoopYarnHPCRPC();
    Object proxy = hpcRPC.getProxy(ApplicationClientProtocol.class, null,
        new Configuration());
    assertTrue("Instance not expected",
        proxy instanceof ApplicationClientProtocol);
  }

  @Test
  public void testGetProxyForContainerManagementProtocol() {
    HadoopYarnHPCRPC hpcRPC = new HadoopYarnHPCRPC();
    Object proxy = hpcRPC.getProxy(ContainerManagementProtocol.class, null,
        new Configuration());
    assertTrue("Instance not expected",
        proxy instanceof ContainerManagementProtocol);
  }
}
