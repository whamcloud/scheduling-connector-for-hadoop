// Copyright (c) 2017 DDN. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package org.apache.hadoop.yarn.hpc;

import java.net.InetSocketAddress;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.api.ApplicationMasterProtocol;
import org.apache.hadoop.yarn.api.ContainerManagementProtocol;
import org.apache.hadoop.yarn.hpc.impl.HPCApplicationClientProtocolImpl;
import org.apache.hadoop.yarn.hpc.impl.HPCApplicationMasterProtocolImpl;
import org.apache.hadoop.yarn.hpc.impl.HPCContainerManagementProtocolImpl;
import org.apache.hadoop.yarn.hpc.records.AllocatedContainersInfo;
import org.apache.hadoop.yarn.ipc.HadoopYarnProtoRPC;

/**
 * It uses HPC Job Schedulers to perform the operations required for completing
 * the Yarn Applications.
 */
@InterfaceAudience.LimitedPrivate({ "MapReduce", "YARN" })
public class HadoopYarnHPCRPC extends HadoopYarnProtoRPC {
  private AllocatedContainersInfo containersInfo = AllocatedContainersInfo.getInstance();

  @Override
  public Object getProxy(@SuppressWarnings("rawtypes") Class protocol, InetSocketAddress address, Configuration conf) {
    Object proxy;
    if (protocol == ApplicationClientProtocol.class) {
      proxy = new HPCApplicationClientProtocolImpl(conf);
    } else if (protocol == ApplicationMasterProtocol.class) {
      proxy = new HPCApplicationMasterProtocolImpl(conf, containersInfo);
    } else if (protocol == ContainerManagementProtocol.class) {
      proxy = new HPCContainerManagementProtocolImpl(conf, containersInfo);
    } else {
      proxy = super.getProxy(protocol, address, conf);
    }
    return proxy;
  }
}
