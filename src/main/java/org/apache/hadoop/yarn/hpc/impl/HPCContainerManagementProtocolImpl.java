// Copyright (c) 2017 Intel Corporation. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package org.apache.hadoop.yarn.hpc.impl;

import java.io.Closeable;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.yarn.api.ContainerManagementProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerStatusesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerStatusesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainersRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainersResponse;
import org.apache.hadoop.yarn.api.protocolrecords.StopContainersRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StopContainersResponse;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.hpc.api.HPCContainerManager;
import org.apache.hadoop.yarn.hpc.conf.HPCConfiguration;
import org.apache.hadoop.yarn.hpc.records.AllocatedContainersInfo;
import org.apache.hadoop.yarn.hpc.records.ContainersResponse;
import org.apache.hadoop.yarn.hpc.records.ContainersStatusResponse;

public class HPCContainerManagementProtocolImpl implements
    ContainerManagementProtocol, Closeable {
  private HPCContainerManager containerManager;

  public HPCContainerManagementProtocolImpl(Configuration conf,
      AllocatedContainersInfo containersInfo) {
    Class<? extends HPCContainerManager> containerMgrClass = conf.getClass(
        HPCConfiguration.YARN_APPLICATION_HPC_CONTAINERMANAGER_CLASS, 
        HPCConfiguration.DEFAULT_YARN_APPLICATION_HPC_CONTAINERMANAGER_CLASS,
        HPCContainerManager.class);
    containerManager = ReflectionUtils.newInstance(containerMgrClass, conf);
    containerManager.setContainersInfo(containersInfo);
  }

  @Override
  public StartContainersResponse startContainers(StartContainersRequest request)
      throws YarnException, IOException {
    ContainersResponse startContainersResp = containerManager
        .startContainers(request.getStartContainerRequests());
    return StartContainersResponse.newInstance(
        startContainersResp.getServicesMetaData(),
        startContainersResp.getSucceededContainers(),
        startContainersResp.getFailedContainers());
  }

  @Override
  public StopContainersResponse stopContainers(StopContainersRequest request)
      throws YarnException, IOException {
    ContainersResponse containersResponse = containerManager
        .stopContainers(request.getContainerIds());
    return StopContainersResponse.newInstance(
        containersResponse.getSucceededContainers(),
        containersResponse.getFailedContainers());
  }

  @Override
  public GetContainerStatusesResponse getContainerStatuses(
      GetContainerStatusesRequest request) throws YarnException, IOException {
    ContainersStatusResponse containerStatuses = containerManager
        .getContainerStatuses(request.getContainerIds());
    return GetContainerStatusesResponse.newInstance(
        containerStatuses.getStatuses(), containerStatuses.getFailedRequests());
  }

  @Override
  public void close() throws IOException {
    
  }
}
