// Copyright (c) 2018 DDN. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package org.apache.hadoop.yarn.hpc.impl;

import java.io.Closeable;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.yarn.api.ApplicationMasterProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.hpc.api.HPCApplicationMaster;
import org.apache.hadoop.yarn.hpc.conf.HPCConfiguration;
import org.apache.hadoop.yarn.hpc.records.AllocatedContainersInfo;
import org.apache.hadoop.yarn.hpc.records.ApplicationMasterFinishRequest;
import org.apache.hadoop.yarn.hpc.records.ApplicationMasterFinishResponse;
import org.apache.hadoop.yarn.hpc.records.ApplicationMasterRegisterRequest;
import org.apache.hadoop.yarn.hpc.records.ApplicationMasterRegisterResponse;
import org.apache.hadoop.yarn.hpc.records.HPCAllocateRequest;
import org.apache.hadoop.yarn.hpc.records.HPCAllocateResponse;
import org.apache.hadoop.yarn.util.Records;

public class HPCApplicationMasterProtocolImpl implements
    ApplicationMasterProtocol, Closeable {
  private HPCApplicationMaster applicationMaster;

  public HPCApplicationMasterProtocolImpl(Configuration conf,
      AllocatedContainersInfo containersInfo) { 
    Class<? extends HPCApplicationMaster> appMasterClass = conf.getClass(
        HPCConfiguration.YARN_APPLICATION_HPC_APPLICATIONMASTER_CLASS, 
        HPCConfiguration.DEFAULT_YARN_APPLICATION_HPC_APPLICATIONMASTER_CLASS,
        HPCApplicationMaster.class);
    applicationMaster = ReflectionUtils.newInstance(appMasterClass, conf);
    applicationMaster.setContainersInfo(containersInfo);
  }

  @Override
  public RegisterApplicationMasterResponse registerApplicationMaster(
      RegisterApplicationMasterRequest request) throws YarnException,
      IOException {
    ApplicationMasterRegisterRequest registerReq = new ApplicationMasterRegisterRequest(
        request.getHost(), request.getRpcPort(), request.getTrackingUrl());
    ApplicationMasterRegisterResponse registerResp = applicationMaster
        .registerApplicationMaster(registerReq);
    RegisterApplicationMasterResponse response = Records
        .newRecord(RegisterApplicationMasterResponse.class);
    response.setMaximumResourceCapability(registerResp.getMaxCapability());
    response.setQueue(registerResp.getQueue());
    return response;
  }

  @Override
  public FinishApplicationMasterResponse finishApplicationMaster(
      FinishApplicationMasterRequest request) throws YarnException, IOException {
    ApplicationMasterFinishRequest finishRequest = new ApplicationMasterFinishRequest(
        request.getFinalApplicationStatus(), request.getDiagnostics(),
        request.getTrackingUrl());
    ApplicationMasterFinishResponse response = applicationMaster
        .finishApplicationMaster(finishRequest);
    return FinishApplicationMasterResponse.newInstance(response
        .isUnregistered());
  }

  @Override
  public AllocateResponse allocate(AllocateRequest request)
      throws YarnException, IOException {
    HPCAllocateRequest allocateRequest = new HPCAllocateRequest();
    allocateRequest.setAppProgress(request.getProgress());
    allocateRequest.setContainersToBeReleased(request.getReleaseList());
    allocateRequest.setResourceAsk(request.getAskList());
    allocateRequest.setResourceBlacklistRequest(request
        .getResourceBlacklistRequest());
    allocateRequest.setResponseID(request.getResponseId());
    HPCAllocateResponse hpcAllocateResponse = applicationMaster
        .allocate(allocateRequest);

    AllocateResponse response = Records.newRecord(AllocateResponse.class);
    response.setAllocatedContainers(hpcAllocateResponse
        .getAllocatedContainers());
    response.setNMTokens(hpcAllocateResponse.getNmTokens());
    response.setCompletedContainersStatuses(hpcAllocateResponse.getCompletedContainers());
    return response;
  }

  @Override
  public void close() throws IOException {

  }
}
