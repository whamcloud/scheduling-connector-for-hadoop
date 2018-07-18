// Copyright (c) 2018 DDN. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package org.apache.hadoop.yarn.hpc.impl;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.CancelDelegationTokenRequest;
import org.apache.hadoop.yarn.api.protocolrecords.CancelDelegationTokenResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationAttemptReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationAttemptReportResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationAttemptsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationAttemptsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterMetricsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterMetricsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodeLabelsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodeLabelsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerReportResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainersRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainersResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetDelegationTokenRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetDelegationTokenResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetLabelsToNodesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetLabelsToNodesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetNodesToLabelsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetNodesToLabelsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetQueueInfoRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetQueueInfoResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetQueueUserAclsInfoRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetQueueUserAclsInfoResponse;
import org.apache.hadoop.yarn.api.protocolrecords.KillApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.KillApplicationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.MoveApplicationAcrossQueuesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.MoveApplicationAcrossQueuesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RenewDelegationTokenRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RenewDelegationTokenResponse;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationDeleteRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationDeleteResponse;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationSubmissionRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationSubmissionResponse;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationUpdateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationUpdateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationResponse;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.QueueState;
import org.apache.hadoop.yarn.api.records.QueueUserACLInfo;
import org.apache.hadoop.yarn.api.records.YarnClusterMetrics;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.hpc.api.HPCApplicationClient;
import org.apache.hadoop.yarn.hpc.conf.HPCConfiguration;
import org.apache.hadoop.yarn.hpc.records.NewApplicationResponse;

public class HPCApplicationClientProtocolImpl implements
    ApplicationClientProtocol, Closeable {

  private HPCApplicationClient appClient;

  public HPCApplicationClientProtocolImpl(Configuration conf) {
    Class<? extends HPCApplicationClient> appClientClass = conf.getClass(
        HPCConfiguration.YARN_APPLICATION_HPC_CLIENT_CLASS,
        HPCConfiguration.DEFAULT_YARN_APPLICATION_HPC_CLIENT_CLASS,
        HPCApplicationClient.class);
    appClient = ReflectionUtils.newInstance(appClientClass, conf);
  }

  @Override
  public GetNewApplicationResponse getNewApplication(
      GetNewApplicationRequest request) throws YarnException, IOException {
    NewApplicationResponse newApplication = appClient.getNewApplication();
    return GetNewApplicationResponse.newInstance(
        newApplication.getApplicationId(), newApplication.getMinCapability(),
        newApplication.getMaxCapability());
  }

  @Override
  public SubmitApplicationResponse submitApplication(
      SubmitApplicationRequest request) throws YarnException, IOException {
    appClient.submitApplication(request.getApplicationSubmissionContext());
    return SubmitApplicationResponse.newInstance();
  }

  @Override
  public KillApplicationResponse forceKillApplication(
      KillApplicationRequest request) throws YarnException, IOException {
    appClient.forceKillApplication(request.getApplicationId());
    return KillApplicationResponse.newInstance(true);
  }

  @Override
  public GetApplicationReportResponse getApplicationReport(
      GetApplicationReportRequest request) throws YarnException, IOException {
    ApplicationReport applicationReport = appClient
        .getApplicationReport(request.getApplicationId());
    return GetApplicationReportResponse.newInstance(applicationReport);
  }

  @Override
  public GetClusterMetricsResponse getClusterMetrics(
      GetClusterMetricsRequest request) throws YarnException, IOException {
    YarnClusterMetrics clusterMetrics = appClient.getClusterMetrics();
    return GetClusterMetricsResponse.newInstance(clusterMetrics);
  }

  @Override
  public GetApplicationsResponse getApplications(GetApplicationsRequest request)
      throws YarnException, IOException {
    List<ApplicationReport> applications = appClient.getApplications();
    return GetApplicationsResponse.newInstance(applications);
  }

  @Override
  public GetClusterNodesResponse getClusterNodes(GetClusterNodesRequest request)
      throws YarnException, IOException {
    List<NodeReport> clusterNodes = appClient.getClusterNodes(request
        .getNodeStates());
    return GetClusterNodesResponse.newInstance(clusterNodes);
  }

  @Override
  public GetQueueInfoResponse getQueueInfo(GetQueueInfoRequest request)
      throws YarnException, IOException {
    QueueInfo queueInfo = QueueInfo.newInstance("default", 10240, 10240, 10240,
        null, null, QueueState.RUNNING, null, null);
    GetQueueInfoResponse queueInfoResponse = GetQueueInfoResponse
        .newInstance(queueInfo);
    return queueInfoResponse;
  }

  @Override
  public GetQueueUserAclsInfoResponse getQueueUserAcls(
      GetQueueUserAclsInfoRequest request) throws YarnException, IOException {
    return GetQueueUserAclsInfoResponse
        .newInstance(new ArrayList<QueueUserACLInfo>());
  }

  @Override
  public GetDelegationTokenResponse getDelegationToken(
      GetDelegationTokenRequest request) throws YarnException, IOException {
    throw new YarnException("Not Supported.");
  }

  @Override
  public RenewDelegationTokenResponse renewDelegationToken(
      RenewDelegationTokenRequest request) throws YarnException, IOException {
    throw new YarnException("Not Supported.");
  }

  @Override
  public CancelDelegationTokenResponse cancelDelegationToken(
      CancelDelegationTokenRequest request) throws YarnException, IOException {
    throw new YarnException("Not Supported.");
  }

  @Override
  public MoveApplicationAcrossQueuesResponse moveApplicationAcrossQueues(
      MoveApplicationAcrossQueuesRequest request) throws YarnException,
      IOException {
    return RecordFactoryProvider.getRecordFactory(null).newRecordInstance(
        MoveApplicationAcrossQueuesResponse.class);
  }

  @Override
  public GetApplicationAttemptReportResponse getApplicationAttemptReport(
      GetApplicationAttemptReportRequest arg0) throws YarnException,
      IOException {
    throw new YarnException("Not Supported.");
  }

  @Override
  public GetApplicationAttemptsResponse getApplicationAttempts(
      GetApplicationAttemptsRequest arg0) throws YarnException, IOException {
    throw new YarnException("Not Supported.");
  }

  @Override
  public GetContainerReportResponse getContainerReport(
      GetContainerReportRequest arg0) throws YarnException, IOException {
    throw new YarnException("Not Supported.");
  }

  @Override
  public GetContainersResponse getContainers(GetContainersRequest arg0)
      throws YarnException, IOException {
    throw new YarnException("Not Supported.");
  }

  @Override
  public void close() throws IOException {
  }

@Override
public ReservationDeleteResponse deleteReservation(ReservationDeleteRequest arg0)
		throws YarnException, IOException {
	// TODO Auto-generated method stub
	return null;
}

@Override
public GetClusterNodeLabelsResponse getClusterNodeLabels(
		GetClusterNodeLabelsRequest arg0) throws YarnException, IOException {
	// TODO Auto-generated method stub
	return null;
}

@Override
public GetLabelsToNodesResponse getLabelsToNodes(GetLabelsToNodesRequest arg0)
		throws YarnException, IOException {
	// TODO Auto-generated method stub
	return null;
}

@Override
public GetNodesToLabelsResponse getNodeToLabels(GetNodesToLabelsRequest arg0)
		throws YarnException, IOException {
	// TODO Auto-generated method stub
	return null;
}

@Override
public ReservationSubmissionResponse submitReservation(
		ReservationSubmissionRequest arg0) throws YarnException, IOException {
	// TODO Auto-generated method stub
	return null;
}

@Override
public ReservationUpdateResponse updateReservation(ReservationUpdateRequest arg0)
		throws YarnException, IOException {
	// TODO Auto-generated method stub
	return null;
}
}
