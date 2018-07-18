// Copyright (c) 2018 DDN. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package org.apache.hadoop.yarn.hpc.pbs;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerRequest;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.SerializedException;
import org.apache.hadoop.yarn.api.records.Token;
import org.apache.hadoop.yarn.hpc.api.HPCContainerManager;
import org.apache.hadoop.yarn.hpc.conf.HPCConfiguration;
import org.apache.hadoop.yarn.hpc.pbs.util.ContainerResponse;
import org.apache.hadoop.yarn.hpc.pbs.util.ContainerResponses;
import org.apache.hadoop.yarn.hpc.pbs.util.LaunchedPBSJobs;
import org.apache.hadoop.yarn.hpc.pbs.util.PBSCommandExecutor;
import org.apache.hadoop.yarn.hpc.records.AllocatedContainersInfo;
import org.apache.hadoop.yarn.hpc.records.ContainersResponse;
import org.apache.hadoop.yarn.hpc.records.ContainersStatusResponse;
import org.apache.hadoop.yarn.util.ConverterUtils;

public class PBSContainerManager implements HPCContainerManager, Configurable {

  private static final Log LOG = LogFactory.getLog(PBSContainerManager.class);
  private Configuration conf;
  private AllocatedContainersInfo containersInfo;

  @Override
  public ContainersResponse startContainers(List<StartContainerRequest> requests)
      throws IOException {

    List<ContainerId> succeededContainers = new ArrayList<ContainerId>();
    Map<ContainerId, SerializedException> failedRequests =
        new HashMap<ContainerId, SerializedException>();

    for (StartContainerRequest startContainerRequest : requests) {
      ContainerLaunchContext containerLaunchContext = startContainerRequest
          .getContainerLaunchContext();
      Token containerToken = startContainerRequest.getContainerToken();
      String containerIdStr = new String(containerToken.getIdentifier().array());
      ContainerId containerId = ConverterUtils.toContainerId(containerIdStr);

      int pbsJobId = containersInfo.get(containerId);

      try {
        LOG.info("Starting container : " + containerIdStr
            + " for the PBS Job id : " + pbsJobId);
        // Launch Containers
        String containerHostName = ContainerResponses.getResponse(pbsJobId)
            .getContainerHostName();
        PBSCommandExecutor.launchContainer(containerLaunchContext,
            containerIdStr, null, conf, pbsJobId, false, containerHostName);
        succeededContainers.add(containerId);
        LaunchedPBSJobs.addRunningContainer(containerId, pbsJobId);
      } catch (Throwable t) {
        LOG.error("Failed to launch container : " + containerIdStr, t);
        SerializedException exception = SerializedException.newInstance(t);
        failedRequests.put(containerId, exception);
      }
    }

    Map<String, ByteBuffer> allServicesMetaData = new HashMap<String, ByteBuffer>();
    ByteBuffer buffer = ByteBuffer.allocate(200);
    buffer.putInt(100);
    allServicesMetaData.put("mapreduce_shuffle", buffer);

    ContainersResponse response = new ContainersResponse();
    response.setServicesMetaData(allServicesMetaData);
    response.setFailedContainers(failedRequests);
    response.setSucceededContainers(succeededContainers);
    return response;
  }

  @Override
  public ContainersResponse stopContainers(List<ContainerId> containerIds)
      throws IOException {

    LOG.info("Stopping containers: " + containerIds);
    List<ContainerId> succeededContainerIds = new ArrayList<ContainerId>();
    Map<ContainerId, SerializedException> failedRequests =
        new HashMap<ContainerId, SerializedException>();
    String qdelCmd = conf.get(
        HPCConfiguration.YARN_APPLICATION_HPC_COMMAND_PBS_QDEL,
        HPCConfiguration.DEFAULT_YARN_APPLICATION_HPC_COMMAND_PBS_QDEL);
    for (ContainerId containerId : containerIds) {
      int pbsJobId = containersInfo.get(containerId);
      try {
        Shell.execCommand(qdelCmd, "-W", "force", String.valueOf(pbsJobId));
        succeededContainerIds.add(containerId);
        containersInfo.remove(containerId);
        ContainerResponse containerResponse = ContainerResponses
            .getResponse(pbsJobId);
        if (containerResponse != null) {
          ContainerResponses.remove(pbsJobId);
          containerResponse.close();
        }
      } catch (Throwable e) {
        LOG.info("Failed to stop container : " + containerId + ", Reason : "
            + e.getMessage());
//        failedRequests.put(containerId,
//            SerializedException.newInstance(new Exception(e.getMessage())));
      }
    }
    ContainersResponse response = new ContainersResponse();
    response.setSucceededContainers(succeededContainerIds);
    response.setFailedContainers(failedRequests);
    return response;
  }

  @SuppressWarnings("unused")
  @Override
  public ContainersStatusResponse getContainerStatuses(
      List<ContainerId> containerIds) throws IOException {
    LOG.info("Requesting status for containers : " + containerIds);
    List<ContainerStatus> statuses = new ArrayList<ContainerStatus>();
    Map<ContainerId, SerializedException> failedRequests =
        new HashMap<ContainerId, SerializedException>();
    ContainersStatusResponse response = new ContainersStatusResponse();

    List<Integer> intContIds = new ArrayList<Integer>();
    Map<Integer, ContainerId> taksIdsVsContainerIds =
        new HashMap<Integer, ContainerId>();
    String pbsJobIds = "";
    for (ContainerId contId : containerIds) {
      Integer taskId = containersInfo.get(contId);
      if (taskId != null) {
        intContIds.add(taskId);
        taksIdsVsContainerIds.put(taskId, contId);
        pbsJobIds += taskId;
      } else {
        ContainerStatus status = ContainerStatus.newInstance(contId,
            ContainerState.COMPLETE, "", 0);
        statuses.add(status);
      }
    }
    String qstatCmd = conf.get(
        HPCConfiguration.YARN_APPLICATION_HPC_COMMAND_PBS_QSTAT,
        HPCConfiguration.DEFAULT_YARN_APPLICATION_HPC_COMMAND_PBS_QSTAT);
    String result = Shell.execCommand(qstatCmd, pbsJobIds);
    
    String[] resultLines = result.split("\\n");
    if (resultLines != null && resultLines.length > 0) {
      for (String resultLine : resultLines) {
        if (resultLine.startsWith("Job id")
            || resultLine.startsWith("----------------")) {
          continue;
        } else if (resultLine.startsWith("qstat: Unknown Job Id")) {
          // If the Job is unknown then consider it as completed
          Matcher matcher = Pattern.compile("(\\d+)").matcher(resultLine);
          if (!matcher.find()) {
            // Ignore the line
            continue;
          }
          int jobId = Integer.parseInt(matcher.group(1));
          ContainerId containerId = taksIdsVsContainerIds.get(jobId);
          ContainerStatus status = ContainerStatus.newInstance(containerId,
              ContainerState.COMPLETE, "", 0);
          statuses.add(status);
        } else {
          String[] lineParts = resultLine.trim().split("\\s+");
          Matcher matcher = Pattern.compile("(\\d+)").matcher(lineParts[0]);
          if (!matcher.find()) {
            LOG.info("Could not get Job Id from " + result);
            continue;
          }
          int jobId = Integer.parseInt(matcher.group(1));
          String state = lineParts[4];
          ContainerState containerState;
          if ("R".equals(state) || "T".equals(state) || "W".equals(state)
              || "S".equals(state)) {
            containerState = ContainerState.RUNNING;
          } else if ("E".equals(state)) {
            containerState = ContainerState.COMPLETE;
          } else {
            containerState = ContainerState.NEW;
          }
          ContainerStatus status = ContainerStatus.newInstance(
              taksIdsVsContainerIds.get(jobId), containerState, "", 0);
          statuses.add(status);
        }
      }
    }
    response.setStatuses(statuses);
    response.setFailedRequests(failedRequests);
    return response;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setContainersInfo(AllocatedContainersInfo containersInfo) {
    this.containersInfo = containersInfo;
  }
}
