// Copyright (c) 2017 DDN. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package org.apache.hadoop.yarn.hpc.slurm;

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
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.SerializedException;
import org.apache.hadoop.yarn.api.records.Token;
import org.apache.hadoop.yarn.hpc.api.HPCContainerManager;
import org.apache.hadoop.yarn.hpc.conf.HPCConfiguration;
import org.apache.hadoop.yarn.hpc.records.AllocatedContainersInfo;
import org.apache.hadoop.yarn.hpc.records.ContainersResponse;
import org.apache.hadoop.yarn.hpc.records.ContainersStatusResponse;
import org.apache.hadoop.yarn.hpc.util.HPCCommandExecutor;
import org.apache.hadoop.yarn.util.ConverterUtils;

public class SlurmContainerManager implements HPCContainerManager, Configurable {

  private static final Log LOG = LogFactory.getLog(SlurmContainerManager.class);
  private Configuration conf;
  private AllocatedContainersInfo containersInfo;

  @Override
  public ContainersResponse startContainers(List<StartContainerRequest> requests) throws IOException {

    List<ContainerId> succeededContainers = new ArrayList<ContainerId>();
    Map<ContainerId,SerializedException> failedRequests = 
      new HashMap<ContainerId, SerializedException>();
    
    for (StartContainerRequest startContainerRequest : requests) {
      ContainerLaunchContext containerLaunchContext = startContainerRequest
          .getContainerLaunchContext();
      Token containerToken = startContainerRequest.getContainerToken();
      String containerIdStr = new String(containerToken.getIdentifier().array());
      LOG.info("Starting container " + containerIdStr);
      try {
        ContainerId containerId = HPCCommandExecutor.launchContainer(
            containerLaunchContext, containerIdStr, null, conf,
            containersInfo.get(ConverterUtils.toContainerId(containerIdStr)),
            null);
        succeededContainers.add(containerId);
      } catch(Throwable t) {
        LOG.error("Failed to launch container : " + containerIdStr, t);
        ContainerId containerId = ConverterUtils.toContainerId(containerIdStr);
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
  public ContainersResponse stopContainers(List<ContainerId> containerIds) throws IOException {
        
    LOG.info("Stopping containers: " + containerIds);
    Map<ContainerId, SerializedException> failedRequests = new HashMap<ContainerId, SerializedException>();
    String scancelCmd = conf.get(
        HPCConfiguration.YARN_APPLICATION_HPC_COMMAND_SLURM_SCANCEL,
        HPCConfiguration.DEFAULT_YARN_APPLICATION_HPC_COMMAND_SLURM_SCANCEL);
    int i = 0;
    String[] cmd = new String[containerIds.size() + 3];    
    cmd[i++] = scancelCmd;
    cmd[i++] = "-b";
    cmd[i++] = "--signal=CONT";
    for (ContainerId containerId : containerIds) {
      int jobId = containersInfo.get(containerId);
      cmd[i++] = String.valueOf(jobId);
    }
    try {
      Shell.execCommand(cmd);
    } catch (IOException e) {
      LOG.info("Failed to stop containers because might be completed already. "
          + e.getMessage());
    }        
    ContainersResponse response = new ContainersResponse();
    response.setSucceededContainers(containerIds);
    response.setFailedContainers(failedRequests);
    return response;
  }

  @Override
  public ContainersStatusResponse getContainerStatuses(
      List<ContainerId> containerIds) throws IOException {
    LOG.info("Requesting status for containers : " + containerIds);
    List<ContainerStatus> statuses = new ArrayList<ContainerStatus>();
    Map<ContainerId, SerializedException> failedRequests = 
        new HashMap<ContainerId, SerializedException>();
    ContainersStatusResponse response = new ContainersStatusResponse();

    // get the container id job ids
    ApplicationAttemptId applicationAttemptId = containerIds.get(0)
        .getApplicationAttemptId();
    List<Integer> intContIds = new ArrayList<Integer>();
    Map<Integer, ContainerId> taksIdsVsContainerIds = new HashMap<Integer, ContainerId>();
    for (ContainerId contId : containerIds) {
      Integer taskId = containersInfo.get(contId);
      if (taskId != null) {
        intContIds.add(taskId);
        taksIdsVsContainerIds.put(taskId, contId);
      } else {
        ContainerStatus status = ContainerStatus.newInstance(contId,
            ContainerState.COMPLETE, "", 0);
        statuses.add(status);
      }
    }
    List<Integer> activeContIntIds = new ArrayList<Integer>();

    // check the container job statuses
    int appJobId = applicationAttemptId.getApplicationId().getId();
    String squeueCmd = conf.get(
        HPCConfiguration.YARN_APPLICATION_HPC_COMMAND_SLURM_SQUEUE,
        HPCConfiguration.DEFAULT_YARN_APPLICATION_HPC_COMMAND_SLURM_SQUEUE);
    // Output of squeue command will be :
    // YarnTask-{$APP_ID}:{TASK_ID}:{STATE=PD/R}:{COMMENT=[priority]:[cpus]:[memory]}:{HOSTNAME}
    // YarnTask-{$APP_ID}:{TASK_ID}:{STATE=PD/R}:
    Pattern pattern = Pattern.compile("YarnTask-" + appJobId
        + ":(\\d+):([A-Z]+):");
    for (String line : Shell.execCommand(squeueCmd, "-h", "-o %j:%i:%t:%k:%N")
        .split("\\n")) {
      Matcher matcher = pattern.matcher(line);
      if (matcher.find()) {
        int cid = Integer.parseInt(matcher.group(1));
        if (intContIds.contains(cid)) {
          ContainerState state;
          if (matcher.group(2).equals("R")) {
            state = ContainerState.RUNNING;
          } else {
            state = ContainerState.NEW;
          }
          ContainerStatus status = ContainerStatus.newInstance(
              taksIdsVsContainerIds.get(cid), state, "", -1);
          statuses.add(status);
          activeContIntIds.add(cid);
        }
      }
    }

    // check for completed containers
    for (Integer id : intContIds) {
      if (activeContIntIds.contains(id) == false) {
        ContainerStatus status = ContainerStatus.newInstance(taksIdsVsContainerIds.get(id),
            ContainerState.COMPLETE, "", 0);
        statuses.add(status);
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
