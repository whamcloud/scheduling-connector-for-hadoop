// Copyright (c) 2018 DDN. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package org.apache.hadoop.yarn.hpc.pbs;

import static org.apache.hadoop.yarn.hpc.conf.HPCConfiguration.DEFAULT_YARN_APPLICATION_HPC_PBS_JOB_STATUS_FILES_LOCATION;
import static org.apache.hadoop.yarn.hpc.conf.HPCConfiguration.DEFAULT_YARN_APPLICATION_HPC_SCHEDULER_MAXIMUM_ALLOCATION_MB;
import static org.apache.hadoop.yarn.hpc.conf.HPCConfiguration.DEFAULT_YARN_APPLICATION_HPC_SCHEDULER_MAXIMUM_ALLOCATION_VCORES;
import static org.apache.hadoop.yarn.hpc.conf.HPCConfiguration.YARN_APPLICATION_HPC_PBS_JOB_STATUS_FILES_LOCATION;
import static org.apache.hadoop.yarn.hpc.conf.HPCConfiguration.YARN_APPLICATION_HPC_SCHEDULER_MAXIMUM_ALLOCATION_MB;
import static org.apache.hadoop.yarn.hpc.conf.HPCConfiguration.YARN_APPLICATION_HPC_SCHEDULER_MAXIMUM_ALLOCATION_VCORES;

import java.io.IOException;
import java.net.URLEncoder;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NMToken;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.Token;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.hpc.api.HPCApplicationMaster;
import org.apache.hadoop.yarn.hpc.conf.HPCConfiguration;
import org.apache.hadoop.yarn.hpc.pbs.util.ContainerResponse;
import org.apache.hadoop.yarn.hpc.pbs.util.ContainerResponses;
import org.apache.hadoop.yarn.hpc.pbs.util.LaunchedPBSJobs;
import org.apache.hadoop.yarn.hpc.pbs.util.PBSCommandExecutor;
import org.apache.hadoop.yarn.hpc.pbs.util.SocketWrapperForMultipleJobs;
import org.apache.hadoop.yarn.hpc.records.AllocatedContainersInfo;
import org.apache.hadoop.yarn.hpc.records.ApplicationMasterFinishRequest;
import org.apache.hadoop.yarn.hpc.records.ApplicationMasterFinishResponse;
import org.apache.hadoop.yarn.hpc.records.ApplicationMasterRegisterRequest;
import org.apache.hadoop.yarn.hpc.records.ApplicationMasterRegisterResponse;
import org.apache.hadoop.yarn.hpc.records.HPCAllocateRequest;
import org.apache.hadoop.yarn.hpc.records.HPCAllocateResponse;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

public class PBSApplicationMaster implements HPCApplicationMaster, Configurable {

  private static final Log LOG = LogFactory.getLog(PBSApplicationMaster.class);

  private Configuration conf;

  private ContainerId appContainerId = ConverterUtils.toContainerId(System
      .getenv("CONTAINER_ID"));
  private ApplicationAttemptId appAttemptId = appContainerId
      .getApplicationAttemptId();

  private AllocatedContainersInfo containersInfo;
  private int containerId = 1;
  
  private SocketWrapperForMultipleJobs socketWrapper;

  public PBSApplicationMaster() {
    socketWrapper = new SocketWrapperForMultipleJobs();
    try {
      socketWrapper.initialize();
    } catch (IOException e) {
      throw new YarnRuntimeException(e);
    }
  }
  
  @Override
  public ApplicationMasterRegisterResponse registerApplicationMaster(
      ApplicationMasterRegisterRequest request) throws IOException {
    String amHost = request.getHost();
    int amRpcPort = request.getPort();
    String trackingUrl = request.getTrackingUrl();
    
    int jobid = appAttemptId.getApplicationId().getId();

    String jobStatusFileName = jobid + "__" + amRpcPort + "__" + amHost + "__"
        + URLEncoder.encode(trackingUrl, HPCConfiguration.CHAR_ENCODING);
    String jobStatusLocation = conf.get(
        YARN_APPLICATION_HPC_PBS_JOB_STATUS_FILES_LOCATION,
        DEFAULT_YARN_APPLICATION_HPC_PBS_JOB_STATUS_FILES_LOCATION);
    FileSystem fileSystem = FileSystem.get(conf);
    Path statusFile = new Path(jobStatusLocation, jobStatusFileName);
    fileSystem.createNewFile(statusFile);
    fileSystem.deleteOnExit(statusFile);

    ApplicationMasterRegisterResponse response = new ApplicationMasterRegisterResponse();
    response.setMaxCapability(getMaxCapability());
    response.setQueue("default");
    return response;
  }

  @Override
  public synchronized HPCAllocateResponse allocate(HPCAllocateRequest request)
      throws IOException {
    HPCAllocateResponse response = new HPCAllocateResponse();
    List<Container> allocatedContainers = new ArrayList<>();
    List<NMToken> nmTokens = new ArrayList<NMToken>();
    
    //Container Requests
    List<ResourceRequest> resourceAsk = request.getResourceAsk();
    int numOfRequestedContainers = 0;
    Map<Priority, ResourceRequest> containersReqs = new HashMap<Priority, ResourceRequest>();
    for (ResourceRequest resourceRequest : resourceAsk) {
      Priority priority = resourceRequest.getPriority();
      if (containersReqs.containsKey(priority) == false) {
        int numContainers = resourceRequest.getNumContainers();
        numOfRequestedContainers += numContainers;
        containersReqs.put(priority, resourceRequest);
        LOG.debug("Resource Request: " + resourceRequest);
      }
    }
    
    // If the PBS Job already present for same priority then remove the request
    String qstatCmd = conf.get(
        HPCConfiguration.YARN_APPLICATION_HPC_COMMAND_PBS_QSTAT,
        HPCConfiguration.DEFAULT_YARN_APPLICATION_HPC_COMMAND_PBS_QSTAT);
    String result = Shell.execCommand(qstatCmd);
    String[] resultLines = result.split("\\n");
    if (resultLines.length > 2) {
      for (int i = 2; i < resultLines.length; i++) {
        String[] lineParts = resultLines[i].trim().split("\\s+");
        Matcher matcher = Pattern.compile("(\\d+)").matcher(lineParts[0]);
        if (!matcher.find() || lineParts.length < 5) {
          LOG.info("Could not get Job Id from " + lineParts);
          continue;
        }
        int jobId = Integer.parseInt(matcher.group(1));
        if ("Q".equals(lineParts[4]) && LaunchedPBSJobs.contains(jobId)) {
          Integer priority = LaunchedPBSJobs.getPriority(jobId);
          containersReqs.remove(Priority.newInstance(priority));
        }
      }
    }
    
    // Receive Allocated Containers
    Collection<ContainerResponse> responses = ContainerResponses.getResponses();
    for (ContainerResponse containerResponse : responses) {
      if (containerResponse.isAssigned() == false) {
        Integer jobId = containerResponse.getPbsJobId();
        containerResponse.setAssigned(true);
        Container container = newContainerInstance(jobId,
            Priority.newInstance(containerResponse.getPriority()),
            Resource.newInstance(containerResponse.getMemory(),
                containerResponse.getCores()),
            containerResponse.getContainerHostName());
        allocatedContainers.add(container);
        containersInfo.put(container.getId(), jobId);
        nmTokens.add(NMToken.newInstance(container.getNodeId(),
            container.getContainerToken()));

        // Remove one container request
        Set<Priority> keySet = containersReqs.keySet();
        Priority deleElem = null;
        for (Priority priority : keySet) {
          deleElem = priority;
          break;
        }
        containersReqs.remove(deleElem);
        LOG.info("Assigned container " + container.getId() + " for PBS Job :"
            + jobId);
      }
    }
    
    if (numOfRequestedContainers > 0) {
      for (Entry<Priority, ResourceRequest> entry : containersReqs.entrySet()) {
        int noOfConts = entry.getValue().getNumContainers();
        int priority = entry.getKey().getPriority();
        int memory = entry.getValue().getCapability().getMemory();
        int cores = entry.getValue().getCapability().getVirtualCores();
        for (int i = 0; i < noOfConts; i++) {
          int jobId = PBSCommandExecutor.submitAndGetPBSJobId(getConf(),
              priority, memory, cores, socketWrapper.getHostName(), socketWrapper.getPort());
          LOG.info("Created a Job with Id : " + jobId);
          LOG.debug("Created a Job with Id : " + jobId + " with priority="
              + priority + ", memory=" + memory + ", cores=" + cores);
          ContainerResponses.createdPbsJobIds.add(jobId);
          LaunchedPBSJobs.put(jobId, priority);
        }
      }
    }
    
    response.setAllocatedContainers(allocatedContainers);
    response.setNmTokens(nmTokens);

    // Release containers
    List<ContainerId> containersToBeReleased = request.getContainersToBeReleased();
    if (containersToBeReleased.size() > 0) {
      LOG.info("Containers to be released : "
          + containersToBeReleased);
      for (ContainerId containerId : containersToBeReleased) {
        if (containersInfo.contains(containerId)) {
          int pbsId = containersInfo.get(containerId);
          containersInfo.remove(containerId);
          ContainerResponses.createdPbsJobIds.remove(pbsId);
          ContainerResponse containerResponse = ContainerResponses
              .getResponse(pbsId);
          if (containerResponse != null) {
            ContainerResponses.remove(pbsId);
            containerResponse.writeShutDownCommand();
            containerResponse.close();
          }
          //Issue the kill command
          String[] cmd = new String[4];
          String qdelCmd = conf.get(
              HPCConfiguration.YARN_APPLICATION_HPC_COMMAND_PBS_QDEL,
              HPCConfiguration.DEFAULT_YARN_APPLICATION_HPC_COMMAND_PBS_QDEL);
          cmd[0] = qdelCmd;
          cmd[1] = "-W";
          cmd[2] = "force";
          cmd[3] = String.valueOf(pbsId);
          try{
            Shell.execCommand(null, cmd, 0L);
          }catch(Throwable e){
            // Ignore exception while deleting pbs jobs during shutdown
            LOG.info("Error while killing PBS Job : " + pbsId + ". "
                + e.getMessage());
          }
        }
      }
    }
    
    //Check for completed containers
    List<ContainerStatus> completedContainers = new ArrayList<ContainerStatus>();
    Map<ContainerId, Integer> runningContainers = LaunchedPBSJobs.getRunningContainers();
    for (Entry<ContainerId, Integer> runningContainer : runningContainers
        .entrySet()) {
      String compContResult;
      try {
        compContResult = Shell.execCommand(qstatCmd, runningContainer.getValue()
            .toString());
      } catch (Exception e) {
        ContainerStatus status = ContainerStatus.newInstance(
            runningContainer.getKey(), ContainerState.COMPLETE, "", 0);
        completedContainers.add(status);
        continue;
      }
      String[] compContResultLines = compContResult.split("\\n");
      if (resultLines != null && compContResultLines.length > 0) {
        for (String resultLine : compContResultLines) {
          if (resultLine.startsWith("Job id")
              || resultLine.startsWith("----------------")) {
            continue;
          } else if (resultLine.startsWith("qstat: Unknown Job Id")) {
            ContainerStatus status = ContainerStatus.newInstance(
                runningContainer.getKey(), ContainerState.COMPLETE, "", 0);
            completedContainers.add(status);
          } else {
            String[] lineParts = resultLine.trim().split("\\s+");
            Matcher matcher = Pattern.compile("(\\d+)").matcher(lineParts[0]);
            if (!matcher.find()) {
              //LOG.info("Could not get Job Id from " + result);
              continue;
            }
            String state = lineParts[4];
            if ("E".equals(state)) {
              ContainerStatus status = ContainerStatus.newInstance(
                  runningContainer.getKey(), ContainerState.COMPLETE, "", 0);
              completedContainers.add(status);
            }
          }
        }
      }
    }
    if (completedContainers.size() > 0) {
      LOG.info("Completed Containers : " + completedContainers);
      for (ContainerStatus containerStatus : completedContainers) {
        LaunchedPBSJobs
            .removeRunningContainer(containerStatus.getContainerId());
      }
    }
    response.setCompletedContainers(completedContainers);
    return response;
  }
  
  private Container newContainerInstance(int id, Priority priority,
      Resource capability, String hostName) throws IOException {
    NodeId nodeId = NodeId.newInstance(hostName, 0);
    Container container = Records.newRecord(Container.class);
    container.setNodeId(nodeId);
    container.setPriority(priority);
    container.setResource(capability);
    container.setId(ContainerId.newContainerId(appAttemptId, ++containerId));
    Token token = Token.newInstance(nodeId.toString().getBytes(),
        nodeId.toString(), nodeId.toString().getBytes(), nodeId.toString());
    byte[] bytes = container.getId().toString().getBytes();
    ByteBuffer buffer = ByteBuffer.allocate(bytes.length);
    buffer.put(bytes);
    token.setIdentifier(buffer);
    container.setContainerToken(token);
    container.setNodeHttpAddress(hostName + ":0");
    return container;
  }

  @Override
  public ApplicationMasterFinishResponse finishApplicationMaster(
      ApplicationMasterFinishRequest request) throws IOException {
    //Close all the PBS Jobs
    Collection<ContainerResponse> responses = ContainerResponses.getResponses();
    for (ContainerResponse response : responses) {
//      response.writeShutDownCommand();
      response.close();
    }
    socketWrapper.close();
    
    //Kill all the remaining PBS Jobs
    if(ContainerResponses.createdPbsJobIds.size() > 0){
      String qdelCmd = conf.get(
          HPCConfiguration.YARN_APPLICATION_HPC_COMMAND_PBS_QDEL,
          HPCConfiguration.DEFAULT_YARN_APPLICATION_HPC_COMMAND_PBS_QDEL);
      LOG.info("Killing PBS Jobs " + ContainerResponses.createdPbsJobIds + " during shutdown");
      String[] cmd = new String[ContainerResponses.createdPbsJobIds.size() + 3];
      int i = 0;
      cmd[i++] = qdelCmd;
      cmd[i++] = "-W";
      cmd[i++] = "force";
      for (Integer jobId : ContainerResponses.createdPbsJobIds) {
        cmd[i++] = String.valueOf(jobId);
      }
      
      try{
        Shell.execCommand(null, cmd, 0L);
      }catch(Throwable e){
        // Ignore exception while deleting pbs jobs during shutdown
        LOG.info("Error while killing PBS Jobs. " + e.getMessage());
      }
    }
    return new ApplicationMasterFinishResponse(true);
  }

  private Resource getMaxCapability() {
    int maxAllocMemory = conf.getInt(
        YARN_APPLICATION_HPC_SCHEDULER_MAXIMUM_ALLOCATION_MB,
        DEFAULT_YARN_APPLICATION_HPC_SCHEDULER_MAXIMUM_ALLOCATION_MB);
    int maxAllocCPUs = conf.getInt(
        YARN_APPLICATION_HPC_SCHEDULER_MAXIMUM_ALLOCATION_VCORES,
        DEFAULT_YARN_APPLICATION_HPC_SCHEDULER_MAXIMUM_ALLOCATION_VCORES);
    return Resource.newInstance(maxAllocMemory, maxAllocCPUs);
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
