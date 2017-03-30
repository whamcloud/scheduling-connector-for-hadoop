// Copyright (c) 2017 Intel Corporation. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package org.apache.hadoop.yarn.hpc.slurm;

import static org.apache.hadoop.yarn.hpc.conf.HPCConfiguration.DEFAULT_YARN_APPLICATION_HPC_COMMAND_SLURM_SCANCEL;
import static org.apache.hadoop.yarn.hpc.conf.HPCConfiguration.DEFAULT_YARN_APPLICATION_HPC_COMMAND_SLURM_SQUEUE;
import static org.apache.hadoop.yarn.hpc.conf.HPCConfiguration.DEFAULT_YARN_APPLICATION_HPC_SCHEDULER_MAXIMUM_ALLOCATION_MB;
import static org.apache.hadoop.yarn.hpc.conf.HPCConfiguration.DEFAULT_YARN_APPLICATION_HPC_SCHEDULER_MAXIMUM_ALLOCATION_VCORES;
import static org.apache.hadoop.yarn.hpc.conf.HPCConfiguration.DEFAULT_YARN_APPLICATION_HPC_SLURM_FINISH_MAX_WAIT_MS;
import static org.apache.hadoop.yarn.hpc.conf.HPCConfiguration.DEFAULT_YARN_APPLICATION_HPC_SLURM_LOG_AGGREGATION_ENABLE;
import static org.apache.hadoop.yarn.hpc.conf.HPCConfiguration.YARN_APPLICATION_HPC_COMMAND_SLURM_SCANCEL;
import static org.apache.hadoop.yarn.hpc.conf.HPCConfiguration.YARN_APPLICATION_HPC_COMMAND_SLURM_SQUEUE;
import static org.apache.hadoop.yarn.hpc.conf.HPCConfiguration.YARN_APPLICATION_HPC_SCHEDULER_MAXIMUM_ALLOCATION_MB;
import static org.apache.hadoop.yarn.hpc.conf.HPCConfiguration.YARN_APPLICATION_HPC_SCHEDULER_MAXIMUM_ALLOCATION_VCORES;
import static org.apache.hadoop.yarn.hpc.conf.HPCConfiguration.YARN_APPLICATION_HPC_SLURM_FINISH_MAX_WAIT_MS;
import static org.apache.hadoop.yarn.hpc.conf.HPCConfiguration.YARN_APPLICATION_HPC_SLURM_LOG_AGGREGATION_ENABLE;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
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
import org.apache.hadoop.yarn.hpc.api.HPCApplicationMaster;
import org.apache.hadoop.yarn.hpc.records.AllocatedContainersInfo;
import org.apache.hadoop.yarn.hpc.records.ApplicationMasterFinishRequest;
import org.apache.hadoop.yarn.hpc.records.ApplicationMasterFinishResponse;
import org.apache.hadoop.yarn.hpc.records.ApplicationMasterRegisterRequest;
import org.apache.hadoop.yarn.hpc.records.ApplicationMasterRegisterResponse;
import org.apache.hadoop.yarn.hpc.records.HPCAllocateRequest;
import org.apache.hadoop.yarn.hpc.records.HPCAllocateResponse;
import org.apache.hadoop.yarn.hpc.util.HPCCommandExecutor;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

public class SlurmApplicationMaster implements HPCApplicationMaster, Configurable {

  private static final Log LOG = LogFactory.getLog(SlurmApplicationMaster.class);

  private Configuration conf;

  private ContainerId appContainerId = ConverterUtils.toContainerId(System
      .getenv("CONTAINER_ID"));
  private ApplicationAttemptId appAttemptId = appContainerId
      .getApplicationAttemptId();

  private AllocatedContainersInfo containersInfo;
  private int containerId = 1;
  private Map<Integer, ContainerId> taskIdVsContainerIds = 
      new ConcurrentHashMap<Integer, ContainerId>();

  @Override
  public ApplicationMasterRegisterResponse registerApplicationMaster(
      ApplicationMasterRegisterRequest request) throws IOException {
    String amHost = request.getHost();
    int amRpcPort = request.getPort();
    int jobid = appAttemptId.getApplicationId().getId();
    
    HPCCommandExecutor.setJobState(jobid, "running:" + amHost + ":" + amRpcPort, conf);

    ApplicationMasterRegisterResponse response = new ApplicationMasterRegisterResponse();
    response.setMaxCapability(getMaxCapability());
    response.setQueue("default");
    return response;
  }

  @Override
  public synchronized HPCAllocateResponse allocate(HPCAllocateRequest request)
      throws IOException {

    List<ResourceRequest> askList = request.getResourceAsk();
    List<Container> containers = new ArrayList<Container>();
    List<NMToken> nmTokens = new ArrayList<NMToken>();

    //Release allocated containers
    releaseContainers(request);

    // TODO: Fix this to -> update requests -> get new requests
    // Requests have to be grouped by priority and by hostnames.
    // We ignore hostnames for now, and just assign any available slot
    // (sub-optimal for HDFS). Multiple calls to this method may have requests
    // belonging to the same priority level. They should not be duplicated in
    // subsequent calls.
    int numOfRequestedContainers = 0;
    Map<Priority, ResourceRequest> containersReqs = new HashMap<Priority, ResourceRequest>();
    for (ResourceRequest resourceRequest : askList) {
      Priority priority = resourceRequest.getPriority();
      if (containersReqs.containsKey(priority) == false) {
        int numContainers = resourceRequest.getNumContainers();
        numOfRequestedContainers += numContainers;
        containersReqs.put(priority, resourceRequest);
        LOG.info("Resource Request: " + resourceRequest);
      }
    }
    int appJobId = appAttemptId.getApplicationId().getId();
    String squeueCmd = conf.get(YARN_APPLICATION_HPC_COMMAND_SLURM_SQUEUE,
        DEFAULT_YARN_APPLICATION_HPC_COMMAND_SLURM_SQUEUE);
    int numNotReadyJobs = 0;
    // Output of squeue command will be :
    // YarnTask-{$APP_ID}:{TASK_ID}:{STATE=PD/R}:{COMMENT=[priority]:[cpus]:[memory]}:{HOSTNAME}
    Pattern pattern = Pattern.compile("YarnTask-" + appJobId
        + ":(\\d+):([A-Z]+):(\\d+):(\\d+):(\\d+):([A-Za-z0-9\\-\\.]*)");
    for (String line : Shell.execCommand(squeueCmd, "-h", "-o %j:%i:%t:%k:%N").split("\\n")) {
      Matcher matcher = pattern.matcher(line);
      if (matcher.find()) {
        // if priority of the request matches with those of already slotted jobs,
        // no need to create new slots.
        Priority p = Priority.newInstance(Integer.parseInt(matcher.group(3)));
        containersReqs.remove(p);

        // if the job has moved from "PD" to "R" state, it can be assigned.
        if (matcher.group(2).equals("R")) {
          int vCores = Integer.parseInt(matcher.group(4));
          int memory = Integer.parseInt(matcher.group(5));
          String nodeName = matcher.group(6);
          Resource r = Resource.newInstance(memory, vCores);
          int cid = Integer.parseInt(matcher.group(1));
          Container container = newContainerInstance(cid, p, r, nodeName);
          containersInfo.put(container.getId(), cid);
          taskIdVsContainerIds.put(cid, container.getId());
          containers.add(container);
          nmTokens.add(NMToken.newInstance(container.getNodeId(),
              container.getContainerToken()));
        } else {
          numNotReadyJobs++;
        }
      }
    }
    LOG.info("Number of jobs in pending state: " + numNotReadyJobs);
    if (numOfRequestedContainers > 0) {
      StringBuffer buffer;
      for (Entry<Priority, ResourceRequest> entry : containersReqs.entrySet()) {
        buffer = new StringBuffer();
        Priority prior = entry.getKey();
        ResourceRequest rsReq = entry.getValue();
        Integer noOfCont = rsReq.getNumContainers();
        Resource cap = rsReq.getCapability();
        for (int i = 0; i < noOfCont; i++) {
          // MetaInfo: {Priority}:{Cpus}:{Memory}
          String meta = prior + ":" + cap.getVirtualCores() + ":"
              + cap.getMemory();
          int jobid = HPCCommandExecutor.createNewJob("YarnTask-" + appJobId,
              meta, conf, cap.getMemory(), cap.getVirtualCores());
          buffer.append(",");
          buffer.append(jobid);
          LOG.info("Created new job: " + jobid);
        }

        if (noOfCont > 0) {
          // List of job ids = 1,2,3,4,5...
          String jobids = buffer.toString().substring(1);

          // Collect newly created jobs if they are ready to run
          String[] runningjobs = Shell.execCommand(squeueCmd, "-h", "-o %i:%N",
              "--jobs=" + jobids, "--states=R").split("\\n");
          for (String line : runningjobs) {
            String[] idHost = line.split(":");
            if (idHost.length == 2 && StringUtils.isNotBlank(idHost[0])
                && StringUtils.isNotBlank(idHost[1])) {
              int cid = Integer.parseInt(idHost[0].trim());
              Container container = newContainerInstance(cid, prior, cap,
                  idHost[1].trim());
              containersInfo.put(container.getId(), cid);
              taskIdVsContainerIds.put(cid, container.getId());
              containers.add(container);
              nmTokens.add(NMToken.newInstance(container.getNodeId(),
                  container.getContainerToken()));
            }
          }
        }
      }
    }
    LOG.info("Containers requested: " + numOfRequestedContainers
        + ", assigned: " + containers.size());
    HPCAllocateResponse response = new HPCAllocateResponse();    
    response.setAllocatedContainers(containers);
    response.setNmTokens(nmTokens);
    response.setResponseId(1 + request.getResponseID());
    
    //Completed Containers
    List<ContainerStatus> completedContainers = new ArrayList<ContainerStatus>();
    // Output of squeue command will be :
    // YarnTask-{$APP_ID}:{TASK_ID}:{STATE=PD/R}:
    Pattern pattern1 = Pattern.compile("YarnTask-" + appJobId
        + ":(\\d+):([A-Z]+):");
    Set<Integer> taskIds = taskIdVsContainerIds.keySet();
    List<Integer> activeContIntIds = new ArrayList<Integer>();
    for (String line : Shell.execCommand(squeueCmd, "-h", "-o %j:%i:%t:%k:%N")
        .split("\\n")) {
      Matcher matcher = pattern1.matcher(line);
      if (matcher.find()) {
        int cid = Integer.parseInt(matcher.group(1));
          activeContIntIds.add(cid);
        }
      }

    // check for completed containers
    Set<Integer> newtaskIds = new HashSet<>(taskIds);
    for (Integer actTaskId : newtaskIds) {
      if (activeContIntIds.contains(actTaskId) == false) {
        completedContainers
            .add(ContainerStatus.newInstance(
                taskIdVsContainerIds.get(actTaskId), ContainerState.COMPLETE,
                "", 0));
        taskIdVsContainerIds.remove(actTaskId);
      }
    }
    response.setCompletedContainers(completedContainers);
    
    return response;
  }

  private void releaseContainers(HPCAllocateRequest request) {
    // Cancel jobs containing released containers:
    // send SIGCONT to parent task which is blocked on SIGSTOP
    List<ContainerId> releaseList = request.getContainersToBeReleased();
    LOG.info("Releasing containers: " + releaseList.size());
    if (releaseList.size() > 0) {
      String[] release = new String[releaseList.size() + 3];
      String scancelCmd = conf.get(YARN_APPLICATION_HPC_COMMAND_SLURM_SCANCEL,
          DEFAULT_YARN_APPLICATION_HPC_COMMAND_SLURM_SCANCEL);
      release[0] = scancelCmd;
      release[1] = "-b";
      release[2] = "--signal=CONT";
      int index = 2;
      for (ContainerId container : releaseList) {
        int jobId = containersInfo.get(container);
        release[++index] = String.valueOf(jobId);
      }
      try {
        Shell.execCommand(release);
      } catch (IOException e) {
        LOG.warn("Exception in scancel", e);
      }
    }
  }

  @SuppressWarnings("deprecation")
  private Container newContainerInstance(int id, Priority priority,
      Resource capability, String hostName) throws IOException {
    HPCCommandExecutor.setJobState(id, "assigned", conf);
    NodeId nodeId = NodeId.newInstance(hostName, 0);
    Container container = Records.newRecord(Container.class);
    container.setNodeId(nodeId);
    container.setPriority(priority);
    container.setResource(capability);
    container.setId(ContainerId.newInstance(appAttemptId, ++containerId));
    Token token = Token.newInstance(nodeId.toString().getBytes(),
        nodeId.toString(), nodeId.toString().getBytes(), nodeId.toString());
    byte[] bytes = container.getId().toString().getBytes();
    ByteBuffer buffer = ByteBuffer.allocate(bytes.length);
    buffer.put(bytes);
    token.setIdentifier(buffer);
    container.setContainerToken(token);
    container.setNodeHttpAddress(hostName + ":0");
    LOG.info("Allocated container " + container.getId() + " for Job id:" + id);
    return container;
  }

  @Override
  public ApplicationMasterFinishResponse finishApplicationMaster(
      ApplicationMasterFinishRequest request) throws IOException {
    int jobid = appAttemptId.getApplicationId().getId();
    HPCCommandExecutor.setJobState(jobid, "finished:localhost:0", conf);
    releaseAllJobTasksAsync(jobid);
    
    // There is no way of getting information on a finished job in slurm
    // unless accounting is enabled. This causes an exception to be thrown 
    // by the client when it tries to query for Application status.
    // Since the application has already exited, there is no status returned
    // and the client never finds out that the application actually completed.
    // So we wait for a few milliseconds to allow the status change to propagate
    // and then send a cancel request to the parent slurm job.
    finishJob(jobid);
    if (conf.getBoolean(YARN_APPLICATION_HPC_SLURM_LOG_AGGREGATION_ENABLE,
        DEFAULT_YARN_APPLICATION_HPC_SLURM_LOG_AGGREGATION_ENABLE)) {
      HPCCommandExecutor.startLogAggregation(appAttemptId, conf);
    }
    return new ApplicationMasterFinishResponse(true);
  }

  private void releaseAllJobTasksAsync(final int jobid) throws IOException {
    Timer t = new Timer("CancelJobTasks", true);
    TimerTask task = new TimerTask() {

      @Override
      public void run() {
        try {
          releaseAllJobTasks(jobid);
          LOG.info("Cleaned up all the tasks for the Job.");
        } catch (Throwable e) {
          LOG.error("Error while cleaning up of all Job tasks.", e);
        }
      }
    };
    int finishWaitTime = getFinishWaitTime();
    finishWaitTime = finishWaitTime <= 0 ? 0 : finishWaitTime / 2;
    t.schedule(task, finishWaitTime);
  }

  private void releaseAllJobTasks(int jobid) throws IOException {
    String squeueCmd = conf.get(YARN_APPLICATION_HPC_COMMAND_SLURM_SQUEUE,
        DEFAULT_YARN_APPLICATION_HPC_COMMAND_SLURM_SQUEUE);
    // Output of squeue command will be :
    // YarnTask-{$APP_ID}:{TASK_ID}:{STATE=PD/R}:{COMMENT=[priority]:[cpus]:[memory]}:{HOSTNAME}
    Pattern pattern = Pattern.compile("YarnTask-" + jobid
        + ":(\\d+):([A-Z]+)");
    ArrayList<Integer> jobIds = new ArrayList<Integer>();
    for (String line : Shell.execCommand(squeueCmd, "-h", "-o %j:%i:%t:%k:%N")
        .split("\\n")) {
      Matcher matcher = pattern.matcher(line);
      if (matcher.find()) {
        int cid = Integer.parseInt(matcher.group(1));
        jobIds.add(cid);
      }
    }
    if (jobIds.size() > 0) {
      // Cancel all the running/pending tasks
      String[] release = new String[jobIds.size() + 3];
      String scancelCmd = conf.get(YARN_APPLICATION_HPC_COMMAND_SLURM_SCANCEL,
          DEFAULT_YARN_APPLICATION_HPC_COMMAND_SLURM_SCANCEL);
      release[0] = scancelCmd;
      release[1] = "-b";
      release[2] = "--signal=CONT";
      int index = 2;
      for (Integer jobId : jobIds) {
        release[++index] = String.valueOf(jobId);
      }
      LOG.info("Releasing the Job tasks with slurm Job id's : " + jobIds);
      try {
        Shell.execCommand(release);
      } catch (IOException e) {
        LOG.warn("Exception in scancel", e);
      }
    }
  }

  private void finishJob(final int jobid) throws IOException {
    Timer t = new Timer("FinishJob", false);
    TimerTask task = new TimerTask() {

      @Override
      public void run() {
        String scancelCmd = conf.get(
            YARN_APPLICATION_HPC_COMMAND_SLURM_SCANCEL,
            DEFAULT_YARN_APPLICATION_HPC_COMMAND_SLURM_SCANCEL);
        try {
          Shell.execCommand(scancelCmd, "-b", "--signal=CONT",
              String.valueOf(jobid));
        } catch (IOException e) {
          LOG.info(e);
        }
      }
    };
    t.schedule(task, getFinishWaitTime());
  }

  private int getFinishWaitTime() {
    return conf.getInt(YARN_APPLICATION_HPC_SLURM_FINISH_MAX_WAIT_MS,
        DEFAULT_YARN_APPLICATION_HPC_SLURM_FINISH_MAX_WAIT_MS);
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