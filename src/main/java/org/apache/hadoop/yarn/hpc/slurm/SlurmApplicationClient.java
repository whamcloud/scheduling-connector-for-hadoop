// Copyright (c) 2018 DDN. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package org.apache.hadoop.yarn.hpc.slurm;

import static org.apache.hadoop.yarn.hpc.conf.HPCConfiguration.DEFAULT_YARN_APPLICATION_HPC_SCHEDULER_MAXIMUM_ALLOCATION_MB;
import static org.apache.hadoop.yarn.hpc.conf.HPCConfiguration.DEFAULT_YARN_APPLICATION_HPC_SCHEDULER_MAXIMUM_ALLOCATION_VCORES;
import static org.apache.hadoop.yarn.hpc.conf.HPCConfiguration.DEFAULT_YARN_APPLICATION_HPC_SCHEDULER_MINIMUM_ALLOCATION_MB;
import static org.apache.hadoop.yarn.hpc.conf.HPCConfiguration.DEFAULT_YARN_APPLICATION_HPC_SCHEDULER_MINIMUM_ALLOCATION_VCORES;
import static org.apache.hadoop.yarn.hpc.conf.HPCConfiguration.YARN_APPLICATION_HPC_SCHEDULER_MAXIMUM_ALLOCATION_MB;
import static org.apache.hadoop.yarn.hpc.conf.HPCConfiguration.YARN_APPLICATION_HPC_SCHEDULER_MAXIMUM_ALLOCATION_VCORES;
import static org.apache.hadoop.yarn.hpc.conf.HPCConfiguration.YARN_APPLICATION_HPC_SCHEDULER_MINIMUM_ALLOCATION_MB;
import static org.apache.hadoop.yarn.hpc.conf.HPCConfiguration.YARN_APPLICATION_HPC_SCHEDULER_MINIMUM_ALLOCATION_VCORES;

import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.api.records.YarnClusterMetrics;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.hpc.api.HPCApplicationClient;
import org.apache.hadoop.yarn.hpc.conf.HPCConfiguration;
import org.apache.hadoop.yarn.hpc.records.NewApplicationResponse;
import org.apache.hadoop.yarn.hpc.util.HPCCommandExecutor;
import org.apache.hadoop.yarn.hpc.util.HPCUtils;
import org.apache.hadoop.yarn.util.Records;

public class SlurmApplicationClient implements HPCApplicationClient,
    Configurable {

  private static final Log LOG = LogFactory.getLog(SlurmApplicationClient.class);  
  private Configuration conf;  

  @SuppressWarnings("deprecation")
  @Override
  public void submitApplication(ApplicationSubmissionContext context)
      throws IOException {
    int waitingTime = conf.getInt(
        HPCConfiguration.YARN_APPLICATION_HPC_CLIENT_RS_MAX_WAIT_MS,
        HPCConfiguration.DEFAULT_YARN_APPLICATION_HPC_CLIENT_RS_MAX_WAIT_MS);
    int noOfTimes = conf.getInt(
        HPCConfiguration.YARN_APPLICATION_HPC_CLIENT_RS_RETRIES_MAX,
        HPCConfiguration.DEFAULT_YARN_APPLICATION_HPC_CLIENT_RS_RETRIES_MAX);
    ApplicationId applicationId = context.getApplicationId();
    String nodeName = checkAndWaitForResourcesToBeAvailable(
        applicationId, waitingTime, noOfTimes);
        
    HPCCommandExecutor.launchContainer(
    		context.getAMContainerSpec(),
        ContainerId.newInstance(
            ApplicationAttemptId.newInstance(applicationId, 1), 1).toString(),
        context.getApplicationName(), conf, applicationId.getId(), nodeName);
    HPCCommandExecutor.setJobState(applicationId.getId(), "running::0", conf);
  }

  private String checkAndWaitForResourcesToBeAvailable(ApplicationId applicationId,
      int waitingTime, int maxNoOfTimes) throws IOException {
    String squeueCmd = conf.get(
        HPCConfiguration.YARN_APPLICATION_HPC_COMMAND_SLURM_SQUEUE,
        HPCConfiguration.DEFAULT_YARN_APPLICATION_HPC_COMMAND_SLURM_SQUEUE);
    int noOfTimesTried = 0;
    try {
      String[] result = getJobStateAndNodeName(applicationId.getId(), squeueCmd);
      while (result != null && "PD".equals(result[0])
          && noOfTimesTried < maxNoOfTimes) {
        // wait for some time and try again
        try {
          Thread.sleep(waitingTime);
        } catch (InterruptedException e) {
          LOG.debug(e);
        }
        LOG.debug("Failed to get resources for " + applicationId
            + ", retrying times:" + (++noOfTimesTried));
        result = getJobStateAndNodeName(applicationId.getId(), squeueCmd);
      }
      if (result == null || result.length < 1 || "R".equals(result[0]) == false) {
        // cancel the Job
        String scancelCmd = conf
            .get(
                HPCConfiguration.YARN_APPLICATION_HPC_COMMAND_SLURM_SCANCEL,
                HPCConfiguration.DEFAULT_YARN_APPLICATION_HPC_COMMAND_SLURM_SCANCEL);
        Shell.execCommand(scancelCmd, String.valueOf(applicationId.getId()));
        String state = (result == null || result.length < 1) ? null : result[0];
        throw new YarnRuntimeException(
            "Failed to submit application, state of the Job is : " + state);
      }
      return result[1];
    } catch (IOException e) {
      throw new YarnRuntimeException(
          "Failed to get the resources for application to submit.", e);
    }
  }

  private String[] getJobStateAndNodeName(int jobId, String squeueCmd)
      throws IOException {
    String cmdResult = Shell.execCommand(squeueCmd, "-h", "-o", "%t:%N", "-j",
        String.valueOf(jobId));
    String[] result = cmdResult != null ? cmdResult.trim().split(":") : null;
    return result;
  }
 
  @Override
  public ApplicationReport getApplicationReport(ApplicationId applicationId)
      throws IOException {
    List<ApplicationReport> reports = null;
    try {
      reports = getApplications(applicationId.getId());
    } catch (Throwable e) {
      LOG.info("Couldn't get application report for " + applicationId
          + ", might be completed already.");
    }
    if (reports == null || reports.isEmpty()) {
      return ApplicationReport.newInstance(applicationId, null, "", "default",
          "", "", 0, null, YarnApplicationState.FINISHED, "", "", 0, 0,
          FinalApplicationStatus.SUCCEEDED, null, "", 100, null, null);
    }
    return reports.get(0);
  }

  @Override
  public List<ApplicationReport> getApplications() throws IOException {
    return getApplications(-1);
  }

  public List<ApplicationReport> getApplications(int id) {
    String result = null;
    String squeueCmd = conf.get(
        HPCConfiguration.YARN_APPLICATION_HPC_COMMAND_SLURM_SQUEUE,
        HPCConfiguration.DEFAULT_YARN_APPLICATION_HPC_COMMAND_SLURM_SQUEUE);
    List<ApplicationReport> applications = new ArrayList<ApplicationReport>();
    try {
      String argJ = id < 0 ? "" : "-j " + id;
      result = Shell.execCommand(squeueCmd, "-h", argJ, "-o %i %P %j %u %k");

      String[] resultLines = result.split("\\n");
      for (String line : resultLines) {
        if (line == null || line.length() == 0) {
          return applications;
        }

        String[] lineParts = line.trim().split("\\s+");

        if (!("YarnMaster".equals(lineParts[2]) && StringUtils
            .isNotEmpty(lineParts[4])))
          continue;

        String[] addr = lineParts[4].split(":");
        if (addr.length != 3)
          continue;

        ApplicationReport appReport = Records
            .newRecord(ApplicationReport.class);

        int jobid = Integer.parseInt(lineParts[0]);
        ApplicationId applicationId = ApplicationId.newInstance(
            getClusterTimestamp(), jobid);

        appReport.setApplicationId(applicationId);
        appReport.setTrackingUrl("");
        appReport.setQueue(lineParts[1]);
        appReport.setName(lineParts[2]);
        appReport.setUser(lineParts[3]);

        appReport.setHost(addr[1]);
        appReport.setRpcPort(Integer.parseInt(addr[2]));

        String stateStr = addr[0].toUpperCase();
        YarnApplicationState state = YarnApplicationState.valueOf(stateStr);
        appReport.setYarnApplicationState(state);
        if (state == YarnApplicationState.FINISHED) {
          appReport.setFinalApplicationStatus(FinalApplicationStatus.SUCCEEDED);
        }else{
          appReport.setFinalApplicationStatus(FinalApplicationStatus.UNDEFINED);
        }
        applications.add(appReport);
      }
    } catch (Throwable t) {
      throw new YarnRuntimeException(result, t);
    }
    return applications;
  }

  @Override
  public boolean forceKillApplication(ApplicationId applicationId)
      throws IOException {
    int jobid = applicationId.getId();
    String scancelCmd = conf.get(
        HPCConfiguration.YARN_APPLICATION_HPC_COMMAND_SLURM_SCANCEL,
        HPCConfiguration.DEFAULT_YARN_APPLICATION_HPC_COMMAND_SLURM_SCANCEL);
    Shell.execCommand(scancelCmd, String.valueOf(jobid));
    return true;
  }

  @Override
  public YarnClusterMetrics getClusterMetrics() throws IOException {
    String sinfoCmd = conf.get(
        HPCConfiguration.YARN_APPLICATION_HPC_COMMAND_SLURM_SINFO,
        HPCConfiguration.DEFAULT_YARN_APPLICATION_HPC_COMMAND_SLURM_SINFO);
    String result = Shell.execCommand(sinfoCmd, "-h", "-o %D");
    int parseInt;
    try {
      parseInt = Integer.parseInt(result.trim());
    } catch (Throwable e) {
      throw new IOException("Failed to get cluster metrics", e);
    }
    return YarnClusterMetrics.newInstance(parseInt);
  }

  @Override
  public List<NodeReport> getClusterNodes(EnumSet<NodeState> states)
      throws IOException {
    String sinfoCmd = conf.get(
        HPCConfiguration.YARN_APPLICATION_HPC_COMMAND_SLURM_SINFO,
        HPCConfiguration.DEFAULT_YARN_APPLICATION_HPC_COMMAND_SLURM_SINFO);
    String result = Shell.execCommand(sinfoCmd, "-h", "-o %N");
    List<NodeReport> reports = new ArrayList<NodeReport>();

    for (String node : HPCUtils.parseHostList(result)) {

      NodeReport nodeReport = NodeReport.newInstance(
          NodeId.newInstance(node, 0), NodeState.RUNNING, "", "",
          Resource.newInstance(0, 0), Resource.newInstance(1024, 2), 0, "", 0);
      reports.add(nodeReport);
    }
    return reports;
  }

  @Override
  public NewApplicationResponse getNewApplication() throws IOException {
    int amMemory = conf.getInt("yarn.application.hpc.am.resource.mb", 1536);
    int cpus = conf.getInt("yarn.application.hpc.am.resource.cpu-vcores", 1);
    int jobid = HPCCommandExecutor.createNewJob("YarnMaster", "new::0", conf,
        amMemory, cpus);
    ApplicationId applicationId = ApplicationId.newInstance(
        getClusterTimestamp(), jobid);
    NewApplicationResponse response = new NewApplicationResponse();
    response.setApplicationId(applicationId);
    response.setMaxCapability(getMaxCapability());
    response.setMinCapability(getMinCapability());
    return response;
  }

  protected long getClusterTimestamp() {
    return HPCUtils.getClusterTimestamp();
  }

  protected Resource getMaxCapability() {
    int maxAllocMemory = conf.getInt(
        YARN_APPLICATION_HPC_SCHEDULER_MAXIMUM_ALLOCATION_MB,
        DEFAULT_YARN_APPLICATION_HPC_SCHEDULER_MAXIMUM_ALLOCATION_MB);
    int maxAllocCPUs = conf.getInt(
        YARN_APPLICATION_HPC_SCHEDULER_MAXIMUM_ALLOCATION_VCORES,
        DEFAULT_YARN_APPLICATION_HPC_SCHEDULER_MAXIMUM_ALLOCATION_VCORES);
    return Resource.newInstance(maxAllocMemory, maxAllocCPUs);
  }

  protected Resource getMinCapability() {
    int minAllocMemory = conf.getInt(
        YARN_APPLICATION_HPC_SCHEDULER_MINIMUM_ALLOCATION_MB,
        DEFAULT_YARN_APPLICATION_HPC_SCHEDULER_MINIMUM_ALLOCATION_MB);
    int minAllocCPUs = conf.getInt(
        YARN_APPLICATION_HPC_SCHEDULER_MINIMUM_ALLOCATION_VCORES,
        DEFAULT_YARN_APPLICATION_HPC_SCHEDULER_MINIMUM_ALLOCATION_VCORES);
    return Resource.newInstance(minAllocMemory, minAllocCPUs);
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }
}
