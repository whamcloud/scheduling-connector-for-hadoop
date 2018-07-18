// Copyright (c) 2017 DDN. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package org.apache.hadoop.yarn.hpc.pbs;

import static org.apache.hadoop.yarn.hpc.conf.HPCConfiguration.DEFAULT_YARN_APPLICATION_HPC_PBS_JOB_STATUS_FILES_LOCATION;
import static org.apache.hadoop.yarn.hpc.conf.HPCConfiguration.DEFAULT_YARN_APPLICATION_HPC_SCHEDULER_MAXIMUM_ALLOCATION_MB;
import static org.apache.hadoop.yarn.hpc.conf.HPCConfiguration.DEFAULT_YARN_APPLICATION_HPC_SCHEDULER_MAXIMUM_ALLOCATION_VCORES;
import static org.apache.hadoop.yarn.hpc.conf.HPCConfiguration.DEFAULT_YARN_APPLICATION_HPC_SCHEDULER_MINIMUM_ALLOCATION_MB;
import static org.apache.hadoop.yarn.hpc.conf.HPCConfiguration.DEFAULT_YARN_APPLICATION_HPC_SCHEDULER_MINIMUM_ALLOCATION_VCORES;
import static org.apache.hadoop.yarn.hpc.conf.HPCConfiguration.YARN_APPLICATION_HPC_PBS_JOB_STATUS_FILES_LOCATION;
import static org.apache.hadoop.yarn.hpc.conf.HPCConfiguration.YARN_APPLICATION_HPC_SCHEDULER_MAXIMUM_ALLOCATION_MB;
import static org.apache.hadoop.yarn.hpc.conf.HPCConfiguration.YARN_APPLICATION_HPC_SCHEDULER_MAXIMUM_ALLOCATION_VCORES;
import static org.apache.hadoop.yarn.hpc.conf.HPCConfiguration.YARN_APPLICATION_HPC_SCHEDULER_MINIMUM_ALLOCATION_MB;
import static org.apache.hadoop.yarn.hpc.conf.HPCConfiguration.YARN_APPLICATION_HPC_SCHEDULER_MINIMUM_ALLOCATION_VCORES;
import static org.apache.hadoop.yarn.hpc.conf.HPCConfiguration.YARN_APPLICATION_HPC_AM_PRIORITY;
import static org.apache.hadoop.yarn.hpc.conf.HPCConfiguration.YARN_APPLICATION_HPC_AM_RESOURCE_MEMORY_MB;
import static org.apache.hadoop.yarn.hpc.conf.HPCConfiguration.YARN_APPLICATION_HPC_AM_RESOURCE_CPU_VCORES;
import static org.apache.hadoop.yarn.hpc.conf.HPCConfiguration.DEFAULT_YARN_APPLICATION_HPC_AM_PRIORITY;
import static org.apache.hadoop.yarn.hpc.conf.HPCConfiguration.DEFAULT_YARN_APPLICATION_HPC_AM_RESOURCE_MEMORY_MB;
import static org.apache.hadoop.yarn.hpc.conf.HPCConfiguration.DEFAULT_YARN_APPLICATION_HPC_AM_RESOURCE_CPU_VCORES;

import java.io.IOException;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
import org.apache.hadoop.yarn.hpc.api.HPCApplicationClient;
import org.apache.hadoop.yarn.hpc.conf.HPCConfiguration;
import org.apache.hadoop.yarn.hpc.pbs.util.PBSCommandExecutor;
import org.apache.hadoop.yarn.hpc.pbs.util.SocketCache;
import org.apache.hadoop.yarn.hpc.pbs.util.SocketFactory;
import org.apache.hadoop.yarn.hpc.pbs.util.SocketWrapper;
import org.apache.hadoop.yarn.hpc.records.NewApplicationResponse;
import org.apache.hadoop.yarn.hpc.util.HPCUtils;
import org.apache.hadoop.yarn.util.Records;

public class PBSApplicationClient implements HPCApplicationClient, Configurable {

  private static final Log LOG = LogFactory.getLog(PBSApplicationClient.class);
  private Configuration conf;

  @Override
  public NewApplicationResponse getNewApplication() throws IOException {
    NewApplicationResponse response = new NewApplicationResponse();

    int priority = conf.getInt(YARN_APPLICATION_HPC_AM_PRIORITY,
        DEFAULT_YARN_APPLICATION_HPC_AM_PRIORITY);
    int amMemory = conf.getInt(YARN_APPLICATION_HPC_AM_RESOURCE_MEMORY_MB,
        DEFAULT_YARN_APPLICATION_HPC_AM_RESOURCE_MEMORY_MB);
    int cpus = conf.getInt(YARN_APPLICATION_HPC_AM_RESOURCE_CPU_VCORES,
        DEFAULT_YARN_APPLICATION_HPC_AM_RESOURCE_CPU_VCORES);

    SocketWrapper socket = SocketFactory.createSocket();
    String hostName = socket.getHostName();
    int port = socket.getPort();
    int jobid = PBSCommandExecutor.submitAndGetPBSJobId(conf, priority,
        amMemory, cpus, hostName, port);
    SocketCache.addSocket(jobid, socket);

    ApplicationId applicationId = ApplicationId.newInstance(
        getClusterTimestamp(), jobid);
    response.setApplicationId(applicationId);
    response.setMaxCapability(getMaxCapability());
    response.setMinCapability(getMinCapability());
    return response;
  }

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

    String applicationName = context.getApplicationName();
    SocketWrapper socket = SocketCache.getSocket(applicationId.getId());
    if (socket.waitForReady(waitingTime * noOfTimes)) {
      PBSCommandExecutor.launchContainer(
          context.getAMContainerSpec(),
          ContainerId.newContainerId(
              ApplicationAttemptId.newInstance(applicationId, 1), 1l)
              .toString(), applicationName, conf, applicationId.getId(), true,
          socket.getContainerHostName());
    }

    // Set the Job Name
    int jobid = applicationId.getId();
    String pbsJobName = applicationName.replaceAll("\\s", "");
    if (pbsJobName.length() > 13) {
      pbsJobName = pbsJobName.substring(0, 12);
    }

    String qalterCmd = conf.get(
        HPCConfiguration.YARN_APPLICATION_HPC_COMMAND_PBS_QALTER,
        HPCConfiguration.DEFAULT_YARN_APPLICATION_HPC_COMMAND_PBS_QALTER);
    Shell
        .execCommand(qalterCmd, String.valueOf(jobid), "-N", "Y#" + pbsJobName);
  }

  @Override
  public ApplicationReport getApplicationReport(ApplicationId applicationId)
      throws IOException {
    int jobId = applicationId.getId();
    ApplicationReport report = Records.newRecord(ApplicationReport.class);
    report.setYarnApplicationState(YarnApplicationState.SUBMITTED);
    report.setFinalApplicationStatus(FinalApplicationStatus.UNDEFINED);
    report.setApplicationId(applicationId);
    report.setTrackingUrl("N/A");
    report.setDiagnostics("N/A");
    String qstatCmd = conf.get(
        HPCConfiguration.YARN_APPLICATION_HPC_COMMAND_PBS_QSTAT,
        HPCConfiguration.DEFAULT_YARN_APPLICATION_HPC_COMMAND_PBS_QSTAT);
    String result = null;
    try {
      result = Shell.execCommand(qstatCmd, String.valueOf(jobId));
    } catch (IOException e) {
      String message = e.getMessage();
      if (message != null && message.startsWith("qstat: Unknown Job Id")) {
        LOG.info("Couldn't get application report for " + applicationId
            + ", might be completed already or unknown application.");
        return ApplicationReport.newInstance(applicationId, null, "",
            "default", "", "", 0, null, YarnApplicationState.FINISHED, "", "",
            0, 0, FinalApplicationStatus.SUCCEEDED, null, "", 100, null, null);
      }
    }
    if (result != null && result.split("\\n").length > 2) {
      String[] resultLines = result.split("\\n");
      String[] lineParts = resultLines[2].trim().split("\\s+");
      String name = lineParts[1].substring(2);
      String user = lineParts[2];
      String state = lineParts[4];
      String queue = lineParts[5];
      report.setName(name);
      report.setUser(user);
      report.setQueue(queue);
      if ("R".equals(state)) {
        // If the application is running
        String jobStatusLocation = conf.get(
            YARN_APPLICATION_HPC_PBS_JOB_STATUS_FILES_LOCATION,
            DEFAULT_YARN_APPLICATION_HPC_PBS_JOB_STATUS_FILES_LOCATION);
        FileSystem fileSystem = FileSystem.get(conf);
        try {
          FileStatus[] files = fileSystem
              .listStatus(new Path(jobStatusLocation));
          if (files != null && files.length > 0) {
            for (FileStatus fileStatus : files) {
              String fileName = fileStatus.getPath().getName();
              if (fileName.startsWith(jobId + "__")) {
                String[] split = fileName.split("__");
                int port = Integer.parseInt(split[1]);
                String hostName = split[2];
                String trackingUrl;
                if (split.length > 3) {
                  trackingUrl = URLDecoder.decode(split[3],
                      HPCConfiguration.CHAR_ENCODING);
                } else {
                  trackingUrl = "N/A";
                }
                report.setTrackingUrl(trackingUrl);
                report.setRpcPort(port);
                report.setHost(hostName);
                report.setYarnApplicationState(YarnApplicationState.RUNNING);
                break;
              }
            }
          }
        } catch (IOException e) {
          LOG.error("Failed to get the status file for application : "
              + e.getMessage() + ", marking the application as finished.");
          report.setYarnApplicationState(YarnApplicationState.FINISHED);
          report.setFinalApplicationStatus(FinalApplicationStatus.SUCCEEDED);
        }
      } else {
        report.setYarnApplicationState(YarnApplicationState.SUBMITTED);
      }
    } else {
      // if the app details not found
      report.setYarnApplicationState(YarnApplicationState.FINISHED);
      report.setFinalApplicationStatus(FinalApplicationStatus.SUCCEEDED);
    }
    return report;
  }

  @Override
  public List<ApplicationReport> getApplications() throws IOException {
    List<ApplicationReport> applications = new ArrayList<ApplicationReport>();
    String qstatCmd = conf.get(
        HPCConfiguration.YARN_APPLICATION_HPC_COMMAND_PBS_QSTAT,
        HPCConfiguration.DEFAULT_YARN_APPLICATION_HPC_COMMAND_PBS_QSTAT);
    String result = Shell.execCommand(qstatCmd);
    String[] resultLines = result.split("\\n");
    if (resultLines.length > 2) {
      for (int i = 2; i < resultLines.length; i++) {
        String[] lineParts = resultLines[i].trim().split("\\s+");
        if (lineParts.length > 5 && lineParts[1].startsWith("Y#")) {
          // If it is an YARN application
          Matcher matcher = Pattern.compile("(\\d+)").matcher(lineParts[0]);
          if (!matcher.find()) {
            LOG.info("Could not get Job Id from " + result);
            continue;
          }
          ApplicationReport report = Records.newRecord(ApplicationReport.class);
          int jobId = Integer.parseInt(matcher.group(1));

          String name = lineParts[1].substring(2);
          String user = lineParts[2];
          String state = lineParts[4];
          String queue = lineParts[5];
          report.setApplicationId(ApplicationId.newInstance(
              HPCUtils.getClusterTimestamp(), jobId));
          report.setName(name);
          report.setUser(user);
          report.setQueue(queue);
          if ("R".equals(state) || "T".equals(state) || "W".equals(state)
              || "S".equals(state)) {
            report.setYarnApplicationState(YarnApplicationState.RUNNING);
            report.setFinalApplicationStatus(FinalApplicationStatus.UNDEFINED);
          } else if ("E".equals(state)) {
            report.setYarnApplicationState(YarnApplicationState.FINISHED);
            report.setFinalApplicationStatus(FinalApplicationStatus.SUCCEEDED);
          } else {
            report.setYarnApplicationState(YarnApplicationState.SUBMITTED);
            report.setFinalApplicationStatus(FinalApplicationStatus.UNDEFINED);
          }
          applications.add(report);
        }
      }
    }
    return applications;
  }

  @Override
  public boolean forceKillApplication(ApplicationId applicationId)
      throws IOException {
    int jobid = applicationId.getId();
    String qdelCmd = conf.get(
        HPCConfiguration.YARN_APPLICATION_HPC_COMMAND_PBS_QDEL,
        HPCConfiguration.DEFAULT_YARN_APPLICATION_HPC_COMMAND_PBS_QDEL);
    Shell.execCommand(qdelCmd, String.valueOf(jobid));
    return true;
  }

  @Override
  public YarnClusterMetrics getClusterMetrics() throws IOException {
    int noOfNodes = 0;
    String pbsNodesCmd = conf.get(
        HPCConfiguration.YARN_APPLICATION_HPC_COMMAND_PBS_PBSNODES,
        HPCConfiguration.DEFAULT_YARN_APPLICATION_HPC_COMMAND_PBS_PBSNODES);
    String result = Shell.execCommand(pbsNodesCmd, "-a");
    String[] resultLines = result.split("\\n");
    for (String line : resultLines) {
      Matcher matcher = Pattern.compile("(\\s+.+)").matcher(line);
      if (!matcher.find()) {
        noOfNodes++;
      }
    }
    return YarnClusterMetrics.newInstance(noOfNodes);
  }

  @Override
  public List<NodeReport> getClusterNodes(EnumSet<NodeState> states)
      throws IOException {
    List<NodeReport> nodes = new ArrayList<NodeReport>();
    String pbsNodesCmd = conf.get(
        HPCConfiguration.YARN_APPLICATION_HPC_COMMAND_PBS_PBSNODES,
        HPCConfiguration.DEFAULT_YARN_APPLICATION_HPC_COMMAND_PBS_PBSNODES);
    String result = Shell.execCommand(pbsNodesCmd, "-a");
    String[] resultLines = result.split("\\n");

    for (int i = 0; i < resultLines.length; i++) {
      Matcher matcher = Pattern.compile("(\\s+.+)").matcher(resultLines[i]);
      if (!matcher.find()) {
        NodeReport nodeReport = Records.newRecord(NodeReport.class);
        nodeReport.setCapability(Records.newRecord(Resource.class));
        nodeReport.setUsed(Records.newRecord(Resource.class));
        String nodeId = resultLines[i];
        nodeReport.setNodeId(NodeId.newInstance(nodeId, 0));
        i++;
        for (; i < resultLines.length; i++) {
          if (resultLines[i].contains("=") == false) {
            nodes.add(nodeReport);
            i--;
            break;
          }
          String[] keyValuePair = resultLines[i].trim().split("=");
          if ("state".equals(keyValuePair[0].trim())) {
            if ("free".equals(keyValuePair[1].trim())) {
              nodeReport.setNodeState(NodeState.RUNNING);
            }
          } else if ("jobs".equals(keyValuePair[0].trim())) {
            int containers = keyValuePair[1].split(",").length;
            nodeReport.setNumContainers(containers);
          } else if ("resources_available.mem".equals(keyValuePair[0].trim())) {
            if (keyValuePair[1].trim().endsWith("kb")) {
              int mb = Integer.parseInt(keyValuePair[1].trim().substring(0,
                  keyValuePair[1].trim().indexOf("kb")));
              nodeReport.getCapability().setMemory(mb);
            }
          } else if ("resources_available.ncpus".equals(keyValuePair[0].trim())) {
            int ncpus = Integer.parseInt(keyValuePair[1].trim());
            nodeReport.getCapability().setVirtualCores(ncpus);
          } else if ("resources_assigned.mem".equals(keyValuePair[0].trim())) {
            if (keyValuePair[1].trim().endsWith("kb")) {
              int mb = Integer.parseInt(keyValuePair[1].trim().substring(0,
                  keyValuePair[1].trim().indexOf("kb")));
              nodeReport.getCapability().setMemory(mb);
            }
          } else if ("resources_assigned.ncpus".equals(keyValuePair[0].trim())) {
            int ncpus = Integer.parseInt(keyValuePair[1].trim());
            nodeReport.getCapability().setVirtualCores(ncpus);
          }
        }
      }
    }
    return nodes;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
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

}
