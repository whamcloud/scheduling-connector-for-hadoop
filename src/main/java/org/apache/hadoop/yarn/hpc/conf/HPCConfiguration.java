// Copyright (c) 2017 Intel Corporation. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package org.apache.hadoop.yarn.hpc.conf;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.hpc.slurm.SlurmApplicationClient;
import org.apache.hadoop.yarn.hpc.slurm.SlurmApplicationMaster;
import org.apache.hadoop.yarn.hpc.slurm.SlurmContainerManager;

@Public
@Evolving
public class HPCConfiguration {
  
  /**
   * Tokens file name
   */
  public static final String TOKENS_FILE_NAME = "job_tokens";
  
  /**
   * Character encoding
   */
  public static final String CHAR_ENCODING = "UTF-8";
  
  /**
   * HPC Application Client class implementation
   */
  public static final String YARN_APPLICATION_HPC_CLIENT_CLASS = 
      "yarn.application.hpc.client.class";
  public static final Class<SlurmApplicationClient> DEFAULT_YARN_APPLICATION_HPC_CLIENT_CLASS = 
      SlurmApplicationClient.class;
  
  /**
   * HPC Application Master class implementation
   */
  public static final String YARN_APPLICATION_HPC_APPLICATIONMASTER_CLASS = 
      "yarn.application.hpc.applicationmaster.class";
  public static final Class<SlurmApplicationMaster> DEFAULT_YARN_APPLICATION_HPC_APPLICATIONMASTER_CLASS = 
      SlurmApplicationMaster.class;
  
  /**
   * HPC Application Container Manager class implementation
   */
  public static final String YARN_APPLICATION_HPC_CONTAINERMANAGER_CLASS = 
      "yarn.application.hpc.containermanager.class";
  public static final Class<SlurmContainerManager> DEFAULT_YARN_APPLICATION_HPC_CONTAINERMANAGER_CLASS = 
      SlurmContainerManager.class;
  
  /**
   * Local directories for yarn applications in the HPC environment
   */
  public static final String YARN_APPLICATION_HPC_LOCAL_DIRS = 
      "yarn.application.hpc.local.dirs";
  
  /**
   * Logs directories for yarn applications in the HPC environment
   */
  public static final String YARN_APPLICATION_HPC_LOG_DIRS = 
      "yarn.application.hpc.log.dirs";
  
  /**
   * Work directory for yarn applications in the HPC environment
   */
  public static final String YARN_APPLICATION_HPC_WORK_DIR = 
      "yarn.application.hpc.work.dir";
  public static final String DEFAULT_YARN_APPLICATION_HPC_WORK_DIR = "/tmp/";
  
  /**
   * sbatch slurm command
   */
  public static final String YARN_APPLICATION_HPC_COMMAND_SLURM_SBATCH = 
      "yarn.application.hpc.command.slurm.sbatch";
  public static final String DEFAULT_YARN_APPLICATION_HPC_COMMAND_SLURM_SBATCH = 
      "sbatch";
  
  /**
   * scontrol slurm command
   */
  public static final String YARN_APPLICATION_HPC_COMMAND_SLURM_SCONTROL = 
      "yarn.application.hpc.command.slurm.scontrol";
  public static final String DEFAULT_YARN_APPLICATION_HPC_COMMAND_SLURM_SCONTROL = 
      "scontrol";
  
  /**
   * scancel slurm command
   */
  public static final String YARN_APPLICATION_HPC_COMMAND_SLURM_SCANCEL = 
      "yarn.application.hpc.command.slurm.scancel";
  public static final String DEFAULT_YARN_APPLICATION_HPC_COMMAND_SLURM_SCANCEL = 
      "scancel";
  
  /**
   * srun slurm command
   */
  public static final String YARN_APPLICATION_HPC_COMMAND_SLURM_SRUN = 
      "yarn.application.hpc.command.slurm.srun";
  public static final String DEFAULT_YARN_APPLICATION_HPC_COMMAND_SLURM_SRUN = 
      "srun";
  
  /**
   * squeue slurm command
   */
  public static final String YARN_APPLICATION_HPC_COMMAND_SLURM_SQUEUE = 
      "yarn.application.hpc.command.slurm.squeue";
  public static final String DEFAULT_YARN_APPLICATION_HPC_COMMAND_SLURM_SQUEUE = 
      "squeue";
  
  /**
   * sinfo slurm command
   */
  public static final String YARN_APPLICATION_HPC_COMMAND_SLURM_SINFO = 
      "yarn.application.hpc.command.slurm.sinfo";
  public static final String DEFAULT_YARN_APPLICATION_HPC_COMMAND_SLURM_SINFO = 
      "sinfo";
  
  /**
   * Wait time for checking the resource availability in HPC Resource scheduler
   */
  public static final String YARN_APPLICATION_HPC_CLIENT_RS_MAX_WAIT_MS = 
      "yarn.application.hpc.client-rs.max-wait.ms";
  public static final int DEFAULT_YARN_APPLICATION_HPC_CLIENT_RS_MAX_WAIT_MS = 2000;

  /**
   * No of max retries to HPC Resource scheduler when resources not available
   */
  public static final String YARN_APPLICATION_HPC_CLIENT_RS_RETRIES_MAX = 
      "yarn.application.hpc.client-rs.retries.max";
  public static final int DEFAULT_YARN_APPLICATION_HPC_CLIENT_RS_RETRIES_MAX = 100;
  
  /**
   * Wait time before giving SIGTERM signal to the Job
   */
  public static final String YARN_APPLICATION_HPC_SLURM_FINISH_MAX_WAIT_MS = 
      "yarn.application.hpc.slurm.finish.max-wait.ms";
  public static final int DEFAULT_YARN_APPLICATION_HPC_SLURM_FINISH_MAX_WAIT_MS = 4000;
  
  /**
   * Whether to aggregate container logs or not
   */
  public static final String YARN_APPLICATION_HPC_SLURM_LOG_AGGREGATION_ENABLE = 
      "yarn.application.hpc.slurm.log-aggregation.enable";
  public static final boolean DEFAULT_YARN_APPLICATION_HPC_SLURM_LOG_AGGREGATION_ENABLE = true;
  
  /**
   * The maximum allocation for every container request at the HPC scheduler, in
   * MBs.
   */
  public static final String YARN_APPLICATION_HPC_SCHEDULER_MAXIMUM_ALLOCATION_MB = 
      "yarn.application.hpc.scheduler.maximum-allocation-mb";
  public static final int DEFAULT_YARN_APPLICATION_HPC_SCHEDULER_MAXIMUM_ALLOCATION_MB = 65536;
  
  /**
   * The maximum allocation for every container request at the HPC scheduler, 
   * in terms of virtual CPU cores.
   */
  public static final String YARN_APPLICATION_HPC_SCHEDULER_MAXIMUM_ALLOCATION_VCORES = 
      "yarn.application.hpc.scheduler.maximum-allocation-vcores";
  public static final int DEFAULT_YARN_APPLICATION_HPC_SCHEDULER_MAXIMUM_ALLOCATION_VCORES = 48;
  
  /**
   * The minimum allocation for every container request at the HPC scheduler, in
   * MBs.
   */
  public static final String YARN_APPLICATION_HPC_SCHEDULER_MINIMUM_ALLOCATION_MB = 
      "yarn.application.hpc.scheduler.minimum-allocation-mb";
  public static final int DEFAULT_YARN_APPLICATION_HPC_SCHEDULER_MINIMUM_ALLOCATION_MB = 512;
  
  /**
   * The minimum allocation for every container request at the HPC scheduler, 
   * in terms of virtual CPU cores.
   */
  public static final String YARN_APPLICATION_HPC_SCHEDULER_MINIMUM_ALLOCATION_VCORES = 
      "yarn.application.hpc.scheduler.minimum-allocation-vcores";
  public static final int DEFAULT_YARN_APPLICATION_HPC_SCHEDULER_MINIMUM_ALLOCATION_VCORES = 1;
  
  /*
   * Max no of threads used for container localization
   */
  public static final String YARN_APPLICATION_HPC_LOCALIZATION_THREADS = 
      "yarn.application.hpc.localization.threads";
  public static final int DEFAULT_YARN_APPLICATION_HPC_LOCALIZATION_THREADS = 4;
  
  /*
   * Application Master resource request priority
   */
  public static final String YARN_APPLICATION_HPC_AM_PRIORITY =
      "yarn.application.hpc.am.priority";
  public static final int DEFAULT_YARN_APPLICATION_HPC_AM_PRIORITY = 10;
  
  /*
   * Application Master resource memory in mb
   */
  public static final String YARN_APPLICATION_HPC_AM_RESOURCE_MEMORY_MB =
      "yarn.application.hpc.am.resource.mb";
  public static final int DEFAULT_YARN_APPLICATION_HPC_AM_RESOURCE_MEMORY_MB = 1536;
  
  /*
   * Application Master resource CPU vcores
   */
  public static final String YARN_APPLICATION_HPC_AM_RESOURCE_CPU_VCORES =
      "yarn.application.hpc.am.resource.cpu-vcores";
  public static final int DEFAULT_YARN_APPLICATION_HPC_AM_RESOURCE_CPU_VCORES = 1;
  
  /******* PBC Scheduler Commands *********/
  /**
   * PBS command to submit Job
   */
  public static final String YARN_APPLICATION_HPC_COMMAND_PBS_QSUB = 
      "yarn.application.hpc.command.pbs.qsub";
  public static final String DEFAULT_YARN_APPLICATION_HPC_COMMAND_PBS_QSUB = "qsub";

  /**
   * PBS Command to cancel Job
   */
  public static final String YARN_APPLICATION_HPC_COMMAND_PBS_QDEL =
      "yarn.application.hpc.command.pbs.qdel";
  public static final String DEFAULT_YARN_APPLICATION_HPC_COMMAND_PBS_QDEL = "qdel";
  
  /**
   * PBS Command to get Job stats
   */
  public static final String YARN_APPLICATION_HPC_COMMAND_PBS_QSTAT = 
      "yarn.application.hpc.command.pbs.qstat";
  public static final String DEFAULT_YARN_APPLICATION_HPC_COMMAND_PBS_QSTAT = 
      "qstat";
  
  /**
   * PBS Command to alter job properties
   */
  public static final String YARN_APPLICATION_HPC_COMMAND_PBS_QALTER = 
      "yarn.application.hpc.command.pbs.qalter";
  public static final String DEFAULT_YARN_APPLICATION_HPC_COMMAND_PBS_QALTER = 
      "qalter";

  /**
   * PBS Command to get pbs nodes
   */
  public static final String YARN_APPLICATION_HPC_COMMAND_PBS_PBSNODES =
      "yarn.application.hpc.command.pbs.pbsnodes";
  public static final String DEFAULT_YARN_APPLICATION_HPC_COMMAND_PBS_PBSNODES =
      "pbsnodes";

  /**
   * PBS Jobs status files location in DFS
   */
  public static final String YARN_APPLICATION_HPC_PBS_JOB_STATUS_FILES_LOCATION = 
      "yarn.application.hpc.pbs.job-status-files.location";
  public static final String DEFAULT_YARN_APPLICATION_HPC_PBS_JOB_STATUS_FILES_LOCATION = 
      "/home/pbs/jobs-status";
  
  
  public static String[] getHPCLogDirs(Configuration conf) {
    return StringUtils.getTrimmedStrings(conf.get(
        HPCConfiguration.YARN_APPLICATION_HPC_LOG_DIRS, conf.get(
            YarnConfiguration.NM_LOG_DIRS,
            YarnConfiguration.DEFAULT_NM_LOG_DIRS)));
  }

  public static String[] getHPCLocalDirs(Configuration conf) {
    String getLocalDirs = conf.get(
        HPCConfiguration.YARN_APPLICATION_HPC_LOCAL_DIRS, conf.get(
            YarnConfiguration.NM_LOCAL_DIRS,
            YarnConfiguration.DEFAULT_NM_LOCAL_DIRS));
    return StringUtils.getTrimmedStrings(getLocalDirs);
  }
}
