// Copyright (c) 2017 Intel Corporation. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package org.apache.hadoop.yarn.hpc.util;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.util.Shell.ShellCommandExecutor;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.hpc.conf.HPCConfiguration;
import org.apache.hadoop.yarn.util.Apps;
import org.apache.hadoop.yarn.util.ConverterUtils;

public class HPCCommandExecutor {

  private static final Log LOG = LogFactory.getLog(HPCCommandExecutor.class);

  public static ContainerId launchContainer(
      ContainerLaunchContext amContainerSpec, String containerIdStr,
      String appName, Configuration conf, int jobId, String nodeName) throws IOException {

    ContainerId containerId = ConverterUtils.toContainerId(containerIdStr);
    ApplicationId applicationId = containerId.getApplicationAttemptId().getApplicationId();
    String hpcLogDir = getHPCLogDir(conf);
    String slurmWorkDir = getHPCWorkDir(conf);
       
    String slurmLogs = slurmWorkDir + File.separatorChar + containerIdStr;
    String containerLogsDir = hpcLogDir + File.separatorChar + applicationId
        + File.separatorChar + containerIdStr;
    String workDir = getHPCLocalDir(containerIdStr, conf, applicationId);

    Map<String, String> environment = setUpEnvironment(
        amContainerSpec.getEnvironment(), hpcLogDir);
    
    // Fix to get around MAPREDUCE-5813: Make sure the workdir is in CLASSPATH
    // Else job.xml is not loaded by child tasks.    
    String classpath = environment.get("CLASSPATH");
    if (classpath != null) {
      String currentDir = System.getenv("LOCAL_DIRS");
      if (currentDir != null) {
        classpath = classpath.replaceAll(currentDir, workDir);
      }
      // Replace variable
      classpath = classpath.replace("{{CLASSPATH}}", "$CLASSPATH");
      classpath = classpath.replaceAll("<CPS>", ":");

      // Adding current directory
      if (currentDir == null) {
        currentDir = ".";
      }
      classpath = currentDir + "/:" + currentDir + "/*:" + classpath + ":";
      
      environment.put("CLASSPATH", classpath);
    }
    
    environment.put("CONTAINER_ID", containerIdStr);
    if (nodeName != null) {
      environment.put("NM_HOST", nodeName);
    }
    environment.put("NM_PORT", "0");
    environment.put("NM_HTTP_PORT", "0");
    environment.put("APP_SUBMIT_TIME_ENV", String.valueOf(HPCUtils.getClusterTimestamp()));
    environment.put("MAX_APP_ATTEMPTS", "1");
    environment.put("LOCAL_DIRS", workDir);                
    environment.put(UserGroupInformation.HADOOP_TOKEN_FILE_LOCATION, HPCConfiguration.TOKENS_FILE_NAME);
    environment.put(ApplicationConstants.APPLICATION_WEB_PROXY_BASE_ENV, "/");
    
    // build the command
    List<String> commands = amContainerSpec.getCommands();
    StringBuilder finalCmdBuilder = new StringBuilder();
    for (String cmd : commands) {
      finalCmdBuilder.append(cmd);
      finalCmdBuilder.append(" ");
    }
    String finalCommand = finalCmdBuilder.toString().replace("<LOG_DIR>",
        containerLogsDir);
    
    //Java home notation resolve
    finalCommand = finalCommand.replace("{{JAVA_HOME}}", "$JAVA_HOME");

    finalCommand = finalCommand.replace("\\\"", "\"");
    
    if (appName == null) {
      appName = System.getenv("APP_NAME");
      appName = "Task-" + appName + "-";
    } else {
      environment.put("APP_NAME", appName);
      appName = "Master-" + appName + "-";
    }
    appName += containerId.getApplicationAttemptId().getApplicationId();

    ByteArrayOutputStream script = new ByteArrayOutputStream(1024);
    PrintStream stream = new PrintStream(script);
    stream.println("#!" + System.getenv("SHELL"));

    // Create directories
    stream.println("if [ -d \"" + containerLogsDir + "\" ]; then");
    stream.println("rm -rf \"" + containerLogsDir + "\"");
    stream.println("fi");
    stream.println("mkdir -p \"" + containerLogsDir + "\"");

    stream.println("if [ -d \"" + workDir + "\" ]; then");
    stream.println("rm -rf \"" + workDir + "\"");
    stream.println("fi");
    stream.println("mkdir -p \"" + workDir + "\"");
    stream.println("cd \"" + workDir + "\"");
    //creating tmp dir in local dir
    stream.println("mkdir tmp");
    
    // Save all tokens
    ByteBuffer amTokens = amContainerSpec.getTokens();
    if (amTokens != null) {
      String tokens = new String(Hex.encodeHex(amTokens.array()));
      stream.println("echo -n '" + tokens
          + "' | perl -ne 's/([0-9a-f]{2})/print chr hex $1/gie' > "
          + HPCConfiguration.TOKENS_FILE_NAME);
    } else {
      // Security framework already loaded the tokens into current ugi
      Credentials credentials = UserGroupInformation.getCurrentUser()
          .getCredentials();
      DataOutputBuffer dob = new DataOutputBuffer();
      credentials.writeTokenStorageToStream(dob);
      ByteBuffer securityTokens = ByteBuffer.wrap(dob.getData(), 0,
          dob.getLength());
      String tokens = new String(Hex.encodeHex(securityTokens.array()));
      stream.println("echo -n '" + tokens
          + "' | perl -ne 's/([0-9a-f]{2})/print chr hex $1/gie' > "
          + HPCConfiguration.TOKENS_FILE_NAME);
    }
    
    StringBuilder localizationArguments = new StringBuilder();
    localizationArguments.append(workDir);
    localizationArguments.append(" ");
    localizationArguments.append(conf.getInt(
        HPCConfiguration.YARN_APPLICATION_HPC_LOCALIZATION_THREADS,
        HPCConfiguration.DEFAULT_YARN_APPLICATION_HPC_LOCALIZATION_THREADS));
    // Resource localization        
    for (Entry<String, LocalResource> entry : amContainerSpec.getLocalResources().entrySet()) {
      try {
        LocalResource value = entry.getValue();
        String resource = ConverterUtils.getPathFromYarnURL(value.getResource())
            .toString();
        String key = entry.getKey();
        String pattern = value.getPattern();
        if (pattern != null) {
          // Encode the special characters in the pattern
          pattern = URLEncoder.encode(pattern, HPCConfiguration.CHAR_ENCODING);
        }
        localizationArguments.append(" ");
        localizationArguments.append(resource);
        localizationArguments.append(" ");
        localizationArguments.append(key);
        localizationArguments.append(" ");
        localizationArguments.append(value.getVisibility().toString());
        localizationArguments.append(" ");
        localizationArguments.append(value.getType().toString());
        localizationArguments.append(" ");
        localizationArguments.append(pattern);
      } catch (URISyntaxException e) {
        LOG.info(e);
      }
    }
    
    //Command for localization
    stream.println(System.getenv("JAVA_HOME") + "/bin/java "
        + "org.apache.hadoop.yarn.hpc.localization.AppLocalizer " + localizationArguments.toString());
    
    // Command execution
    stream.println(finalCommand);
    stream.println("scancel -b --signal=CONT " + jobId);
    stream.close();

    LOG.info(script.toString());
    String sbatchCmd = conf.get(
        HPCConfiguration.YARN_APPLICATION_HPC_COMMAND_SLURM_SBATCH,
        HPCConfiguration.DEFAULT_YARN_APPLICATION_HPC_COMMAND_SLURM_SBATCH);
    try {
      String[] command = new String[]{sbatchCmd, "--jobid=" + jobId, "--job-name=" + appName,
          "--output=" + slurmLogs + ".out", "--error=" + slurmLogs + ".err"};

      Shell.ShellCommandExecutor shell = new Shell.ShellCommandExecutor
      (command, new File("."), environment);
      StdInputThread exec = new StdInputThread(shell, script.toString());
      exec.start();
      shell.execute();
      exec.checkException();
      LOG.info("Output : " + shell.getOutput());
    } catch (IOException e) {
      throw new YarnRuntimeException(e);
    }
    return containerId;
  }

  private static Map<String, String> setUpEnvironment(
      Map<String, String> launchEnv, String hpcLogDir) {
    // Set up environment
    Map<String, String> environment = new HashMap<String, String>();
    for (String key : System.getenv().keySet()) {
      if (key.contains("HADOOP"))
        environment.put(key, System.getenv(key));
    }

    // Replace environment variables with actual values
    Pattern pattern = Pattern.compile("(\\$[\\w]+)");
    Set<String> envKeys = new HashSet<>(launchEnv.keySet());
    for (String key : envKeys) {
      String value = launchEnv.get(key);
      Matcher matcher = pattern.matcher(value);
      while (matcher.find()) {
        String var = matcher.group();
        String envVar = System.getenv(var.substring(1));
        if(envVar == null){
          continue;
        }
        value = value.replace(var, envVar);
      }
      value = value.replace("<LOG_DIR>", hpcLogDir);
      launchEnv.put(key, value);
    }
    environment.putAll(launchEnv);
    environment.put("JAVA_HOME", System.getenv("JAVA_HOME"));
    String ldLibraryPath = System.getenv("LD_LIBRARY_PATH");
    if (ldLibraryPath != null) {
      environment.put("LD_LIBRARY_PATH", ldLibraryPath);
    }
    return environment;
  }

  public static void setJobState(int jobid, String state, Configuration conf)
      throws IOException {
    String scontrolCmd = conf.get(
        HPCConfiguration.YARN_APPLICATION_HPC_COMMAND_SLURM_SCONTROL,
        HPCConfiguration.DEFAULT_YARN_APPLICATION_HPC_COMMAND_SLURM_SCONTROL);
    Shell
        .execCommand(scontrolCmd, "update", "job=" + jobid, "Comment=" + state);
  }

  public static class StdInputThread extends Thread {

    private IOException exception;
    private String input;
    private ShellCommandExecutor shell = null;

    public StdInputThread(ShellCommandExecutor shell, String input) {
      this.shell = shell;
      this.input = input;
    }

    public void run() {
      exception = null;
      try {
        Process p = null;
        // Wait until process starts
        while (true) {
          p = shell.getProcess();
          if (null != p)
            break;

          try {
            Thread.sleep(1);
          } catch (InterruptedException ie) {
            throw new IOException(ie);
          }
        }
        if (p != null) {
          OutputStream out = null;
          IOException exception = null;
          try {
            out = p.getOutputStream();
            out.write(input.getBytes());
          } catch (IOException ioe) {
            exception = ioe;
          } finally {
            try {
              out.close();
            } catch (IOException e) {
              if (exception == null) {
                exception = e;
              }
            }
            if (exception != null) {
              throw exception;
            }
          }
        }
        // Write script to process STDIN
      } catch (IOException ioe) {
        exception = ioe;
      }
    }

    public void checkException() throws IOException {
      if (exception != null)
        throw exception;
    }
  }

  public static int createNewJob(String name, String metainfo,
      Configuration conf, int memory, int cpus) throws IOException {
    
    String slurmWorkDir = getHPCWorkDir(conf);
    if (metainfo == null)
      metainfo = "";
    String sbatchCmd = conf.get(
        HPCConfiguration.YARN_APPLICATION_HPC_COMMAND_SLURM_SBATCH,
        HPCConfiguration.DEFAULT_YARN_APPLICATION_HPC_COMMAND_SLURM_SBATCH);
    try {
      String[] command = { sbatchCmd, "--job-name=" + name,
          "--comment=" + metainfo, "--workdir=" + slurmWorkDir,
          "--mem=" + memory, "--cpus-per-task=" + cpus };

      // This creates a parent job slot with a simple script
      // The idea is to have a script that does not take up any computational
      // resources but keeps running until we ask it to stop
      // Send stop signal to self to go into an endless wait
      // the actual yarn task will be added as a job step to this parent job
      String scancelCmd = conf.get(
          HPCConfiguration.YARN_APPLICATION_HPC_COMMAND_SLURM_SCANCEL,
          HPCConfiguration.DEFAULT_YARN_APPLICATION_HPC_COMMAND_SLURM_SCANCEL);
      String script = "#!" + System.getenv("SHELL") + "\n" + scancelCmd
          + " -b -s STOP $SLURM_JOB_ID";
      Shell.ShellCommandExecutor shell = new Shell.ShellCommandExecutor(command);
      StdInputThread exec = new StdInputThread(shell, script);
      exec.start();
      shell.execute();
      exec.checkException();
      String result = shell.getOutput();

      Matcher matcher = Pattern.compile("(\\d+)").matcher(result);
      if (!matcher.find())
        throw new NumberFormatException("Invalid output for: " + result);
      return Integer.parseInt(matcher.group(1));
    } catch (Throwable t) {
      LOG.error("Failed to allocate a container.", t);
      throw new Error("Failed to allocate a container.", t);
    }
  }

  private static String getHPCWorkDir(Configuration conf) {
    String slurmWorkDir = conf.get(
        HPCConfiguration.YARN_APPLICATION_HPC_WORK_DIR,
        HPCConfiguration.DEFAULT_YARN_APPLICATION_HPC_WORK_DIR);
    return slurmWorkDir;
  }

  @SuppressWarnings("deprecation")
  public static void startLogAggregation(ApplicationAttemptId appAttemptId,
      Configuration conf) {
    String slurmWorkDir = getHPCWorkDir(conf);
    String sbatchCmd = conf.get(
        HPCConfiguration.YARN_APPLICATION_HPC_COMMAND_SLURM_SBATCH,
        HPCConfiguration.DEFAULT_YARN_APPLICATION_HPC_COMMAND_SLURM_SBATCH);
    String srunCmd = conf.get(
        HPCConfiguration.YARN_APPLICATION_HPC_COMMAND_SLURM_SRUN,
        HPCConfiguration.DEFAULT_YARN_APPLICATION_HPC_COMMAND_SLURM_SRUN);
   
    Map<String, String> launchEnv = new HashMap<String, String>();
    
    // Hadoop jars
    for (String c : conf.getStrings(
        YarnConfiguration.YARN_APPLICATION_CLASSPATH,
        YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH)) {
      Apps.addToEnvironment(launchEnv, Environment.CLASSPATH.name(), c.trim());
    }
    Map<String, String> environment = setUpEnvironment(launchEnv,
        getHPCLogDir(conf));
    
    try {
      String user = UserGroupInformation.getCurrentUser().getShortUserName();
      String logCmd = System.getenv("JAVA_HOME") + "/bin/java "
          + "org.apache.hadoop.yarn.hpc.log.HPCLogAggregateHandler "
          + appAttemptId.getApplicationId().toString() + " " + user;
      String[] command = { sbatchCmd, "--job-name=HPCLogAggregateHandler",
          "--share", "--workdir=" + slurmWorkDir };

      String script = "#!" + System.getenv("SHELL") + "\n"
          + "nodes=$(sinfo -h -o %D --state=idle,alloc,mixed,future,completing)\n"
          + srunCmd + " --share -N$nodes " + logCmd;
      Shell.ShellCommandExecutor shell = new Shell.ShellCommandExecutor(
          command, new File("."), environment);
      StdInputThread exec = new StdInputThread(shell, script);
      exec.start();
      shell.execute();
      exec.checkException();
      shell.getOutput();
    } catch (Throwable t) {
      throw new YarnRuntimeException("Failed to aggregate logs.", t);
    }
  }

  private static String getHPCLocalDir(String containerIdStr,
      Configuration conf, ApplicationId applicationId) {
    return HPCConfiguration.getHPCLocalDirs(conf)[0] + File.separatorChar
        + applicationId + File.separatorChar + containerIdStr;
  }

  private static String getHPCLogDir(Configuration conf) {
    return HPCConfiguration.getHPCLogDirs(conf)[0];
  }
}