// Copyright (c) 2017 Intel Corporation. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package org.apache.hadoop.yarn.hpc.pbs.util;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
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
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.hpc.conf.HPCConfiguration;
import org.apache.hadoop.yarn.hpc.util.HPCCommandExecutor.StdInputThread;
import org.apache.hadoop.yarn.hpc.util.HPCUtils;
import org.apache.hadoop.yarn.util.Apps;
import org.apache.hadoop.yarn.util.ConverterUtils;

public class PBSCommandExecutor {
  private static final Log LOG = LogFactory.getLog(PBSCommandExecutor.class);

  @SuppressWarnings("deprecation")
  public static int submitAndGetPBSJobId(Configuration conf, int priority,
      int memory, int cpuCores, String hostName, int port) {
    String qsubCmd = conf.get(
        HPCConfiguration.YARN_APPLICATION_HPC_COMMAND_PBS_QSUB,
        HPCConfiguration.DEFAULT_YARN_APPLICATION_HPC_COMMAND_PBS_QSUB);
    try {
      Map<String, String> launchEnv = new HashMap<String, String>();

      // Hadoop jars
      for (String c : conf.getStrings(
          YarnConfiguration.YARN_APPLICATION_CLASSPATH,
          YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH)) {
        Apps.addToEnvironment(launchEnv, Environment.CLASSPATH.name(), c.trim());
      }

      Map<String, String> environment = setUpEnvironment(launchEnv);
      
      // specifying memory and cpu for qsub
      String[] command = { qsubCmd };
      if (cpuCores > 0 && memory > 0) {
        command = new String[] { qsubCmd, "-l",
            "select=1:mem=" + memory + "mb:ncpus=" + cpuCores };
      }
      if (memory > 0) {
        command = new String[] { qsubCmd, "-l", "mem=" + memory + "mb" };
      }
      if (cpuCores > 0) {
        command = new String[] { qsubCmd, "-l", "ncpus=" + cpuCores };
      }
      
      StringBuilder scriptBuilder = new StringBuilder();
      scriptBuilder.append("#!").append(System.getenv("SHELL")).append("\n");
      for (Entry<String, String> envPair : environment.entrySet()) {
        scriptBuilder.append("export ").append(envPair.getKey()).append("=")
            .append(envPair.getValue()).append("\n");
      }
      scriptBuilder.append(System.getenv("JAVA_HOME")).append(
          "/bin/java org.apache.hadoop.yarn.hpc.pbs.util.ContainerLauncher $PBS_JOBID "
              + hostName + " " + port + " " + priority + " " + memory + " "
              + cpuCores);
      LOG.debug("Script for creating PBS Job : \n" + scriptBuilder.toString());
      Shell.ShellCommandExecutor shell = new Shell.ShellCommandExecutor(
          command, new File("."), environment);
      StdInputThread exec = new StdInputThread(shell, scriptBuilder.toString());
      exec.start();
      shell.execute();
      exec.checkException();
      String result = shell.getOutput();
      LOG.debug("Created PBS Job with ID : " + result);
      Matcher matcher = Pattern.compile("(\\d+)").matcher(result);
      if (!matcher.find())
        throw new NumberFormatException("Invalid output for: " + result);
      int jobId = Integer.parseInt(matcher.group(1));
      return jobId;
    } catch (Throwable t) {
      LOG.error("Failed to allocate a container.", t);
      throw new Error("Failed to allocate a container.", t);
    }
  }

  public static void launchContainer(ContainerLaunchContext amContainerSpec,
      String containerIdStr, String appName, Configuration conf, int jobId,
      boolean masterContainer, String containerHostName) throws IOException {
    ContainerId containerId = ConverterUtils.toContainerId(containerIdStr);
    ApplicationId applicationId = containerId.getApplicationAttemptId().getApplicationId();
    String hpcLogDir = getHPCLogDir(conf);
       
    String containerLogsDir = hpcLogDir + File.separatorChar + applicationId
        + File.separatorChar + containerIdStr;
    String workDir = getHPCLocalDir(containerIdStr, conf, applicationId);

    Map<String, String> environment = setUpEnvironment(
        amContainerSpec.getEnvironment());
    
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

    // Exporting Environment variables
    for (Entry<String, String> envPair : environment.entrySet()) {
      String key = envPair.getKey();
      if(key.startsWith("-D") == false){
        String value = envPair.getValue().replace("<LOG_DIR>",
            containerLogsDir);
        stream.println("export " + key + "=\"" + value +"\"");
      }
    }
    
    stream.println("export NM_HOST=" + containerHostName);
    
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
    
    // Command for localization
    stream.println(System.getenv("JAVA_HOME") + "/bin/java "
        + "org.apache.hadoop.yarn.hpc.localization.AppLocalizer "
        + localizationArguments.toString());
    
    // Copy the job_tokens file to pdgfEnvironment.tar/data-generator/
    stream.println("if [ -d \"pdgfEnvironment.tar/data-generator/\" ]; then");
    stream.println("  if [ -f \"job_tokens\" ]; then");
    stream.println("    cp job_tokens pdgfEnvironment.tar/data-generator/");
    stream.println("  fi");
    stream.println("fi");
    
    // Command execution
    stream.println(finalCommand);
    stream.close();

    String sciptForLaunch = script.toString();
 
    sciptForLaunch = sciptForLaunch.replace("${yarn.log.dir}",
        environment.get("HADOOP_COMMON_HOME") + "/logs");
    
    LOG.debug("Script for launching container : \n" + sciptForLaunch);
    if (masterContainer) {
      SocketWrapper socket = SocketCache.getSocket(jobId);
      socket.setChildCommand(sciptForLaunch);
    } else {
      ContainerResponse response = ContainerResponses.getResponse(jobId);
      response.writeChildCommand(sciptForLaunch);
    }
  }
  

  private static Map<String, String> setUpEnvironment(
      Map<String, String> launchEnv) {
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
        if (envVar == null) {
          continue;
        }
        value = value.replace(var, envVar);
      }
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
  
  private static String getHPCLogDir(Configuration conf) {
    return HPCConfiguration.getHPCLogDirs(conf)[0];
  }
  
  private static String getHPCLocalDir(String containerIdStr,
      Configuration conf, ApplicationId applicationId) {
    return HPCConfiguration.getHPCLocalDirs(conf)[0] + File.separatorChar
        + applicationId + File.separatorChar + containerIdStr;
  }
}