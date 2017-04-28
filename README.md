// Copyright (c) 2017 Intel Corporation. All rights reserved.

// Use of this source code is governed by a MIT-style

// license that can be found in the LICENSE file.

# HPC Adapter for Mapreduce/Yarn(HAM)

Apache Hadoop is architected for storage and large scale processing of data-sets on clusters of commodity hardware. Apache Hadoop daemons should be up and running if users want to get any service from Apache Hadoop. If users want to run Mapreduce (M/R) Jobs/Yarn applications then they should have running Job Tracker (in Hadoop1) or Resource Manager (in Hadoop 2). HPC Adapter for Mapreduce/Yarn(HAM) gives provision to run Apache Hadoop M/R Jobs and Yarn applications in existing HPC Job scheduling frameworks like Slurm, PBS Pro, etc. without having Apache Hadoop daemons running.


## Build and Install instructions

Requirements:

* JDK 1.7+
* Maven 3.0 or later
* Internet connection for first build (to fetch all Maven and Hadoop dependencies)

Maven build goals:

 * Clean				: mvn clean
 * Compile				: mvn compile
 * Run tests				: mvn test
 * Generate Eclipse files		: mvn eclipse:eclipse
 * Create JAR/Install JAR in M2 cache   : mvn install

Installation:

After the 'mvn install', we can find the generated jar file with the name 'ham-*-ieel-*.jar' and copy the jar file to the location which is available to Apache Hadoop application classpath.

## Configurations

The property 'yarn.ipc.rpc.class' is the main configuration which decides to submit Jobs to HPC scheduler when the value is 'org.apache.hadoop.yarn.hpc.HadoopYarnHPCRPC'. If this property is not configured then all the remaining configurations in Slurm Scheduler and PBS Pro Scheduler will not be applicable. These need to be configured in yarn-site.xml.

```
<property>
	<description>RPC class implementation</description>
	<name>yarn.ipc.rpc.class</name>
	<value>org.apache.hadoop.yarn.hpc.HadoopYarnHPCRPC</value>
</property>
```

### Slurm Scheduler

```
  <property>
	<description>HPC Application Client class implementation</description>
	<name>yarn.application.hpc.client.class</name>
	<value>org.apache.hadoop.yarn.hpc.slurm.SlurmApplicationClient</value>
  </property>
	
  <property>
	<description>HPC Application Master class implementation</description>
	<name>yarn.application.hpc.applicationmaster.class</name>
	<value>org.apache.hadoop.yarn.hpc.slurm.SlurmApplicationMaster</value>
  </property>
	
  <property>
	<description>HPC Application Container Manager class implementation
	</description>
	<name>yarn.application.hpc.containermanager.class</name>
	<value>org.apache.hadoop.yarn.hpc.slurm.SlurmContainerManager</value>
  </property>
```

### PBS Pro Scheduler

```
  <property>
	<description>HPC Application Client class implementation</description>
	<name>yarn.application.hpc.client.class</name>
	<value>org.apache.hadoop.yarn.hpc.pbs.PBSApplicationClient</value>
  </property>
	
  <property>
	<description>HPC Application Master class implementation</description>
	<name>yarn.application.hpc.applicationmaster.class</name>
	<value>org.apache.hadoop.yarn.hpc.pbs.PBSApplicationMaster</value>
  </property>
	
  <property>
	<description>HPC Application Container Manager class implementation
	</description>
	<name>yarn.application.hpc.containermanager.class</name>
	<value>org.apache.hadoop.yarn.hpc.pbs.PBSContainerManager</value>
  </property>
```

### M/R Shuffle

Configure the below configurations in mapred-site.xml file for handling the M/R shuffle.

```
<property>
	<name>mapreduce.job.map.output.collector.class</name>
	<value>org.apache.hadoop.mapred.SharedFsPlugins$MapOutputBuffer</value>
</property>

<property>
	<name>mapreduce.job.reduce.shuffle.consumer.plugin.class</name>
	<value>org.apache.hadoop.mapred.SharedFsPlugins$Shuffle</value>
</property>
```

### Running Apache Hadoop Applications

Run the Apache Hadoop Applications with the above configurations, all the tasks will run with the configured HPC Scheduler.

### References

Kavali, Devaraj. "Hadoop Applications on High Performance Computing." http://events.linuxfoundation.org/sites/events/files/slides/ApacheCon2015-Devaraj-Kavali.pdf 1 (2015).
Kavali, Devaraj. "Hadoop Applications on High Performance Computing." ApacheCon2015 1.1 (2015): 31.