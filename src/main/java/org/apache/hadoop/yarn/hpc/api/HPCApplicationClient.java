// Copyright (c) 2018 DDN. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package org.apache.hadoop.yarn.hpc.api;

import java.io.IOException;
import java.util.EnumSet;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.YarnClusterMetrics;
import org.apache.hadoop.yarn.hpc.records.NewApplicationResponse;

/**
 * Protocol between Yarn Application client and HPC Job Scheduler for Job submit
 * and statistics querying.
 */
@Public
@Evolving
public interface HPCApplicationClient {

  /**
   * Yarn HPC client gets the new application/job id using this interface.
   * 
   * @return response includes application id
   */
  @Public
  NewApplicationResponse getNewApplication() throws IOException;

  /**
   * Yarn HPC client submits the new application/job using this interface.
   * 
   * @param context
   *          Context information for starting the Job
   */
  @Public
  void submitApplication(ApplicationSubmissionContext context) throws IOException;

  /**
   * Yarn HPC client gets the application report using this interface.
   * 
   * @param applicationId
   * @return application report
   */
  @Public
  ApplicationReport getApplicationReport(ApplicationId applicationId) throws IOException;

  /**
   * Yarn HPC client gets all applications report using this interface.
   * 
   * @return all applications report
   */
  @Public
  List<ApplicationReport> getApplications() throws IOException;

  /**
   * Yarn HPC client stops/kills a running application using this interface.
   * 
   * @param applicationId
   * @return whether successfully killed or not
   */
  @Public
  boolean forceKillApplication(ApplicationId applicationId) throws IOException;

  /**
   * Yarn HPC client gets the cluster metrics using this interface.
   * 
   * @return cluster metrics
   */
  @Public
  YarnClusterMetrics getClusterMetrics() throws IOException;

  /**
   * Yarn HPC client gets the cluster nodes using this interface.
   * 
   * @param states
   * @return All the cluster nodes reports
   */
  @Public
  List<NodeReport> getClusterNodes(EnumSet<NodeState> states) throws IOException;
}
