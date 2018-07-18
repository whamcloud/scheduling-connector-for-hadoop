// Copyright (c) 2018 DDN. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package org.apache.hadoop.yarn.hpc.api;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.yarn.hpc.records.AllocatedContainersInfo;
import org.apache.hadoop.yarn.hpc.records.ApplicationMasterFinishRequest;
import org.apache.hadoop.yarn.hpc.records.ApplicationMasterFinishResponse;
import org.apache.hadoop.yarn.hpc.records.ApplicationMasterRegisterRequest;
import org.apache.hadoop.yarn.hpc.records.ApplicationMasterRegisterResponse;
import org.apache.hadoop.yarn.hpc.records.HPCAllocateRequest;
import org.apache.hadoop.yarn.hpc.records.HPCAllocateResponse;

/**
 * Protocol between Yarn Application Master and HPC Job Scheduler for resource
 * allocation.
 */
@Public
@Evolving
public interface HPCApplicationMaster {

  /**
   * The Application Master registers with the HPC scheduler using this
   * interface.
   * 
   * @param request
   *          contains AM IPC ip, port and tracking url
   * @return response contains HPC capability
   */
  @Public
  ApplicationMasterRegisterResponse registerApplicationMaster(
      ApplicationMasterRegisterRequest request) throws IOException;

  /**
   * The Application Master unregisters with the HPC scheduler using this
   * interface.
   * 
   * @param request
   *          contains final application status, url and diagnostics.
   * @return response contains whether successfully unregistered or not
   */
  @Public
  ApplicationMasterFinishResponse finishApplicationMaster(
      ApplicationMasterFinishRequest request) throws IOException;

  /**
   * The Application Master requests the the HPC scheduler for containers using
   * this interface.
   * 
   * @param request
   *          contains resources required for running tasks to complete
   *          application/job
   * @return response includes allocated containers
   */
  @Public
  HPCAllocateResponse allocate(HPCAllocateRequest request) throws IOException;
  
  /**
   * This interface provides sharing allocated containers information between
   * HPC Application Master and HPC Container Manager protocols.
   * 
   * @param containersInfo
   *          contains allocated containers
   */
  @Public
  void setContainersInfo(AllocatedContainersInfo containersInfo);
}
