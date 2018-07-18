// Copyright (c) 2017 DDN. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package org.apache.hadoop.yarn.hpc.api;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerRequest;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.hpc.records.AllocatedContainersInfo;
import org.apache.hadoop.yarn.hpc.records.ContainersResponse;
import org.apache.hadoop.yarn.hpc.records.ContainersStatusResponse;

/**
 * Protocol between Yarn Application Master and HPC Job Scheduler for managing
 * the child containers/tasks.
 */
@Public
@Evolving
public interface HPCContainerManager {

  /**
   * The Application Master requests the HPC scheduler to start the
   * container/tasks using this interface.
   * 
   * @param requests
   *          List of container start requests
   * @return response includes succeeded and failed start container requests
   */
  @Public
  ContainersResponse startContainers(List<StartContainerRequest> requests) throws IOException;

  /**
   * The Application Master requests the HPC scheduler to stop the
   * container/tasks using this interface.
   * 
   * @param containerIds
   *          List of container id's for stopping the containers
   * @return response includes succeeded and failed stop container requests
   */
  @Public
  ContainersResponse stopContainers(List<ContainerId> containerIds) throws IOException;

  /**
   * The Application Master requests the HPC scheduler to get the
   * container/tasks status using this interface.
   * 
   * @param containerIds
   *          List of container id's for getting the container statuses
   * @return response includes container statuses for the list of container id's
   */
  @Public
  ContainersStatusResponse getContainerStatuses(List<ContainerId> containerIds) throws IOException;
  
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
