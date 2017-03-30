// Copyright (c) 2017 Intel Corporation. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package org.apache.hadoop.yarn.hpc.pbs.util;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class SocketCache {
  private static Map<Integer, SocketWrapper> socketCache = new ConcurrentHashMap<>();

  /**
   * Adding socket into the Socket Cache
   * 
   * @param jobId
   * @param socket
   */
  public static void addSocket(Integer jobId, SocketWrapper socket) {
    socketCache.put(jobId, socket);
  }

  /**
   * Remove Socket from the socket cache
   * 
   * @param jobId
   */
  public static void removeSocket(Integer jobId) {
    socketCache.remove(jobId);
  }

  /**
   * Return SocketWrapper mapped to the jobId
   * 
   * @param jobId
   * @return
   */
  public static SocketWrapper getSocket(Integer jobId) {
    return socketCache.get(jobId);
  }

  /**
   * @return 
   */
  public static Set<Entry<Integer, SocketWrapper>> getSocketEntry() {
    return socketCache.entrySet();
  }
}
