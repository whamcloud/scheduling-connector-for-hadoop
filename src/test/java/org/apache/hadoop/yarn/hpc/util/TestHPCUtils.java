// Copyright (c) 2017 DDN. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package org.apache.hadoop.yarn.hpc.util;

import org.junit.Assert;
import org.junit.Test;

public class TestHPCUtils {

  @Test
  public void testParseHostList() {
    String[] hostList = HPCUtils.parseHostList("ham[2-4]");
    Assert.assertEquals("ham2", hostList[0]);
    Assert.assertEquals("ham3", hostList[1]);
    Assert.assertEquals("ham4", hostList[2]);
  }
}
