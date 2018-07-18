// Copyright (c) 2018 DDN. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package org.apache.hadoop.yarn.hpc.util;

import java.util.Vector;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class HPCUtils {

	public static long getClusterTimestamp() {
		return 1234567890L;
	}

	public static String[] parseHostList(String hosts) {

		Matcher m = Pattern.compile("([^,\\[]+)(\\[(([^\\]])*)\\])?").matcher(
				hosts);
		Vector<String> list = new Vector<String>();

		while (m.find()) {
			String prefix = m.group(1);
			String range = m.group(3);
			if (range == null) {
				list.add(prefix);
				continue;
			}
			for (String x : range.split(",")) {
				String[] y = x.split("-");
				int start = Integer.parseInt(y[0]);
				int end = y.length == 1 ? start : Integer.parseInt(y[1]);
				for (int i = start; i <= end; i++)
					list.add(prefix + i);
			}
		}
		return list.toArray(new String[0]);
	}
}
