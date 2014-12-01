/*
 * Copyright 2014 Spotify AB. All rights reserved.
 *
 * The contents of this file are licensed under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package com.spotify.hdfs2cass.crunch;

import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.hadoop.conf.Configuration;

public class CrunchConfigHelper {
  public static final String COLUMN_FAMILY_CONFIG = "spotify.cassandra.column.family";

  /**
   * Set the column family for the output of this job.
   * <p>
   * Use this instead of
   * {@link org.apache.cassandra.hadoop.ConfigHelper#setOutputColumnFamily(org.apache.hadoop.conf.Configuration, String)}
   * </p>
   */
  public static void setOutputColumnFamily(Configuration conf, String columnFamily) {
    conf.set(COLUMN_FAMILY_CONFIG, columnFamily);
  }

  /**
   * Set the keyspace and column family for the output of this job.
   * <p>
   * Use this instead of
   * {@link org.apache.cassandra.hadoop.ConfigHelper#setOutputColumnFamily(org.apache.hadoop.conf.Configuration, String, String)}
   * </p>
   */
  public static void setOutputColumnFamily(Configuration conf, String keyspace, String columnFamily) {
    ConfigHelper.setOutputKeyspace(conf, keyspace);
    setOutputColumnFamily(conf, columnFamily);
  }

  public static String getOutputColumnFamily(final Configuration conf) {
    return conf.get(COLUMN_FAMILY_CONFIG);
  }
}
