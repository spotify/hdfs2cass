/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * The modifications to the upstream file is Copyright 2014 Spotify AB.
 * The original upstream file can be found at
 * https://github.com/apache/cassandra/blob/trunk/src/java/org/apache/cassandra/hadoop/cql3/CqlBulkOutputFormat.java
 */
package com.spotify.hdfs2cass.cassandra.cql;

import org.apache.cassandra.hadoop.AbstractBulkOutputFormat;
import org.apache.crunch.CrunchRuntimeException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.Progressable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * This is an almost-copy of {@link org.apache.cassandra.hadoop.cql3.CqlBulkOutputFormat}
 * <p>
 * We return {@link com.spotify.hdfs2cass.cassandra.cql.CrunchCqlBulkRecordWriter}
 * with our improvements and resolving conflicts with Crunch. This issue is tracked in
 * https://issues.apache.org/jira/browse/CASSANDRA-8367
 * </p>
 */
public class CrunchCqlBulkOutputFormat extends AbstractBulkOutputFormat<Object, List<ByteBuffer>> {

  private static final String OUTPUT_CQL_SCHEMA_PREFIX = "cassandra.columnfamily.schema.";
  private static final String OUTPUT_CQL_INSERT_PREFIX = "cassandra.columnfamily.insert.";

  /**
   * Not used anyway, so do not bother implementing.
   */
  @Deprecated
  public CrunchCqlBulkRecordWriter getRecordWriter(FileSystem filesystem, JobConf job, String name, Progressable progress) throws IOException {
    throw new CrunchRuntimeException("Use getRecordWriter(org.apache.hadoop.mapreduce.TaskAttemptContext)");
  }

  public CrunchCqlBulkRecordWriter getRecordWriter(final TaskAttemptContext context) throws IOException, InterruptedException {
    return new CrunchCqlBulkRecordWriter(context);
  }

  public static void setColumnFamilySchema(Configuration conf, String columnFamily, String schema) {
    conf.set(OUTPUT_CQL_SCHEMA_PREFIX + columnFamily, schema);
  }

  public static void setColumnFamilyInsertStatement(Configuration conf, String columnFamily, String insertStatement) {
    conf.set(OUTPUT_CQL_INSERT_PREFIX + columnFamily, insertStatement);
  }

  public static String getColumnFamilySchema(Configuration conf, String columnFamily) {
    String schema = conf.get(OUTPUT_CQL_SCHEMA_PREFIX + columnFamily);
    if (schema == null) {
      throw new UnsupportedOperationException("You must set the ColumnFamily schema using setColumnFamilySchema.");
    }
    return schema;
  }

  public static String getColumnFamilyInsertStatement(Configuration conf, String columnFamily) {
    String insert = conf.get(OUTPUT_CQL_INSERT_PREFIX + columnFamily);
    if (insert == null) {
      throw new UnsupportedOperationException("You must set the ColumnFamily insert statement using setColumnFamilySchema.");
    }
    return insert;
  }

}
