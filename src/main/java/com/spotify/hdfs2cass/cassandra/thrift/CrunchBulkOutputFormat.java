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
 * https://github.com/apache/cassandra/blob/trunk/src/java/org/apache/cassandra/hadoop/BulkOutputFormat.java
 */
package com.spotify.hdfs2cass.cassandra.thrift;

import org.apache.cassandra.hadoop.AbstractBulkOutputFormat;
import org.apache.cassandra.thrift.Mutation;
import org.apache.crunch.CrunchRuntimeException;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * This is an almost-copy of {@link org.apache.cassandra.hadoop.BulkOutputFormat}
 * <p>
 * We had to re-implement this class (and its inner private classes) because of clash between
 * Cassandra's and Crunch's MapReduce configs.
 * See https://issues.apache.org/jira/browse/CASSANDRA-8367 for more info.
 *
 * This is a temporary workaround and will be removed in the future.
 *
 * We return {@link com.spotify.hdfs2cass.cassandra.cql.CrunchCqlBulkRecordWriter}.
 * </p>
 */
public class CrunchBulkOutputFormat extends AbstractBulkOutputFormat<ByteBuffer, List<Mutation>> {
  private final Logger logger = LoggerFactory.getLogger(CrunchBulkOutputFormat.class);

  /**
   * Not used anyway, so do not bother implementing.
   */
  @Deprecated
  public CrunchBulkRecordWriter getRecordWriter(org.apache.hadoop.fs.FileSystem filesystem, org.apache.hadoop.mapred.JobConf job, String name, org.apache.hadoop.util.Progressable progress) {
    throw new CrunchRuntimeException("Use getRecordWriter(org.apache.hadoop.mapreduce.TaskAttemptContext)");
  }

  @Override
  public CrunchBulkRecordWriter getRecordWriter(final TaskAttemptContext context) {
    return new CrunchBulkRecordWriter(context);
  }
}
