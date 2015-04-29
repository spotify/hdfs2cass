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
 * https://github.com/apache/cassandra/blob/trunk/src/java/org/apache/cassandra/hadoop/cql3/CqlBulkRecordWriter.java
 */
package com.spotify.hdfs2cass.cassandra.cql;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Uninterruptibles;
import com.spotify.hdfs2cass.cassandra.thrift.ProgressHeartbeat;
import com.spotify.hdfs2cass.cassandra.thrift.ProgressIndicator;
import com.spotify.hdfs2cass.crunch.CrunchConfigHelper;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.hadoop.AbstractBulkRecordWriter;
import org.apache.cassandra.hadoop.BulkRecordWriter;
import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.cassandra.hadoop.HadoopCompat;
import org.apache.cassandra.io.sstable.CQLSSTableWriter;
import org.apache.cassandra.io.sstable.SSTableLoader;
import org.apache.cassandra.streaming.StreamState;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * This is an almost-copy of {@link org.apache.cassandra.hadoop.cql3.CqlBulkRecordWriter}
 * <p>
 * We had to re-implement this class because of https://issues.apache.org/jira/browse/CASSANDRA-8367
 * </p>
 */
public class CrunchCqlBulkRecordWriter extends AbstractBulkRecordWriter<Object, List<ByteBuffer>> {

  private static final Logger LOG = LoggerFactory.getLogger(CrunchCqlBulkRecordWriter.class);

  private String keyspace;
  private final ProgressHeartbeat heartbeat;

  private String columnFamily;
  private String schema;
  private String insertStatement;
  private File outputDir;

  public CrunchCqlBulkRecordWriter(TaskAttemptContext context) throws IOException {
    super(context);
    setConfigs();
    heartbeat = new ProgressHeartbeat(context, 120);
  }

  private void setConfigs() throws IOException
  {
    // if anything is missing, exceptions will be thrown here, instead of on write()
    keyspace = ConfigHelper.getOutputKeyspace(conf);
    columnFamily = CrunchConfigHelper.getOutputColumnFamily(conf);
    schema = CrunchCqlBulkOutputFormat.getColumnFamilySchema(conf, columnFamily);
    insertStatement = CrunchCqlBulkOutputFormat.getColumnFamilyInsertStatement(conf, columnFamily);
    outputDir = getColumnFamilyDirectory();
  }

  private void prepareWriter() throws IOException {
    try {
      if (writer == null) {
        writer = CQLSSTableWriter.builder()
            .forTable(schema)
            .using(insertStatement)
            .withPartitioner(ConfigHelper.getOutputPartitioner(conf))
            .inDirectory(outputDir)
            .withBufferSizeInMB(Integer.parseInt(conf.get(BUFFER_SIZE_IN_MB, "64")))
            .build();
      }
      if (loader == null) {
        CrunchExternalClient externalClient = new CrunchExternalClient(conf);
        externalClient.addKnownCfs(keyspace, schema);
        this.loader = new SSTableLoader(outputDir, externalClient,
            new BulkRecordWriter.NullOutputHandler());
      }
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public void write(Object key, List<ByteBuffer> values) throws IOException {
    prepareWriter();
    // To ensure Crunch doesn't reuse CQLSSTableWriter's objects
    List<ByteBuffer> bb = Lists.newArrayList();
    for (ByteBuffer v : values) {
      bb.add(ByteBufferUtil.clone(v));
    }
    values = bb;
    try {
      ((CQLSSTableWriter) writer).rawAddRow(values);
      if (null != progress)
        progress.progress();
      if (null != context)
        HadoopCompat.progress(context);
    } catch (InvalidRequestException e) {
      throw new IOException("Error adding row with key: " + key, e);
    }
  }

  private File getColumnFamilyDirectory() throws IOException {
    File dir = new File(String.format("%s%s%s%s%s",
        getOutputLocation(), File.separator, keyspace, File.separator, columnFamily));
    if (!dir.exists() && !dir.mkdirs()) {
      throw new IOException("Failed to created output directory: " + dir);
    }
    return dir;
  }

  @Override
  public void close(TaskAttemptContext context) throws IOException, InterruptedException {
    close();
  }

  @Deprecated
  public void close(org.apache.hadoop.mapred.Reporter reporter) throws IOException {
    close();
  }

  private void close() throws IOException {
    LOG.info("SSTables built. Now starting streaming");
    heartbeat.startHeartbeat();
    try {
      if (writer != null) {
        writer.close();
        Future<StreamState> future =
            loader.stream(Collections.<InetAddress>emptySet(), new ProgressIndicator());
        try {
          StreamState streamState = Uninterruptibles.getUninterruptibly(future);
          if (streamState.hasFailedSession()) {
            LOG.warn("Some streaming sessions failed");
          } else {
            LOG.info("Streaming finished successfully");
          }
        } catch (ExecutionException e) {
          throw new RuntimeException("Streaming to the following hosts failed: " +
              loader.getFailedHosts(), e);
        }
      } else {
        LOG.info("SSTableWriter wasn't instantiated, no streaming happened.");
      }
    } finally {
      heartbeat.stopHeartbeat();
    }
  }
}
