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
 * https://github.com/apache/cassandra/blob/trunk/src/java/org/apache/cassandra/hadoop/AbstractBulkRecordWriter.java
 */
package com.spotify.hdfs2cass.cassandra.thrift;

import com.google.common.util.concurrent.Uninterruptibles;
import com.spotify.hdfs2cass.crunch.CrunchConfigHelper;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.cassandra.hadoop.HadoopCompat;
import org.apache.cassandra.io.sstable.SSTableLoader;
import org.apache.cassandra.io.sstable.SSTableSimpleUnsortedWriter;
import org.apache.cassandra.streaming.StreamState;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.CounterColumn;
import org.apache.cassandra.thrift.Mutation;
import org.apache.cassandra.utils.OutputHandler;
import org.apache.crunch.CrunchRuntimeException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * This is an almost-copy of {@link org.apache.cassandra.hadoop.BulkRecordWriter}.
 * <p>
 * We had to re-implement this class because of https://issues.apache.org/jira/browse/CASSANDRA-8367
 * </p>
 */
public class CrunchBulkRecordWriter
    extends RecordWriter<ByteBuffer, List<Mutation>> implements
    org.apache.hadoop.mapred.RecordWriter<ByteBuffer, List<Mutation>> {

  private final static Logger logger = LoggerFactory.getLogger(CrunchBulkRecordWriter.class);

  private final static String OUTPUT_LOCATION = "mapreduce.output.bulkoutputformat.localdir";
  private final static String BUFFER_SIZE_IN_MB = "mapreduce.output.bulkoutputformat.buffersize";
  private final static String STREAM_THROTTLE_MBITS = "mapreduce.output.bulkoutputformat.streamthrottlembits";
  private final static String MAX_FAILED_HOSTS = "mapreduce.output.bulkoutputformat.maxfailedhosts";

  private final Configuration conf;
  private SSTableSimpleUnsortedWriter writer;
  private SSTableLoader loader;
  private File outputdir;
  private TaskAttemptContext context;

  private enum CFType {
    NORMAL, SUPER
  }

  private enum ColType {
    NORMAL, COUNTER
  }

  private CFType cfType;
  private ColType colType;

  public CrunchBulkRecordWriter(TaskAttemptContext context) {
    Config.setClientMode(true);
    Config.setOutboundBindAny(true);
    this.conf = HadoopCompat.getConfiguration(context);
    this.context = context;
    int megabitsPerSec = Integer.parseInt(conf.get(STREAM_THROTTLE_MBITS, "0"));
    DatabaseDescriptor.setStreamThroughputOutboundMegabitsPerSec(megabitsPerSec);
  }

  private String getOutputLocation() {
    String dir = conf.get(OUTPUT_LOCATION, System.getProperty("java.io.tmpdir"));
    if (dir == null) {
      throw new CrunchRuntimeException(
          "Output directory not defined, if hadoop is not setting java.io.tmpdir then define "
              + OUTPUT_LOCATION);
    }
    return dir;
  }

  private void setTypes(Mutation mutation) {
    if (cfType == null) {
      if (mutation.getColumn_or_supercolumn().isSetSuper_column()
          || mutation.getColumn_or_supercolumn().isSetCounter_super_column())
        cfType = CFType.SUPER;
      else
        cfType = CFType.NORMAL;
      if (mutation.getColumn_or_supercolumn().isSetCounter_column()
          || mutation.getColumn_or_supercolumn().isSetCounter_super_column())
        colType = ColType.COUNTER;
      else
        colType = ColType.NORMAL;
    }
  }

  private void prepareWriter() {
    String columnFamily = CrunchConfigHelper.getOutputColumnFamily(conf);
    String keyspace = ConfigHelper.getOutputKeyspace(conf);

    if (outputdir == null) {
      // dir must be named by ks/cf for the loader
      outputdir = Paths.get(getOutputLocation(), keyspace, columnFamily).toFile();
      outputdir.mkdirs();
    }

    if (writer == null) {
      AbstractType<?> subcomparator = null;

      if (cfType == CFType.SUPER)
        subcomparator = BytesType.instance;

      int bufferSizeInMB = Integer.parseInt(conf.get(BUFFER_SIZE_IN_MB, "64"));
      this.writer = new SSTableSimpleUnsortedWriter(
          outputdir, ConfigHelper.getOutputPartitioner(conf),
          keyspace, columnFamily,
          BytesType.instance, subcomparator,
          bufferSizeInMB,
          ConfigHelper.getOutputCompressionParamaters(conf));

      ExternalSSTableLoaderClient externalClient = new ExternalSSTableLoaderClient(
          ConfigHelper.getOutputInitialAddress(conf),
          ConfigHelper.getOutputRpcPort(conf),
          ConfigHelper.getOutputKeyspaceUserName(conf),
          ConfigHelper.getOutputKeyspacePassword(conf));

      this.loader = new SSTableLoader(outputdir, externalClient, new OutputHandler.SystemOutput(true, true));
    }
  }

  @Override
  public void write(ByteBuffer keybuff, List<Mutation> value) throws IOException {
    ProgressHeartbeat heartbeat = new ProgressHeartbeat(context, 120);
    heartbeat.startHeartbeat();
    try {
      setTypes(value.get(0));
      prepareWriter();
      writer.newRow(keybuff);
      for (Mutation mut : value) {
        if (cfType == CFType.SUPER) {
          writer.newSuperColumn(mut.getColumn_or_supercolumn().getSuper_column().name);
          if (colType == ColType.COUNTER)
            for (CounterColumn column : mut.getColumn_or_supercolumn().getCounter_super_column().columns)
              writer.addCounterColumn(column.name, column.value);
          else {
            for (Column column : mut.getColumn_or_supercolumn().getSuper_column().columns) {
              if (column.ttl == 0)
                writer.addColumn(column.name, column.value, column.timestamp);
              else
                writer.addExpiringColumn(column.name, column.value, column.timestamp, column.ttl,
                    System.currentTimeMillis() + ((long) column.ttl * 1000));
            }
          }
        } else {
          if (colType == ColType.COUNTER) {
            writer.addCounterColumn(mut.getColumn_or_supercolumn().counter_column.name,
                mut.getColumn_or_supercolumn().counter_column.value);
          } else {
            if (mut.getColumn_or_supercolumn().column.ttl == 0) {
              writer.addColumn(mut.getColumn_or_supercolumn().column.name,
                  mut.getColumn_or_supercolumn().column.value,
                  mut.getColumn_or_supercolumn().column.timestamp);
            } else {
              writer.addExpiringColumn(mut.getColumn_or_supercolumn().column.name,
                  mut.getColumn_or_supercolumn().column.value,
                  mut.getColumn_or_supercolumn().column.timestamp,
                  mut.getColumn_or_supercolumn().column.ttl, System.currentTimeMillis()
                      + ((long) (mut.getColumn_or_supercolumn().column.ttl) * 1000));
            }
          }
        }
      }
    } finally {
      heartbeat.stopHeartbeat();
    }
  }

  @Override
  public void close(TaskAttemptContext context) throws IOException, InterruptedException {
    close();
  }

  /**
   * Fills the deprecated RecordWriter interface for streaming.
   */
  @Deprecated
  public void close(org.apache.hadoop.mapred.Reporter reporter) throws IOException {
    close();
  }

  private void close() throws IOException {
    logger.info("Closing bulk record writer");
    ProgressHeartbeat heartbeat = new ProgressHeartbeat(context, 120);
    heartbeat.startHeartbeat();
    try {
      if (writer != null) {
        writer.close();
        Future<StreamState> future = loader.stream(Collections.<InetAddress>emptySet(), new ProgressIndicator());
        try {
          Uninterruptibles.getUninterruptibly(future);
        } catch (ExecutionException e) {
          throw new RuntimeException("Streaming to the following hosts failed: " + loader.getFailedHosts(), e);
        }
      }
    } finally {
      heartbeat.stopHeartbeat();
    }
    logger.info("Succesfully closed bulk record writer");
  }
}
