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
package com.spotify.hdfs2cass.crunch.cql;

import com.google.common.collect.Maps;
import com.spotify.hdfs2cass.cassandra.cql.CrunchCqlBulkOutputFormat;
import com.spotify.hdfs2cass.crunch.CrunchConfigHelper;
import com.spotify.hdfs2cass.cassandra.utils.CassandraParams;
import org.apache.crunch.CrunchRuntimeException;
import org.apache.crunch.SourceTarget;
import org.apache.crunch.Target;
import org.apache.crunch.io.CrunchOutputs;
import org.apache.crunch.io.FormatBundle;
import org.apache.crunch.io.MapReduceTarget;
import org.apache.crunch.io.OutputHandler;
import org.apache.crunch.types.Converter;
import org.apache.crunch.types.PTableType;
import org.apache.crunch.types.PType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.Serializable;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Responsible for configuring the MapReduce job to use CQL version of bulk output format.
 */
public class CQLTarget implements MapReduceTarget, Serializable {
  private Map<String, String> extraConf = Maps.newHashMap();

  private URI resource;
  private final CassandraParams params;

  public CQLTarget(final URI resource, final CassandraParams params) {
    this.resource = resource;
    this.params = params;
  }

  @Override
  public void configureForMapReduce(final Job job, final PType<?> pType, final Path outputPath, final String name) {

    if (name == null) {
      throw new CrunchRuntimeException("'name' arguments should not be null. We don't know why tho");
    }

    FileOutputFormat.setOutputPath(job, outputPath);
    job.setOutputFormatClass(CrunchCqlBulkOutputFormat.class);

    JobConf conf = new JobConf();
    params.configure(conf);

    for (Map.Entry<String, String> e : extraConf.entrySet()) {
      conf.set(e.getKey(), e.getValue());
    }

    FormatBundle<CrunchCqlBulkOutputFormat> bundle = FormatBundle.forOutput(CrunchCqlBulkOutputFormat.class);
    for (Map.Entry<String, String> e : conf) {
      bundle.set(e.getKey(), e.getValue());
    }

    Configuration jobConfiguration = job.getConfiguration();

    // we don't know why exactly this is needed, but without this, the actual streaming will not
    // see the the throttling and buffer size arguments
    params.configure(jobConfiguration);

    CrunchConfigHelper.setOutputColumnFamily(jobConfiguration, params.getKeyspace(),
        params.getColumnFamily());
    CrunchCqlBulkOutputFormat.setColumnFamilySchema(jobConfiguration, params.getColumnFamily(),
        params.getSchema());
    CrunchCqlBulkOutputFormat.setColumnFamilyInsertStatement(jobConfiguration,
        params.getColumnFamily(), params.getStatement());

    String[] colNames = params.getColumnNames();
    for(int i=0; i< colNames.length; i++) {
      CrunchCqlBulkOutputFormat.setColumnIndex(jobConfiguration, params.getColumnFamily(), colNames[i], i);
    }

    CrunchOutputs.addNamedOutput(job, name, bundle, ByteBuffer.class, List.class);
  }

  @Override
  public Target outputConf(final String key, final String value) {
    extraConf.put(key, value);
    return this;
  }

  @Override
  public boolean handleExisting(final WriteMode writeMode, final long lastModifiedAt, final Configuration conf) {
    return false;
  }

  @Override
  public boolean accept(final OutputHandler handler, final PType<?> pType) {
    if (pType instanceof PTableType) {
      PTableType pTableType = (PTableType) pType;
      PType<?> keyType = pTableType.getKeyType();
      PType<?> valueType = pTableType.getValueType();
      List<PType> subTypes = valueType.getSubTypes();

      if (ByteBuffer.class.equals(keyType.getTypeClass())
          && Collection.class.equals(valueType.getTypeClass())
          && subTypes.size() == 1
          && ByteBuffer.class.equals(subTypes.get(0).getTypeClass())) {
        handler.configure(this, pType);
        return true;
      }
    }
    return false;
  }

  @Override
  public Converter<?, ?, ?, ?> getConverter(final PType<?> pType) {
    return new CQLConverter();
  }

  @Override
  public <T> SourceTarget<T> asSourceTarget(final PType<T> pType) {
    return null;
  }

  @Override
  public String toString() {
    return resource.toString();
  }


}
