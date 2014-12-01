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
package com.spotify.hdfs2cass.crunch.thrift;

import com.google.common.collect.Maps;
import com.spotify.hdfs2cass.cassandra.thrift.CrunchBulkOutputFormat;
import com.spotify.hdfs2cass.crunch.CrunchConfigHelper;
import com.spotify.hdfs2cass.cassandra.utils.CassandraParams;
import org.apache.cassandra.thrift.Mutation;
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
 * Responsible for configuring the MapReduce job to use Thrift version of bulk output format.
 */
public class ThriftTarget implements MapReduceTarget, Serializable {
  private Map<String, String> extraConf = Maps.newHashMap();

  private URI resource;
  private final CassandraParams params;

  public ThriftTarget(final URI resource, final CassandraParams params) {
    this.resource = resource;
    this.params = params;
  }

  @Override
  public void configureForMapReduce(final Job job, final PType<?> pType, final Path outputPath, final String name) {
    FileOutputFormat.setOutputPath(job, outputPath);

    job.setOutputFormatClass(CrunchBulkOutputFormat.class);

    if (name == null) {

      JobConf conf = (JobConf) job.getConfiguration();
      for (Map.Entry<String, String> e : extraConf.entrySet()) {
        conf.set(e.getKey(), e.getValue());
      }
      params.configure(conf);

    } else {

      JobConf conf = new JobConf();
      params.configure(conf);

      for (Map.Entry<String, String> e : extraConf.entrySet()) {
        conf.set(e.getKey(), e.getValue());
      }

      FormatBundle<CrunchBulkOutputFormat> bundle = FormatBundle.forOutput(CrunchBulkOutputFormat.class);
      for (Map.Entry<String, String> e : conf) {
        bundle.set(e.getKey(), e.getValue());
      }

      Configuration jobConfiguration = job.getConfiguration();
      CrunchConfigHelper
          .setOutputColumnFamily(jobConfiguration, params.getKeyspace(), params.getColumnFamily());

      CrunchOutputs.addNamedOutput(job, name, bundle, ByteBuffer.class, List.class);

    }
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
          && Mutation.class.equals(subTypes.get(0).getTypeClass())) {
        handler.configure(this, pType);
        return true;
      }
    }
    return false;
  }

  @Override
  public Converter<?, ?, ?, ?> getConverter(final PType<?> pType) {
    return new ThriftConverter();
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
