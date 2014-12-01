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
package com.spotify.hdfs2cass;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.spotify.hdfs2cass.cassandra.utils.CassandraParams;
import com.spotify.hdfs2cass.crunch.cql.CQLRecord;
import com.spotify.hdfs2cass.crunch.cql.CQLTarget;
import com.spotify.hdfs2cass.crunch.thrift.ThriftRecord;
import com.spotify.hdfs2cass.crunch.thrift.ThriftTarget;
import org.apache.crunch.PCollection;
import org.apache.crunch.Pipeline;
import org.apache.crunch.PipelineResult;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.io.From;
import org.apache.crunch.io.avro.AvroFileSource;
import org.apache.crunch.types.avro.Avros;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.BasicConfigurator;

import java.io.Serializable;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * Crunch job used to import files with legacy-formatted data into Cassandra Thrift or CQL table.
 * <p>
 *   See {@link com.spotify.hdfs2cass.LegacyInputFormat} for format definition.
 * </p><p>
 * How to use command line:
 * TODO(zvo): add example usage
 * </p><p>
 * TODO(zvo): add example URIS
 * </p><p>
 * </p>
 */

public class LegacyHdfs2Cass extends Configured implements Tool, Serializable {

  @Parameter(names = "--input", required = true)
  protected static List<String> input;

  @Parameter(names = "--output", required = true)
  protected static String output;


  public static void main(String[] args) throws Exception {
    // Logging for local runs
    BasicConfigurator.configure();
    ToolRunner.run(new Configuration(), new LegacyHdfs2Cass(), args);
  }

  @Override
  public int run(String[] args) throws Exception {

    new JCommander(this, args);

    URI outputUri = URI.create(output);

    // Our crunch job is a MapReduce job
    Pipeline pipeline = new MRPipeline(LegacyHdfs2Cass.class, getConf());

    // Parse & fetch info about target Cassandra cluster
    CassandraParams params = CassandraParams.parse(outputUri);

    // Read records from Avro files in inputFolder
    PCollection<ByteBuffer> records =
        pipeline.read(From.avroFile(inputList(input), Avros.records(ByteBuffer.class)));

    // Transform the input
    String protocol = outputUri.getScheme();
    if (protocol.equalsIgnoreCase("thrift")) {
      records
          // First convert ByteBuffers to ThriftRecords
          .parallelDo(new LegacyHdfsToThrift(), ThriftRecord.PTYPE)
          // Then group the ThriftRecords in preparation for writing them
          .parallelDo(new ThriftRecord.AsPair(), ThriftRecord.AsPair.PTYPE)
          .groupByKey(params.createGroupingOptions())
          // Finally write the ThriftRecords to Cassandra
          .write(new ThriftTarget(outputUri, params));
    }
    else if (protocol.equalsIgnoreCase("cql")) {
      records
          // In case of CQL, convert ByteBuffers to CQLRecords
          .parallelDo(new LegacyHdfsToCQL(), CQLRecord.PTYPE)
          .parallelDo(new CQLRecord.AsPair(), CQLRecord.AsPair.PTYPE)
          .groupByKey(params.createGroupingOptions())
          .write(new CQLTarget(outputUri, params));
    }

    // Execute the pipeline
    PipelineResult result = pipeline.done();
    return result.succeeded() ? 0 : 1;
  }

  private static List<Path> inputList(List<String> inputs) {
    return Lists.newArrayList(Iterables.transform(inputs, new StringToHDFSPath()));
  }

  private static class StringToHDFSPath implements Function<String, Path> {
    @Override
    public Path apply(String resource) {
      return new Path(resource);
    }
  }

}

