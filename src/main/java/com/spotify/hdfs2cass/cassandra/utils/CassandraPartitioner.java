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
package com.spotify.hdfs2cass.cassandra.utils;

import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.cassandra.dht.*;
import org.apache.cassandra.thrift.Mutation;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Partitioner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 * Uses the cassandra topology to send a key to a particular set of reducers
 */
public class CassandraPartitioner extends Partitioner<AvroKey<ByteBuffer>, AvroValue<Mutation>> implements Configurable, Serializable {

  private static final Logger logger = LoggerFactory.getLogger(CassandraPartitioner.class);

  private static final BigInteger MURMUR3_SCALE =
      BigInteger.valueOf(Murmur3Partitioner.MINIMUM.token).abs();

  private AbstractPartitioner partitioner;
  private BigInteger rangePerReducer;
  private List<Integer> reducers;
  private boolean distributeRandomly;
  private Random random;
  private Configuration conf;

  public CassandraPartitioner() {
  }

  @Override
  public int getPartition(AvroKey<ByteBuffer> key, AvroValue<Mutation> value, int numReducers) {
    if (distributeRandomly) {
      return reducers.get(random.nextInt(reducers.size()));
    }

    final Token token = partitioner.getToken(key.datum());
    BigInteger bigIntToken;
    if (token instanceof BigIntegerToken) {
      bigIntToken = ((BigIntegerToken) token).token.abs();
    } else if (token instanceof LongToken) {
      bigIntToken = BigInteger.valueOf(((LongToken) token).token).add(MURMUR3_SCALE);
    } else {
      throw new RuntimeException("Invalid partitioner Token type. Only BigIntegerToken and LongToken supported");
    }
    return reducers.get(bigIntToken.divide(rangePerReducer).intValue());
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;

    final String partitionerParam = conf.get(CassandraParams.SCRUB_CASSANDRACLUSTER_PARTITIONER_CONFIG);
    logger.info(CassandraParams.SCRUB_CASSANDRACLUSTER_PARTITIONER_CONFIG + ": " + partitionerParam);
    if (partitionerParam == null) {
      throw new RuntimeException("Didn't get any cassandra partitioner information");
    }

    try {
      partitioner = (AbstractPartitioner) Class.forName(partitionerParam).newInstance();
    } catch (Exception ex) {
      throw new RuntimeException("Invalid partitioner class name: " + partitionerParam);
    }

    final String rangePerReducerStr = conf.get(CassandraParams.SCRUB_CASSANDRACLUSTER_RANGE_PER_REDUCER_CONFIG);
    if (rangePerReducerStr == null) {
      throw new RuntimeException("Didn't get cassandra range per reducer");
    }

    rangePerReducer = new BigInteger(rangePerReducerStr);

    final String reducersStr = conf.get(CassandraParams.SCRUB_CASSANDRACLUSTER_REDUCERS_CONFIG);
    if (reducersStr == null) {
      throw new RuntimeException("Failed to get list of reducers");
    }

    final String[] parts = StringUtils.splitByWholeSeparatorPreserveAllTokens(reducersStr, ",");
    if ((parts == null) || (parts.length == 0)) {
      throw new RuntimeException("Didn't get any valid list of reducers");
    }

    reducers = new ArrayList<>(parts.length);
    for (String part : parts) {
      reducers.add(Integer.parseInt(part));
    }

    distributeRandomly = conf.getBoolean(CassandraParams.SCRUB_CASSANDRACLUSTER_DISTRIBUTE_RANDOMLY_CONFIG, false);
    if (distributeRandomly) {
      random = new Random();
    }

    logger.info("CP: range per reducer: {}, reducers: {}, distribute randomly: {}",
        new Object[]{rangePerReducerStr,
            Arrays.toString(reducers.toArray()),
            distributeRandomly});
  }

  @Override
  public Configuration getConf() {
    return conf;
  }
}
