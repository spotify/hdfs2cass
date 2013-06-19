// Copyright (c) 2013 Spotify AB
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy of
// the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations under
// the License.

package com.spotify.hdfs2cass;

import com.spotify.hdfs2cass.misc.ClusterInfo;
import org.apache.cassandra.dht.RandomPartitioner;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.junit.Test;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

/*
 * Copyright (c) 2013 Spotify AB
 *
 */

/**
 *
 * @author anand
 */
public class CassandraPartitionerTest {

  @Test
  public void testGetPartition() throws Exception {
    final int maxNodes = 5;

    final List<String> tokenRanges = new ArrayList<String>();

    BigInteger start = BigInteger.ZERO;
    BigInteger step = RandomPartitioner.MAXIMUM.divide(BigInteger.valueOf(maxNodes));
    for (int i = 0; i < maxNodes - 1; i++) {
      BigInteger end = start.add(step);

      tokenRanges.add(String.format("%d:%d", start, end));
      start = end.add(BigInteger.ONE);
    }

    tokenRanges.add(String.format("%d:0", start));

    final JobConf conf = new JobConf();
    conf.set(ClusterInfo.SPOTIFY_CASSANDRA_TOKENS_PARAM, StringUtils.join(tokenRanges, ","));
    conf.set(ClusterInfo.SPOTIFY_CASSANDRA_PARTITIONER_PARAM, "org.apache.cassandra.dht.RandomPartitioner");

    CassandraPartitioner instance = new CassandraPartitioner();
    instance.configure(conf);

    Text key = new Text("foobar");
    assertEquals(2, instance.getPartition(key, null, 5));

    key = new Text("someotherkey");
    assertEquals(1, instance.getPartition(key, null, 5));

    key = new Text("1ce5cf4b861941f4aa799ae39ac9daa4");
    assertEquals(4, instance.getPartition(key, null, 5));
  }
}
