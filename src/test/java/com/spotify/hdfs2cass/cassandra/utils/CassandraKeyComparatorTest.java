/*
/*
 * Copyright 2016 Spotify AB. All rights reserved.
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

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.junit.MatcherAssert.assertThat;

import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.OrderPreservingPartitioner;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class CassandraKeyComparatorTest {
  private static final EncoderFactory ENCODERS = EncoderFactory.get();

  private final CassandraKeyComparator comparator = new CassandraKeyComparator();
  private final Configuration conf = new Configuration();

  @Test
  public void compareOrderPreservingPartitioner() throws IOException {
    conf.set(CassandraParams.SCRUB_CASSANDRACLUSTER_PARTITIONER_CONFIG,
        OrderPreservingPartitioner.class.getName());
    comparator.setConf(conf);
    checkOrder("abc", "def");
    checkOrder("1", "2");
    checkOrder("abc1", "abc2");
    checkOrder("abc", "abcdef");
  }

  @Test
  public void compareMurmur3Partitioner() throws IOException {
    conf.set(CassandraParams.SCRUB_CASSANDRACLUSTER_PARTITIONER_CONFIG,
        Murmur3Partitioner.class.getName());
    comparator.setConf(conf);
    // murmur3_128("foo")[0] = -2129773440516405919
    // murmur3_128("bar")[0] = -7911037993560119804
    // murmur3_128("baz")[0] = 8295379539955784970
    checkOrder("bar", "foo");
    checkOrder("foo", "baz");
    checkOrder("bar", "baz");

    // Murmur3Partitioner maps empty string to Long.MIN_VALUE
    checkOrder("", "foo");
    checkOrder("", "bar");
  }

  private void checkOrder(final String key1, final String key2) throws IOException {
    final byte[] buf1 = bytes(key1, 0);
    final int offset = 3;
    final byte[] buf2 = bytes(key2, offset);

    final int l1 = buf1.length;
    final int l2 = buf2.length - offset;
    assertThat(comparator.compare(buf1, 0, l1, buf2, offset, l2), lessThan(0));
    assertThat(comparator.compare(buf2, offset, l2, buf1, 0, l1), greaterThan(0));
    assertThat(comparator.compare(buf1, 0, l1, buf1, 0, l1), is(0));
    assertThat(comparator.compare(buf2, offset, l2, buf2, offset, l2), is(0));
  }

  private static byte[] bytes(final String s, final int offset)
      throws IOException {
    final ByteArrayOutputStream baos = new ByteArrayOutputStream(32);
    baos.write(new byte[offset], 0, offset);
    final BinaryEncoder enc = ENCODERS.directBinaryEncoder(baos, null);
    enc.writeString(s);
    return baos.toByteArray();
  }
}
