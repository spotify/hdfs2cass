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

import com.google.common.base.Throwables;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.mapred.AvroKey;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.RawComparator;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * A comparator for sorting keys in sstable order. This is used in the shuffle
 * to ensure that the reducer sees inputs in the correct order and can append
 * them to sstables without sorting again.
 */
public class CassandraKeyComparator implements RawComparator<AvroKey<ByteBuffer>>, Configurable {
  private static final DecoderFactory DECODER_FACTORY = DecoderFactory.get();

  private Configuration conf;
  private IPartitioner<?> partitioner;

  @Override
  public int compare(byte[] o1, int s1, int l1, byte[] o2, int s2, int l2) {
    try {
      final BinaryDecoder d1 = DECODER_FACTORY.binaryDecoder(o1, s1, l1, null);
      final ByteBuffer key1 = d1.readBytes(null);

      // re-use the decoder instance, but do not re-use the byte buffer,
      // because DecoratedKey stores a reference
      final BinaryDecoder d2 = DECODER_FACTORY.binaryDecoder(o2, s2, l2, d1);
      final ByteBuffer key2 = d2.readBytes(null);

      return compare(key1, key2);
    } catch (final IOException e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public int compare(AvroKey<ByteBuffer> o1, AvroKey<ByteBuffer> o2) {
    final ByteBuffer key1 = o1.datum();
    final ByteBuffer key2 = o2.datum();
    return compare(key1, key2);
  }

  private int compare(final ByteBuffer key1, final ByteBuffer key2) {
    assert key1 != key2 : "bug - unsafe buffer re-use";
    return partitioner.decorateKey(key1).compareTo(partitioner.decorateKey(key2));
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
    final String partitionerParam = conf.get(CassandraParams.SCRUB_CASSANDRACLUSTER_PARTITIONER_CONFIG);
    if (partitionerParam == null) {
      throw new RuntimeException("Didn't get any cassandra partitioner information");
    }
    try {
      partitioner = (IPartitioner<?>) Class.forName(partitionerParam).newInstance();
    } catch (final Exception e) {
      throw Throwables.propagate(e);
    }
  }
}
