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

import com.google.common.base.Objects;
import com.spotify.hdfs2cass.cassandra.utils.CassandraRecordUtils;
import com.spotify.hdfs2cass.crunch.thrift.ThriftRecord;
import org.apache.cassandra.thrift.Mutation;
import org.apache.crunch.MapFn;
import org.joda.time.DateTimeUtils;

import java.nio.ByteBuffer;

/**
 * {@link org.apache.crunch.MapFn} implementation used to transform outdated hdfs2cass source format
 * into records suitable for being inserted into non-CQL/Thrift Cassandra table.
 */
public class LegacyHdfsToThrift extends MapFn<ByteBuffer, ThriftRecord>  {

  /**
   * Thrift-based import requires us to provide {@link org.apache.cassandra.thrift.Mutation}.
   * Therefore we convert each input line into one.
   *
   * @param inputRow byte representation of the input row as it was read from Avro file
   * @return wraps the record into something that blends nicely into Crunch
   */
    @Override
    public ThriftRecord map(ByteBuffer inputRow) {
      LegacyInputFormat row = LegacyInputFormat.parse(inputRow);
      ByteBuffer key = CassandraRecordUtils.toByteBuffer(row.getRowkey());
      long ts = Objects.firstNonNull(row.getTimestamp(), DateTimeUtils.currentTimeMillis());
      int ttl = Objects.firstNonNull(row.getTtl(), 0l).intValue();
      Mutation mutation = CassandraRecordUtils.createMutation(
          row.getColname(), row.getColval(), ts, ttl);
      return ThriftRecord.of(key, mutation);
    }

}
