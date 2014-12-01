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
import com.google.common.collect.Lists;
import com.spotify.hdfs2cass.crunch.cql.CQLRecord;
import org.apache.crunch.MapFn;
import org.joda.time.DateTimeUtils;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * {@link org.apache.crunch.MapFn} implementation used to transform outdated hdfs2cass source format
 * into records suitable for being inserted into CQL-defined Cassandra table.
 */
public class LegacyHdfsToCQL extends MapFn<ByteBuffer, CQLRecord> {

  /**
   * CQL-based import requires us to provide list of values we want to insert (it it's
   * smart enough to figure everything else automatically). So, we convert each input
   * row into a list of values, and wrap all of them in CQLRecord.
   *
   * @param inputRow byte representation of the input row as it was read from Avro file
   * @return wraps the record into something that blends nicely into Crunch
   */
    @Override
    public CQLRecord map(ByteBuffer inputRow) {
      LegacyInputFormat row = LegacyInputFormat.parse(inputRow);
      long ts = Objects.firstNonNull(row.getTimestamp(), DateTimeUtils.currentTimeMillis());
      int ttl = Objects.firstNonNull(row.getTtl(), 0l).intValue();
      CharSequence key = row.getRowkey();
      List values = Lists.newArrayList(key, row.getColname(), row.getColval());
      return CQLRecord.create(key, ts, ttl, values);
    }

}
