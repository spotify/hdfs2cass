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
import com.google.common.collect.Sets;
import com.spotify.hdfs2cass.crunch.cql.CQLRecord;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.crunch.MapFn;
import org.joda.time.DateTimeUtils;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * {@link org.apache.crunch.MapFn} implementation used to transform generic Avro records
 * into records suitable for being inserted into Cassandra table created using CQL.
 */
public class AvroToCQL extends MapFn<GenericRecord, CQLRecord> {

  private String rowkey;
  private String timestamp;
  private String ttl;
  private Set<String> ignore;

  private boolean posInitialized = false;
  private int rowkeyPos = 0;
  private int ttlPos = -1;
  private int timestampPos = -1;
  private Set<Integer> ignorePos = Sets.newHashSet();

  public AvroToCQL(final String rowkey, final String timestamp, final String ttl,
      final List<String> ignore) {
    this.rowkey = rowkey;
    this.timestamp = timestamp;
    this.ttl = ttl;
    this.ignore = new HashSet<>(ignore);
  }

  @Override
  public CQLRecord map(GenericRecord record) {
    if (!posInitialized) {
      initPos(record);
    }

    Object rowkey = null;
    long timestamp = DateTimeUtils.currentTimeMillis();
    int ttl = 0;
    List<Object> values = Lists.newArrayList();
    for (Schema.Field field : record.getSchema().getFields()) {
      int pos = field.pos();
      if (pos == rowkeyPos) {
        rowkey = record.get(pos);
        if (!ignorePos.contains(pos)) {
          values.add(rowkey);
        }
      } else if (pos == ttlPos) {
        ttl = (int) Objects.firstNonNull(record.get(ttlPos), 0);
      } else if (pos == timestampPos) {
        timestamp = (long) Objects.firstNonNull(record.get(timestampPos), timestamp);
      } else if (!ignorePos.contains(pos)) {

        values.add(record.get(pos));
      }
    }

    return CQLRecord.create(rowkey, timestamp, ttl, values);
  }

  private void initPos(final GenericRecord record) {
    Schema schema = record.getSchema();
    for (Schema.Field field : schema.getFields()) {
      int pos = field.pos();
      if (field.name().equals(rowkey)) {
        rowkeyPos = pos;
      } else if (field.name().equals(timestamp)) {
        timestampPos = pos;
      } else if (field.name().equals(ttl)) {
        ttlPos = pos;
      } else if (ignore.contains(field.name())) {
        ignorePos.add(pos);
      }
    }
  }

}
