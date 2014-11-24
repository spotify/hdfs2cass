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
package com.spotify.hdfs2cass.crunch.thrift.converters;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.spotify.hdfs2cass.crunch.thrift.ThriftRecord;
import com.spotify.hdfs2cass.cassandra.utils.CassandraRecordUtils;
import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecord;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.Mutation;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.crunch.CrunchRuntimeException;
import org.apache.crunch.MapFn;
import org.joda.time.DateTimeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.List;

public class ThriftByFieldNamesFn<T extends SpecificRecord> extends MapFn<T, ThriftRecord> {
  private final Logger logger = LoggerFactory.getLogger(ThriftByFieldNamesFn.class);

  private int rowKeyIndex = -1;
  private int ttlIndex = -1;
  private int timestampIndex = -1;

  public ThriftByFieldNamesFn() {
  }

  public ThriftByFieldNamesFn(final Schema schema, final String rowKeyFieldName, final String ttlFieldName, final String timestampFieldName) {

    Schema.Field rowKeyField = schema.getField(rowKeyFieldName);
    if (rowKeyField == null) {
      throw new CrunchRuntimeException("Row key field name not found: " + rowKeyFieldName);
    }
    rowKeyIndex = rowKeyField.pos();

    Schema.Field ttlField = schema.getField(ttlFieldName);
    if (ttlField == null) {
      logger.info("TTL field not found, TTL will not be set");
    } else {
      logger.info("Using TTL field name: " + ttlFieldName);
      ttlIndex = ttlField.pos();
    }

    Schema.Field timestampField = schema.getField(timestampFieldName);
    if (timestampField == null) {
      logger.info("Timestamp field not found, System.currentTimeMillis() will be used");
    } else {
      logger.info("Using timestamp field name: " + ttlFieldName);
      timestampIndex = timestampField.pos();
    }
  }

  @Override
  public ThriftRecord map(T input) {
    ByteBuffer key = getRowKey(input);
    List<Mutation> values = getMutations(input);
    return ThriftRecord.of(key, values);
  }

  private List<Mutation> getMutations(final T input) {
    List<Mutation> mutations = Lists.newArrayList();

    long timestamp = getTimestamp(input);
    Optional<Integer> ttl = getTtl(input);

    for (Schema.Field field : input.getSchema().getFields()) {
      int fieldPos = field.pos();
      if (fieldPos == rowKeyIndex || fieldPos == ttlIndex || fieldPos == timestampIndex) {
        continue;
      }

      Object fieldValue = input.get(fieldPos);

      Column column = new Column();
      column.setName(ByteBufferUtil.bytes(field.name()));
      column.setTimestamp(timestamp);
      if (ttl.isPresent()) {
        column.setTtl(ttl.get());
      }
      column.setValue(CassandraRecordUtils.toByteBuffer(fieldValue));

      Mutation mutation = new Mutation();
      mutation.column_or_supercolumn = new ColumnOrSuperColumn();
      mutation.column_or_supercolumn.column = column;

      mutations.add(mutation);
    }


    return mutations;
  }

  private Optional<Integer> getTtl(final T input) {
    if (ttlIndex > -1) {
      Object value = input.get(timestampIndex);
      if (value instanceof Long) {
        return Optional.fromNullable(Integer.class.cast(value));
      } else {
        throw new CrunchRuntimeException("Can not transform ttl field (class: " + value.getClass() + ") to Integer");
      }
    } else {
      return Optional.absent();
    }
  }

  private long getTimestamp(final T input) {
    if (timestampIndex > -1) {
      Object value = input.get(timestampIndex);
      if (value instanceof Long) {
        return (long) value;
      } else {
        throw new CrunchRuntimeException("Can not transform timestamp field (class: " + value.getClass() + ") to long");
      }
    } else {
      return DateTimeUtils.currentTimeMillis();
    }
  }

  public ByteBuffer getRowKey(final T input) {
    Object value = input.get(rowKeyIndex);
    return CassandraRecordUtils.toByteBuffer(value);
  }
}
