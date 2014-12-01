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

import com.google.common.collect.Lists;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.specific.SpecificRecord;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.Mutation;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.crunch.CrunchRuntimeException;
import org.joda.time.DateTimeUtils;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.List;

public final class CassandraRecordUtils implements Serializable {

  public static ByteBuffer toByteBuffer(final Object value) {
    if (value == null) {
      return ByteBufferUtil.EMPTY_BYTE_BUFFER;
    } else if (value instanceof CharSequence) {
      return ByteBufferUtil.bytes(value.toString());
    } else if (value instanceof Double) {
      return ByteBufferUtil.bytes((Double) value);
    } else if (value instanceof Float) {
      return ByteBufferUtil.bytes((Float) value);
    } else if (value instanceof Integer) {
      return ByteBufferUtil.bytes((Integer) value);
    } else if (value instanceof Long) {
      return ByteBufferUtil.bytes((Long) value);
    } else if (value instanceof ByteBuffer) {
      return ByteBufferUtil.clone((ByteBuffer) value);
    } else if (value instanceof GenericData.Array) {
      List<ByteBuffer> buffers = Lists.newArrayList();
      for (Object item : ((GenericData.Array)value)) {
        buffers.add(toByteBuffer(item));
      }
      return CompositeType.build(buffers.toArray(new ByteBuffer[0]));
    } else if (value instanceof SpecificRecord) {
      List<ByteBuffer> buffers = Lists.newArrayList();
      SpecificRecord record = (SpecificRecord) value;
      for (Schema.Field field : record.getSchema().getFields()) {
        buffers.add(toByteBuffer(record.get(field.pos())));
      }
      return CompositeType.build(buffers.toArray(new ByteBuffer[0]));
    }
    throw new CrunchRuntimeException("Can not transform field (class: " + value.getClass() + ") to ByteBuffer");
  }

  public static Mutation createMutation(final Object name, final Object value) {
    return createMutation(name, value, DateTimeUtils.currentTimeMillis(), 0);
  }

  public static Mutation createMutation(Object name, Object value, long timestamp, int ttl) {
    Column column = new Column();
    column.setName(toByteBuffer(name));
    column.setValue(toByteBuffer(value));
    column.setTimestamp(timestamp);
    if (ttl > 0) {
      column.setTtl(ttl);
    }

    Mutation mutation = new Mutation();
    mutation.column_or_supercolumn = new ColumnOrSuperColumn();
    mutation.column_or_supercolumn.column = column;
    return mutation;
  }

  private CassandraRecordUtils() {
  }

}
