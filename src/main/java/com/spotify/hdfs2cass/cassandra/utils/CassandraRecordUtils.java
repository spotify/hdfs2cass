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
import org.apache.cassandra.serializers.BooleanSerializer;
import org.apache.cassandra.serializers.DecimalSerializer;
import org.apache.cassandra.serializers.DoubleSerializer;
import org.apache.cassandra.serializers.FloatSerializer;
import org.apache.cassandra.serializers.InetAddressSerializer;
import org.apache.cassandra.serializers.Int32Serializer;
import org.apache.cassandra.serializers.IntegerSerializer;
import org.apache.cassandra.serializers.ListSerializer;
import org.apache.cassandra.serializers.LongSerializer;
import org.apache.cassandra.serializers.MapSerializer;
import org.apache.cassandra.serializers.SetSerializer;
import org.apache.cassandra.serializers.TimestampSerializer;
import org.apache.cassandra.serializers.TypeSerializer;
import org.apache.cassandra.serializers.UTF8Serializer;
import org.apache.cassandra.serializers.UUIDSerializer;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.Mutation;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.crunch.CrunchRuntimeException;
import org.joda.time.DateTimeUtils;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

public final class CassandraRecordUtils implements Serializable {

  private static final Map<Class<?>, TypeSerializer<?>> serializers;
  static {
    serializers = new HashMap<>();
    serializers.put(BigInteger.class, IntegerSerializer.instance);
    serializers.put(Boolean.class, BooleanSerializer.instance);
    serializers.put(BigDecimal.class, DecimalSerializer.instance);
    serializers.put(Date.class, TimestampSerializer.instance);
    serializers.put(Double.class, DoubleSerializer.instance);
    serializers.put(Float.class, FloatSerializer.instance);
    serializers.put(InetAddress.class, InetAddressSerializer.instance);
    serializers.put(Integer.class, Int32Serializer.instance);
    serializers.put(Long.class, LongSerializer.instance);
    serializers.put(String.class, UTF8Serializer.instance);
    serializers.put(UUID.class, UUIDSerializer.instance);
  }

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
    } else if (value instanceof Map) {
      return serializeMap((Map<?, ?>) value);
    } else if (value instanceof Set) {
      return serializeSet((Set<?>) value);
    } else if (value instanceof List) {
      return serializeList((List<?>) value);
    }


    throw new CrunchRuntimeException("Can not transform field (class: " + value.getClass() + ") to ByteBuffer");
  }

  private static ByteBuffer serializeMap(Map<?, ?> map) {
    TypeSerializer keySerializer = null;
    TypeSerializer valueSerializer = null;
    // no need to pass a serializer for elements if the collection is empty
    if (!map.isEmpty()) {
      // need to derive the type of the keys and values of the map
      Map.Entry<?, ?> firstEntry = map.entrySet().iterator().next();
      Class<?> keyType = firstEntry.getKey().getClass();
      Class<?> valueType = firstEntry.getValue().getClass();
      keySerializer = getSerializer(Map.class, keyType);
      valueSerializer = getSerializer(Map.class, valueType);
    }
    return MapSerializer.getInstance(keySerializer, valueSerializer).serialize(map);
  }

  private static ByteBuffer serializeList(List<?> list) {
    TypeSerializer elementSerializer = null;
    if (!list.isEmpty()) {
      Object first = list.iterator().next();
      elementSerializer = getSerializer(List.class, first.getClass());
    }
    return ListSerializer.getInstance(elementSerializer).serialize(list);
  }

  private static ByteBuffer serializeSet(Set<?> set) {
    TypeSerializer elementSerializer = null;
    if (!set.isEmpty()) {
      Object first = set.iterator().next();
      elementSerializer = getSerializer(Set.class, first.getClass());
    }
    return SetSerializer.getInstance(elementSerializer).serialize(set);
  }

  private static TypeSerializer getSerializer(Class<?> collectionType, Class<?> clazz) {
    if (!serializers.containsKey(clazz)) {
      throw new CrunchRuntimeException(
          "Can not transform " + collectionType + " with element types of " + clazz
          + " to ByteBuffer");
    }
    return serializers.get(clazz);
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
