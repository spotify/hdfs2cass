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

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.specific.SpecificRecord;
import org.apache.avro.util.Utf8;
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
//    serializers.put(Utf8.class, UTF8Serializer.instance);
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
      return serializeList((GenericData.Array)value);
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
    } else if (value instanceof UUID) {
      return ByteBufferUtil.bytes((UUID) value);
    }


    throw new CrunchRuntimeException("Can not transform field (class: " + value.getClass() + ") to ByteBuffer");
  }

  /**
   * Serialize a map using Cassandra's map serializer.
   * Avro's Utf8 can't be cast to String and needs to be converted manually. This applies to both
   * List and Set.
   */
  private static ByteBuffer serializeMap(Map<?, ?> map) {
    TypeSerializer keySerializer = null;
    TypeSerializer valueSerializer = null;
    // no need to pass a serializer for elements if the collection is empty
    if (!map.isEmpty()) {
      // need to derive the type of the keys and values of the map
      Map.Entry<?, ?> firstEntry = map.entrySet().iterator().next();
      if (firstEntry.getKey() instanceof Utf8) {
        return serializeMap(updateKeysToString(map));
      }
      if (firstEntry.getValue() instanceof Utf8) {
        return serializeMap(updateValuesToString(map));
      }
      Class<?> keyType = firstEntry.getKey().getClass();
      Class<?> valueType = firstEntry.getValue().getClass();
      keySerializer = getSerializer(Map.class, keyType);
      valueSerializer = getSerializer(Map.class, valueType);
    }
    return MapSerializer.getInstance(keySerializer, valueSerializer).serialize(map);
  }

  /**
   * Serialize a list using Cassandra's list serializer.
   */
  private static ByteBuffer serializeList(List<?> list) {
    TypeSerializer elementSerializer = null;
    if (!list.isEmpty()) {
      Object first = list.iterator().next();
      if (first instanceof Utf8) {
        return serializeList(toIterableOfStrings(list));
      }
      elementSerializer = getSerializer(List.class, first.getClass());
    }
    return ListSerializer.getInstance(elementSerializer).serialize(list);
  }

  /**
   * Serialize a set using Cassandra's set serializer.
   */
  private static ByteBuffer serializeSet(Set<?> set) {
    TypeSerializer elementSerializer = null;
    if (!set.isEmpty()) {
      Object first = set.iterator().next();
      if (first instanceof Utf8) {
        return serializeSet(Sets.newLinkedHashSet(toIterableOfStrings(set)));
      }
      elementSerializer = getSerializer(Set.class, first.getClass());
    }
    return SetSerializer.getInstance(elementSerializer).serialize(set);
  }

  /**
   * Calls .toString() on each element in the iterable
   * @return new list with Strings in it
   */
  private static List<?> toIterableOfStrings(Iterable<?> list) {
    List<Object> newList = Lists.newArrayList();
    for (Object o : list) {
      newList.add(o.toString());
    }
    return newList;
  }

  private static Map<?, ?> updateKeysToString(Map<?, ?> oldMap) {
    Map<Object, Object> newMap = Maps.newLinkedHashMap();
    for (Object oldKey : oldMap.keySet()) {
      newMap.put(oldKey.toString(), oldMap.get(oldKey));
    }
    return newMap;
  }

  private static Map<?, ?> updateValuesToString(Map<?, ?> oldMap) {
    Map<Object, Object> newMap = Maps.newLinkedHashMap();
    for (Object oldKey : oldMap.keySet()) {
      newMap.put(oldKey, oldMap.get(oldKey).toString());
    }
    return newMap;
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

  public static ByteBuffer getPartitionKey(final List<ByteBuffer> values,
                                           final int[] keyIndexes) {
    if (keyIndexes.length == 1) {
      return values.get(keyIndexes[0]);
    } else {
      final ByteBuffer[] components = new ByteBuffer[keyIndexes.length];
      for (int i = 0; i < components.length; i++) {
        components[i] = values.get(keyIndexes[i]);
      }
      return compose(components);
    }
  }

  /**
   * Serialize a composite key.
   */
  private static ByteBuffer compose(final ByteBuffer[] buffers) {
    int totalLength = 0;
    for (final ByteBuffer bb : buffers)
        totalLength += 2 + bb.remaining() + 1;

    final ByteBuffer out = ByteBuffer.allocate(totalLength);
    for (final ByteBuffer buffer : buffers)
    {
        final ByteBuffer bb = buffer.duplicate();
        putShortLength(out, bb.remaining());
        out.put(bb);
        out.put((byte) 0);
    }
    out.flip();
    return out;
  }

  private static void putShortLength(final ByteBuffer bb, final int length) {
    bb.put((byte) ((length >> 8) & 0xFF));
    bb.put((byte) (length & 0xFF));
  }

  private CassandraRecordUtils() {
  }

}
