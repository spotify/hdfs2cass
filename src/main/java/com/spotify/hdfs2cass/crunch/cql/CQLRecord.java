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
package com.spotify.hdfs2cass.crunch.cql;

import com.google.common.collect.Lists;
import com.spotify.hdfs2cass.cassandra.cql.CrunchCqlBulkOutputFormat;
import com.spotify.hdfs2cass.cassandra.utils.CassandraRecordUtils;
import com.spotify.hdfs2cass.crunch.CrunchConfigHelper;
import org.apache.avro.generic.IndexedRecord;
import org.apache.cassandra.utils.Hex;
import org.apache.crunch.types.PType;
import org.apache.crunch.types.avro.Avros;
import org.apache.hadoop.conf.Configuration;
import org.joda.time.DateTimeUtils;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

/**
 * Data structure used when importing hdfs2cass to Cassandra column families with schema.
 * These are column families that have been created using CQL.
 *
 * <p>
 * A CQLRecord consists of a List of values. The values are passed to
 * {@link org.apache.cassandra.hadoop.cql3.CqlBulkOutputFormat}.  They are used as instances of
 * parameters to CqlBulkOutputFormat's prepared statement. hdfs2cass can figure the prepared
 * statement out automatically in {@link com.spotify.hdfs2cass.cassandra.utils.CassandraParams}, but
 * it's possible to shuffle the order of columns in the prepared statement around using the
 * 'columnnames' parameter in the Cassandra target URI.
 * </p><p>
 * However, order of values must match the order of column names in the prepared statement.
 * </p>
 */
public class CQLRecord implements Serializable {
  public static PType<CQLRecord> PTYPE = Avros.reflects(CQLRecord.class);

  private final List<ByteBuffer> values;

  /**
   * Constructor for Avro reflection-based serialization.
   */
  public CQLRecord() {
    this(Lists.<ByteBuffer>newArrayList());
  }

  /**
   * @param values List of column values
   */
  private CQLRecord(final List<ByteBuffer> values) {
    this.values = values;
  }

  public static CQLRecord create(final Configuration conf, final Map<String, Object> valueMap) {
    return create(conf, DateTimeUtils.currentTimeMillis(), 0, valueMap);
  }

  public static CQLRecord create(final Configuration conf, final long timestamp, final int ttl,
                                 final Map<String, Object> valueMap) {
    List<Object> values = Lists.newArrayList(new Object[valueMap.size()]);
    String cfName = CrunchConfigHelper.getOutputColumnFamily(conf);
    for (Map.Entry<String, Object> valueMapEntry : valueMap.entrySet()) {
      int columnIndex = CrunchCqlBulkOutputFormat.getColumnIndex(conf, cfName, valueMapEntry.getKey());
      values.set(columnIndex, valueMapEntry.getValue());
    }
    return create(timestamp, ttl, values);
  }

  public static CQLRecord create(final long timestamp, final List<?> values) {
    return create(timestamp, 0, values);
  }

  public static CQLRecord create(final long timestamp, final int ttl, final List<?> values) {
    List<ByteBuffer> list = Lists.newArrayList();
    for (Object value : values) {
      list.add(CassandraRecordUtils.toByteBuffer(value));
    }
    list.add(CassandraRecordUtils.toByteBuffer(timestamp));
    list.add(CassandraRecordUtils.toByteBuffer(ttl));
    return new CQLRecord(list);
  }

  /**
   * @deprecated Use the overload without the {@code key} argument
   */
  @Deprecated
  public static CQLRecord create(final Configuration conf, final Object rowKey,
                                 final long timestamp, final int ttl,
                                 final Map<String, Object> valueMap) {
    return create(conf, timestamp, ttl, valueMap);
  }

  /**
   * @deprecated Use the overload without the {@code key} argument
   */
  @Deprecated
  public static CQLRecord create(final Configuration conf, final Object rowKey,
                                 final Map<String, Object> valueMap) {
    return create(conf, valueMap);
  }

  /**
   * @deprecated Use the overload without the {@code key} argument
   */
  @Deprecated
  public static CQLRecord create(final Configuration conf, final Object rowKey,
                                 final long timestamp, final Map<String, Object> valueMap) {
    return CQLRecord.create(conf, timestamp, 0, valueMap);
  }

  /**
   * @deprecated Use the overload without the {@code key} argument
   */
  @Deprecated
  public static CQLRecord create(final Object key, final List<?> values) {
    return create(DateTimeUtils.currentTimeMillis(), values);
  }

  /**
   * @deprecated Use the overload without the {@code key} argument
   */
  @Deprecated
  public static CQLRecord create(final Object key, final long timestamp, final List<?> values) {
    return create(timestamp, 0, values);
  }

  /**
   * @deprecated Use the overload without the {@code key} argument
   */
  @Deprecated
  public static CQLRecord create(final Object key, final long timestamp, final int ttl,
                                 final List<?> values) {
    return create(timestamp, ttl, values);
  }

  public static CQLRecord transform(final IndexedRecord record) {
    Object key = record.get(0);
    List<Object> values = Lists.newArrayList();
    for (int i = 0; i < record.getSchema().getFields().size(); i++) {
      values.add(record.get(i));
    }

    return create(key, values);
  }

  public List<ByteBuffer> getValues() {
    return values;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    CQLRecord cqlRecord = (CQLRecord) o;

    if (!values.equals(cqlRecord.values)) return false;

    return true;
  }

  @Override
  public int hashCode() {
    return values.hashCode();
  }

  @Override
  public String toString() {
    StringBuilder valuesAsStrings = new StringBuilder();
    valuesAsStrings.append("[");
    for (ByteBuffer value : values) {
      valuesAsStrings.append(Hex.bytesToHex(value.array()));
      valuesAsStrings.append(",");
    }
    valuesAsStrings.deleteCharAt(valuesAsStrings.length()-1);
    valuesAsStrings.append("]");
    return String.format("CQLRecord(key=%s, values=%s", valuesAsStrings);
  }
}
