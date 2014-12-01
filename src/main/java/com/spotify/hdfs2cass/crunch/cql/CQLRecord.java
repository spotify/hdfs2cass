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
import com.spotify.hdfs2cass.cassandra.utils.CassandraRecordUtils;
import org.apache.avro.generic.IndexedRecord;
import org.apache.crunch.MapFn;
import org.apache.crunch.Pair;
import org.apache.crunch.types.PTableType;
import org.apache.crunch.types.PType;
import org.apache.crunch.types.avro.Avros;
import org.joda.time.DateTimeUtils;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;

/**
 * Data structure used when importing hdfs2cass to Cassandra column families with schema.
 * These are column families that have been created using CQL.
 *
 * <p>
 * A CQLRecord consists of Cassandra row key and a List of values. The values are passed to
 * {@link org.apache.cassandra.hadoop.cql3.CqlBulkOutputFormat}.  They are used as instances of
 * parameters to CqlBulkOutputFormat's prepared statement. hdfs2cass can figure the prepared
 * statement out automatically in {@link com.spotify.hdfs2cass.cassandra.utils.CassandraParams}, but
 * it's possible to shuffle the order of columns in the prepared statement around using the
 * 'columnnames' parameter in the Cassandra target URI.
 * </p><p>
 * However, order of values must match the order of column names in the prepared statement.
 * Furthermore, CQLRecord does not add key into the values - this is left up to the user.
 * </p>
 */
public class CQLRecord implements Serializable {
  public static PType<CQLRecord> PTYPE = Avros.reflects(CQLRecord.class);

  private ByteBuffer key;
  private List<ByteBuffer> values;

  public CQLRecord() {
  }

  /**
   * @param key    Cassandra row (i.e. partition) key
   * @param values List of column values
   */
  public CQLRecord(final ByteBuffer key, final List<ByteBuffer> values) {
    this.key = key;
    this.values = values;
  }

  /**
   * @param key    Cassandra row (i.e. partition) key
   * @param values List of column values
   * @return
   */
  public static CQLRecord create(final Object key, final List<?> values) {
    return CQLRecord.create(key, DateTimeUtils.currentTimeMillis(), values);
  }

  public static CQLRecord create(final Object key, final long timestamp, final List<?> values) {
    return CQLRecord.create(key, timestamp, 0, values);
  }

  public static CQLRecord create(final Object key, final long timestamp, final int ttl, final List<?> values) {
    List<ByteBuffer> list = Lists.newArrayList();
    for (Object value : values) {
      list.add(CassandraRecordUtils.toByteBuffer(value));
    }
    list.add(CassandraRecordUtils.toByteBuffer(timestamp));
    list.add(CassandraRecordUtils.toByteBuffer(ttl));
    return new CQLRecord(CassandraRecordUtils.toByteBuffer(key), list);
  }

  public static CQLRecord transform(final IndexedRecord record) {
    Object key = record.get(0);
    List<Object> values = Lists.newArrayList();
    for (int i = 0; i < record.getSchema().getFields().size(); i++) {
      values.add(record.get(i));
    }

    return create(key, values);
  }

  public Pair<ByteBuffer, Collection<ByteBuffer>> asPair() {
    // casting won't most likely work, because java
    Collection<ByteBuffer> v = values;
    return Pair.of(key, v);
  }

  public static class AsPair extends MapFn<CQLRecord, Pair<ByteBuffer, Collection<ByteBuffer>>> {
    public static PTableType<ByteBuffer, Collection<ByteBuffer>> PTYPE =
        Avros.tableOf(Avros.bytes(), Avros.collections(Avros.bytes()));

    @Override
    public Pair<ByteBuffer, Collection<ByteBuffer>> map(final CQLRecord input) {
      return input.asPair();
    }
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    CQLRecord cqlRecord = (CQLRecord) o;

    if (!key.equals(cqlRecord.key)) return false;
    if (!values.equals(cqlRecord.values)) return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = key.hashCode();
    result = 31 * result + values.hashCode();
    return result;
  }

  @Override
  public String toString() {
    return "CQLRecord{" + "key=" + key + ", values=" + values + '}';
  }
}
