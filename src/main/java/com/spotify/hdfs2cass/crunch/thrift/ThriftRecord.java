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
package com.spotify.hdfs2cass.crunch.thrift;

import com.google.common.collect.Lists;
import org.apache.cassandra.thrift.Mutation;
import org.apache.crunch.MapFn;
import org.apache.crunch.Pair;
import org.apache.crunch.types.PTableType;
import org.apache.crunch.types.PType;
import org.apache.crunch.types.avro.Avros;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;

/**
 * Data structure used when importing hdfs2cass to schema-less Cassandra column families.
 * Schema-less Cassandra column families are the ones that have been created without CQL.
 */
public class ThriftRecord implements Serializable {
  public static PType<ThriftRecord> PTYPE = Avros.reflects(ThriftRecord.class);

  private ByteBuffer key;
  private List<Mutation> values;

  public ThriftRecord() {
  }

  /**
   * A ThriftRecord consists of Cassandra row key and a collection of
   * {@link org.apache.cassandra.thrift.Mutation}.
   * Mutations are passed to {@link org.apache.cassandra.hadoop.BulkOutputFormat}
   * and correspond to column insertions.
   * Mutations can be in any order. One row can be split into multiple ThriftRecords, Cassandra
   * will eventually handle this.
   * Placing 5,000+ mutations in one causes A LOT of memory pressure and should be avoided.
   *
   * @param key Cassandra row (i.e. partition) key
   * @param values List of columns belonging to this row
   */
  public ThriftRecord(final ByteBuffer key, final List<Mutation> values) {
    this.key = key;
    this.values = values;
  }

  public ByteBuffer getKey() {
    return key;
  }

  public static ThriftRecord of(final ByteBuffer key, final Mutation... values) {
    return of(key, Lists.newArrayList(values));
  }



  /**
   * @param key Cassandra row (i.e. partition) key
   * @param values List of columns belonging to this row
   * @return
   */
  public static ThriftRecord of(final ByteBuffer key, final List<Mutation> values) {
    return new ThriftRecord(key, values);
  }

  public Pair<ByteBuffer, Collection<Mutation>> asPair() {
    Collection<Mutation> collection = values;
    return Pair.of(key, collection);
  }

  public List<Mutation> getValues() {
    return values;
  }

  public static class AsPair extends MapFn<ThriftRecord, Pair<ByteBuffer, Collection<Mutation>>> {
    public static PTableType<ByteBuffer, Collection<Mutation>> PTYPE =
        Avros.tableOf(Avros.bytes(), Avros.collections(Avros.records(Mutation.class)));

    @Override
    public Pair<ByteBuffer, Collection<Mutation>> map(final ThriftRecord input) {
      return input.asPair();
    }
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    ThriftRecord that = (ThriftRecord) o;

    if (!key.equals(that.key)) return false;
    if (!values.equals(that.values)) return false;

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
    return "ThriftRecord{" + "key=" + key + ", values=" + values + '}';
  }
}
