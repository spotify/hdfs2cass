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

import com.spotify.hdfs2cass.crunch.thrift.ThriftRecord;
import org.apache.avro.specific.SpecificRecord;
import org.apache.crunch.PCollection;

public final class Thrift {
  public static final String DEFAULT_ROWKEY_FIELD_NAME = "rowkey";
  public static final String DEFAULT_TTL_FIELD_NAME = "ttl";
  public static final String DEFAULT_TIMESTAMP_FIELD_NAME = "timestamp";

  private Thrift() {
  }

  public static <T extends SpecificRecord> PCollection<ThriftRecord> byConvention(final PCollection<T> collection) {
    return byFieldNames(collection, DEFAULT_ROWKEY_FIELD_NAME, DEFAULT_TTL_FIELD_NAME, DEFAULT_TIMESTAMP_FIELD_NAME);
  }

  public static <T extends SpecificRecord> PCollection<ThriftRecord> byFieldNames(
      final PCollection<T> collection,
      final String rowKeyFieldName,
      final String ttlFieldName,
      final String timestampFieldName
  ) {
    final Class<T> recordType = collection.getPType().getTypeClass();
    T record;
    try {
      record = recordType.getConstructor().newInstance();
    } catch (Exception e) {
      throw new RuntimeException("Could not create an instance of the record to determine it's schema", e);
    }

    ThriftByFieldNamesFn<T> doFn = new ThriftByFieldNamesFn<T>(record.getSchema(), rowKeyFieldName, ttlFieldName, timestampFieldName);
    return collection.parallelDo(doFn, ThriftRecord.PTYPE);
  }

}
