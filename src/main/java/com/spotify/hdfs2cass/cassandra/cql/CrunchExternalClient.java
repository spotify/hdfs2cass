/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * The modifications to the upstream file is Copyright 2014 Spotify AB.
 * The original upstream file can be found at
 * https://github.com/apache/cassandra/blob/trunk/src/java/org/apache/cassandra/hadoop/AbstractBulkRecordWriter.java
 */
package com.spotify.hdfs2cass.cassandra.cql;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.hadoop.AbstractBulkRecordWriter;
import org.apache.hadoop.conf.Configuration;

import java.util.HashMap;
import java.util.Map;

/**
 * This is an almost-copy of {@link org.apache.cassandra.hadoop.cql3.CqlBulkRecordWriter.ExternalClient}
 * <p>
 * We had to re-implement this class because of https://issues.apache.org/jira/browse/CASSANDRA-8367
 * </p>
 */
public class CrunchExternalClient extends AbstractBulkRecordWriter.ExternalClient {
  private Map<String, Map<String, CFMetaData>> knownCqlCfs = new HashMap<>();

  public CrunchExternalClient(Configuration conf) {
    super(conf);
  }

  public void addKnownCfs(String keyspace, String cql) {
    Map<String, CFMetaData> cfs = knownCqlCfs.get(keyspace);

    if (cfs == null) {
      cfs = new HashMap<>();
      knownCqlCfs.put(keyspace, cfs);
    }
    CFMetaData metadata = CFMetaData.compile(cql, keyspace);
    cfs.put(metadata.cfName, metadata);
  }

  @Override
  public CFMetaData getCFMetaData(String keyspace, String cfName) {
    CFMetaData metadata = super.getCFMetaData(keyspace, cfName);
    if (metadata != null) {
      return metadata;
    }
    Map<String, CFMetaData> cfs = knownCqlCfs.get(keyspace);
    return cfs != null ? cfs.get(cfName) : null;
  }
}
