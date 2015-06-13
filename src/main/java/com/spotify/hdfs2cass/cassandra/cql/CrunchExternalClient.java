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

import org.apache.cassandra.auth.IAuthenticator;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.hadoop.AbstractBulkRecordWriter;
import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.cassandra.thrift.*;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.hadoop.conf.Configuration;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;

/**
 * This is an almost-copy of {@link org.apache.cassandra.hadoop.cql3.CqlBulkRecordWriter.ExternalClient}
 * <p>
 * We had to re-implement this class because of https://issues.apache.org/jira/browse/CASSANDRA-8367
 * </p>
 */
public class CrunchExternalClient extends AbstractBulkRecordWriter.ExternalClient {

  private final Map<String, CFMetaData> knownCfs = new HashMap<>();
  private final Configuration conf;
  private final String hostlist;
  private final int rpcPort;
  private final String username;
  private final String password;

  public CrunchExternalClient(Configuration conf) {

    super(conf);
    this.conf = conf;
    this.hostlist = ConfigHelper.getOutputInitialAddress(conf);
    this.rpcPort = ConfigHelper.getOutputRpcPort(conf);
    this.username = ConfigHelper.getOutputKeyspaceUserName(conf);
    this.password = ConfigHelper.getOutputKeyspacePassword(conf);

  }

  @Override
  public void init(String keyspace)
  {
    Set<InetAddress> hosts = new HashSet<>();
    String[] nodes = hostlist.split(",");
    for (String node : nodes)
    {
      try
      {
        hosts.add(InetAddress.getByName(node));
      }
      catch (UnknownHostException e)
      {
        throw new RuntimeException(e);
      }
    }
    Iterator<InetAddress> hostiter = hosts.iterator();
    while (hostiter.hasNext())
    {
      try
      {
        InetAddress host = hostiter.next();
        Cassandra.Client client = ConfigHelper.createConnection(conf, host.getHostAddress(), rpcPort);

        // log in
        client.set_keyspace(keyspace);
        if (username != null)
        {
          Map<String, String> creds = new HashMap<>();
          creds.put(IAuthenticator.USERNAME_KEY, username);
          creds.put(IAuthenticator.PASSWORD_KEY, password);
          AuthenticationRequest authRequest = new AuthenticationRequest(creds);
          client.login(authRequest);
        }

        List<TokenRange> tokenRanges = client.describe_ring(keyspace);

        setPartitioner(client.describe_partitioner());
        Token.TokenFactory tkFactory = getPartitioner().getTokenFactory();

        for (TokenRange tr : tokenRanges)
        {
          Range<Token> range = new Range<>(tkFactory.fromString(tr.start_token), tkFactory.fromString(tr.end_token));
          for (String ep : tr.endpoints)
          {
            addRangeForEndpoint(range, InetAddress.getByName(ep));
          }
        }

        String cfQuery = String.format("SELECT * FROM %s.%s WHERE keyspace_name = '%s'",
                Keyspace.SYSTEM_KS,
                SystemKeyspace.SCHEMA_COLUMNFAMILIES_CF,
                keyspace);
        CqlResult cfRes = client.execute_cql3_query(ByteBufferUtil.bytes(cfQuery), Compression.NONE, ConsistencyLevel.ONE);


        for (CqlRow row : cfRes.rows)
        {
          String columnFamily = UTF8Type.instance.getString(row.columns.get(1).bufferForName());
          String columnsQuery = String.format("SELECT * FROM %s.%s WHERE keyspace_name = '%s' AND columnfamily_name = '%s'",
                  Keyspace.SYSTEM_KS,
                  SystemKeyspace.SCHEMA_COLUMNS_CF,
                  keyspace,
                  columnFamily);
          CqlResult columnsRes = client.execute_cql3_query(ByteBufferUtil.bytes(columnsQuery), Compression.NONE, ConsistencyLevel.ONE);

          CFMetaData metadata = CFMetaData.fromThriftCqlRow(row, columnsRes);
          knownCfs.put(metadata.cfName, metadata);
        }
        break;
      }
      catch (Exception e)
      {
        if (!hostiter.hasNext())
          throw new RuntimeException("Could not retrieve endpoint ranges: ", e);
      }
    }
  }

  @Override
  public CFMetaData getCFMetaData(String keyspace, String cfName) {
    return knownCfs.get(cfName);
  }
}
