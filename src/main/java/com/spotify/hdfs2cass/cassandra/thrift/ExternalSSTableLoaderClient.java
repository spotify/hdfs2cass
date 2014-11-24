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
 * https://github.com/apache/cassandra/blob/trunk/src/java/org/apache/cassandra/hadoop/BulkRecordWriter.java
 */
package com.spotify.hdfs2cass.cassandra.thrift;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.cassandra.auth.IAuthenticator;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.SSTableLoader;
import org.apache.cassandra.thrift.AuthenticationRequest;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.KsDef;
import org.apache.cassandra.thrift.TokenRange;
import org.apache.crunch.CrunchRuntimeException;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * This is an almost-copy of {@link org.apache.cassandra.hadoop.AbstractBulkRecordWriter.ExternalClient}
 * <p>
 * We had to re-implement this class because of https://issues.apache.org/jira/browse/CASSANDRA-8367
 * </p>
 */
public class ExternalSSTableLoaderClient extends SSTableLoader.Client {
  private final Map<String, Map<String, CFMetaData>> knownCfs = new HashMap<>();
  private final String hostlist;
  private final int rpcPort;
  private final String username;
  private final String password;

  public ExternalSSTableLoaderClient(String hostlist, int port, String username, String password) {
    super();
    this.hostlist = hostlist;
    this.rpcPort = port;
    this.username = username;
    this.password = password;
  }

  public void init(String keyspace) {
    Set<InetAddress> hosts = Sets.newHashSet();
    String[] nodes = hostlist.split(",");
    for (String node : nodes) {
      try {
        hosts.add(InetAddress.getByName(node));
      } catch (UnknownHostException e) {
        throw new RuntimeException(e);
      }
    }

    Iterator<InetAddress> hostiter = hosts.iterator();
    while (hostiter.hasNext()) {
      try {
        InetAddress host = hostiter.next();
        Cassandra.Client client = createThriftClient(host.getHostAddress(), rpcPort);

        // log in
        client.set_keyspace(keyspace);
        if (username != null) {
          Map<String, String> creds = Maps.newHashMap();
          creds.put(IAuthenticator.USERNAME_KEY, username);
          creds.put(IAuthenticator.PASSWORD_KEY, password);
          AuthenticationRequest authRequest = new AuthenticationRequest(creds);
          client.login(authRequest);
        }

        List<TokenRange> tokenRanges = client.describe_ring(keyspace);
        List<KsDef> ksDefs = client.describe_keyspaces();

        setPartitioner(client.describe_partitioner());
        Token.TokenFactory tkFactory = getPartitioner().getTokenFactory();

        for (TokenRange tr : tokenRanges) {
          Range<Token> range = new Range<>(tkFactory.fromString(tr.start_token), tkFactory.fromString(tr.end_token));
          for (String ep : tr.endpoints) {
            addRangeForEndpoint(range, InetAddress.getByName(ep));
          }
        }

        for (KsDef ksDef : ksDefs) {
          Map<String, CFMetaData> cfs = new HashMap<>(ksDef.cf_defs.size());
          for (CfDef cfDef : ksDef.cf_defs)
            cfs.put(cfDef.name, CFMetaData.fromThrift(cfDef));
          knownCfs.put(ksDef.name, cfs);
        }
        break;
      } catch (Exception e) {
        throw new CrunchRuntimeException("Could not retrieve endpoint ranges: ", e);
      }
    }
  }

  public CFMetaData getCFMetaData(String keyspace, String cfName) {
    Map<String, CFMetaData> cfs = knownCfs.get(keyspace);
    return cfs != null ? cfs.get(cfName) : null;
  }

  private static Cassandra.Client createThriftClient(String host, int port) throws TTransportException {
    TSocket socket = new TSocket(host, port);
    TTransport trans = new TFramedTransport(socket);
    trans.open();
    TProtocol protocol = new org.apache.thrift.protocol.TBinaryProtocol(trans);
    return new Cassandra.Client(protocol);
  }
}
