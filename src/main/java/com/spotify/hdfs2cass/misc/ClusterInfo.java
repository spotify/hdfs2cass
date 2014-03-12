// Copyright (c) 2013 Spotify AB
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy of
// the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations under
// the License.

package com.spotify.hdfs2cass.misc;

import org.apache.cassandra.auth.IAuthenticator;
import org.apache.cassandra.thrift.AuthenticationRequest;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.TokenRange;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.mapred.JobConf;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import java.util.*;

/*
 * Copyright (c) 2013 Spotify AB
 *
 */

/**
 * Retrieves information from the specified cassandra cluster
 *
 * @author anand
 */
public class ClusterInfo {

  public static final String SPOTIFY_CASSANDRA_TOKENS_PARAM = "spotify.cassandra.tokens";
  public static final String SPOTIFY_CASSANDRA_PARTITIONER_PARAM = "spotify.cassandra.partitioner";
  private final String hosts;
  private final int port;
  private final String username;
  private final String password;
  private final List<String> tokenRanges;
  private String partitionerClass;
  private int numClusterNodes;

  public ClusterInfo(final String hosts,
                     final String port) {
    this(hosts, port, null, null);
  }

  public ClusterInfo(final String hosts,
                     final String port,
                     final String username,
                     final String password) {
    this.hosts = hosts;
    this.port = Integer.valueOf(port);
    this.username = username;
    this.password = password;

    this.tokenRanges = new ArrayList<String>();
  }

  /**
   * Query the cluster and get info
   *
   * @param keyspace Keyspace to use
   * @return false in case of any errors connecting to the Cassandra cluster
   */
  public Void init(final String keyspace) {
    try {
      final Cassandra.Client client = createClient();

      // get the partitioner class that the cluster uses
      partitionerClass = client.describe_partitioner();

      // get the unique list of endpoints
      final Set<String> endpoints = new HashSet<String>();
      for (TokenRange tr : client.describe_ring(keyspace)) {
        if (!tr.endpoints.isEmpty()) {
          for (String endpoint : tr.endpoints) {
            endpoints.add(endpoint);
          }

          getTokenRanges().add(String.format("%s:%s", tr.start_token, tr.end_token));
        }
      }

      // the list of nodes currently in the cluster
      numClusterNodes = endpoints.size();
    } catch (Exception e) {
      // Don't let exceptions propagate to caller
      throw new RuntimeException("Failed to get information about the cluster", e);
    }

    if ((partitionerClass == null) || partitionerClass.isEmpty() || (numClusterNodes == 0)) {
      throw new RuntimeException("Got invalid information about the cluster");
    }

    return null;
  }

  /**
   * The partitioner of the cluster
   *
   * @return The class name of the partitioner or null, if error
   */
  public String getPartitionerClass() {
    return partitionerClass;
  }

  /**
   * The number of nodes participating in the cluster
   *
   * @return The number of nodes or zero, if error
   */
  public int getNumClusterNodes() {
    return numClusterNodes;
  }

  /**
   * Adds the tokens ranges so that individual nodes know about the Cassandra layout
   *
   * @param conf The config object
   * @return true if the Cassandra cluster could be queried, false otherwise
   */
  public boolean setConf(final JobConf conf) {
    if (hasTokenRanges()) {
      final String tokenRangesStr = StringUtils.join(getTokenRanges(), ",");
      conf.set(SPOTIFY_CASSANDRA_TOKENS_PARAM, tokenRangesStr);
      conf.set(SPOTIFY_CASSANDRA_PARTITIONER_PARAM, partitionerClass);

      return true;
    }

    return false;
  }

  /**
   * Creates a thrift client to the cluster
   *
   * @return Thrift client instance
   * @throws Exception
   */
  Cassandra.Client createClient() throws Exception {

    final TTransport tTransport = getOpenTTransport();

    final Cassandra.Client client = new Cassandra.Client(new TBinaryProtocol(tTransport));
    if ((username != null) && (password != null)) {
      final Map<String, String> credentials = new HashMap<String, String>();
      credentials.put(IAuthenticator.USERNAME_KEY, username);
      credentials.put(IAuthenticator.PASSWORD_KEY, password);

      client.login(new AuthenticationRequest(credentials));
    }

    return client;
  }

  TTransport getOpenTTransport() throws Exception {
      LinkedList<String> hosts = new LinkedList(Arrays.asList(this.hosts.split(",")));
      Collections.shuffle(hosts);
      while (!hosts.isEmpty()) {
          try {
              final TTransport tTransport = new TFramedTransport(new TSocket(hosts.remove(), port));
              tTransport.open();
              return tTransport;
          } catch (TTransportException e) {
              System.err.println("Could not connect to Cassandra host");
          }
      }
      throw new Exception("Could not open connection to Cassandra, tried all: " + this.hosts);
  }

  List<String> getTokenRanges() {
    return tokenRanges;
  }

  boolean hasTokenRanges() {
    return !tokenRanges.isEmpty();
  }
}
