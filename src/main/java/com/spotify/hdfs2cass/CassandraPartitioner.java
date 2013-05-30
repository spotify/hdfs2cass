package com.spotify.hdfs2cass;

import com.spotify.hdfs2cass.misc.ClusterInfo;
import com.spotify.hdfs2cass.misc.SearchComparator;
import com.spotify.hdfs2cass.misc.TokenNode;
import org.apache.cassandra.dht.AbstractPartitioner;
import org.apache.cassandra.dht.BigIntegerToken;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;

import java.nio.ByteBuffer;
import java.util.*;

/*
 * Copyright (c) 2013 Spotify AB
 *
 */

/**
 * Uses the cassandra topology to send a key to a particular set of reducers
 *
 * @author anand
 */
class CassandraPartitioner implements Partitioner<Text, Text> {

  private final static Random RANDOM;
  private final static SearchComparator SEARCH_COMPARATOR;
  private List<TokenNode> tokenNodes;
  private AbstractPartitioner<BigIntegerToken> partitioner;

  static {
    RANDOM = new Random();
    SEARCH_COMPARATOR = new SearchComparator();
  }

  public CassandraPartitioner() {
  }

  @Override
  public int getPartition(Text key, Text value, int numReducers) {
    final int partition;

    final BigIntegerToken token = partitioner.getToken(ByteBuffer.wrap(key.getBytes()));

    final int index = Collections.binarySearch(tokenNodes, new TokenNode(token), SEARCH_COMPARATOR);
    if (index >= 0) {
      final int multiple = numReducers / tokenNodes.size();
      partition = index + (multiple * RANDOM.nextInt(multiple));
    } else {
      throw new RuntimeException("Failed to find a node for token " + token);
    }

    return partition;
  }

  @SuppressWarnings("unchecked")
  @Override
  public void configure(JobConf entries) {
    final String partitionerParam = entries.get(ClusterInfo.SPOTIFY_CASSANDRA_PARTITIONER_PARAM);
    if (partitionerParam == null) {
      throw new RuntimeException("Didn't get any cassandra partitioner information");
    }

    try {
      partitioner = (AbstractPartitioner<BigIntegerToken>) Class.forName(partitionerParam).newInstance();
    } catch (Exception ex) {
      throw new RuntimeException("Invalid partitioner class name: " + partitionerParam);
    }

    final String tokenNodesParam = entries.get(ClusterInfo.SPOTIFY_CASSANDRA_TOKENS_PARAM);
    if (tokenNodesParam == null) {
      throw new RuntimeException("Didn't get any cassandra information");
    }

    final String[] parts = StringUtils.splitByWholeSeparatorPreserveAllTokens(tokenNodesParam, ",");
    if ((parts == null) || (parts.length == 0)) {
      throw new RuntimeException("Didn't get any valid cassandra nodes information");
    }

    tokenNodes = new ArrayList<TokenNode>();
    for (String part : parts) {
      tokenNodes.add(new TokenNode(part));
    }

    Collections.sort(tokenNodes, new Comparator<TokenNode>() {
      @Override
      public int compare(TokenNode o1, TokenNode o2) {
        if (o1.equals(o2)) {
          return 0;
        }

        return o1.getStartToken().compareTo(o2.getStartToken());
      }
    });
  }
}
