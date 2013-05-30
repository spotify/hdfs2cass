package com.spotify.hdfs2cass.misc;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.TokenRange;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.mapred.JobConf;
import org.junit.Test;
import org.mockito.Matchers;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/*
 * Copyright (c) 2013 Spotify AB
 *
 */

/**
 *
 * @author anand
 */
public class ClusterInfoTest {

  @Test
  public void testGetInfo() throws Exception {
    final ClusterInfo clusterInfo = createClusterInfo();
    clusterInfo.init("foo");

    assertEquals("partitioner", clusterInfo.getPartitionerClass());
    assertEquals(4, clusterInfo.getNumClusterNodes());
  }

  @Test
  public void testSetConf() throws Exception {
    final ClusterInfo clusterInfo = createClusterInfo();
    clusterInfo.init("foo");

    final JobConf conf = new JobConf();
    clusterInfo.setConf(conf);

    final String result = conf.get(ClusterInfo.SPOTIFY_CASSANDRA_TOKENS_PARAM);
    assertEquals(4, StringUtils.splitPreserveAllTokens(result, ",").length);
  }

  private ClusterInfo createClusterInfo() throws Exception {
    final List<TokenRange> tokenRanges = new ArrayList<TokenRange>();
    addTokenRanges(tokenRanges, "1", "5", "a", "b", "c");
    addTokenRanges(tokenRanges, "5", "10", "b", "c", "d");
    addTokenRanges(tokenRanges, "10", "15", "c", "d", "a");
    addTokenRanges(tokenRanges, "15", "20", "d", "a", "b");

    final Cassandra.Client client = mock(Cassandra.Client.class);
    when(client.describe_partitioner()).thenReturn("partitioner");
    when(client.describe_ring(Matchers.anyString())).thenReturn(tokenRanges);

    final ClusterInfo clusterInfo = mock(ClusterInfo.class);
    when(clusterInfo.getNumClusterNodes()).thenCallRealMethod();
    when(clusterInfo.getPartitionerClass()).thenCallRealMethod();
    when(clusterInfo.init(Matchers.anyString())).thenCallRealMethod();
    when(clusterInfo.setConf(Matchers.any(JobConf.class))).thenCallRealMethod();
    when(clusterInfo.createClient()).thenReturn(client);
    when(clusterInfo.getTokenRanges()).thenReturn(new ArrayList<String>());
    when(clusterInfo.hasTokenRanges()).thenReturn(Boolean.TRUE);

    return clusterInfo;
  }

  private void addTokenRanges(final List<TokenRange> tokenRanges,
                              final String startToken,
                              final String endToken,
                              String... endpoints) {
    tokenRanges.add(new TokenRange(startToken, endToken, Arrays.asList(endpoints)));
  }
}
