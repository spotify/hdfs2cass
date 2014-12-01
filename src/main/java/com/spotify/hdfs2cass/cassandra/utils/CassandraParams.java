
package com.spotify.hdfs2cass.cassandra.utils;

import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.collect.Maps;
import com.spotify.hdfs2cass.crunch.CrunchConfigHelper;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.RandomPartitioner;
import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.cassandra.tools.BulkLoader;
import org.apache.commons.lang.StringUtils;
import org.apache.crunch.GroupingOptions;
import org.apache.hadoop.mapred.JobConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.math.BigInteger;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import java.util.Random;

public class CassandraParams implements Serializable {
  private static final Logger logger = LoggerFactory.getLogger(CassandraParams.class);

  public static final String SCRUB_CASSANDRACLUSTER_PARTITIONER_CONFIG = "scrub.cassandracluster.com.spotify.cassandra.thrift.partitioner";
  public static final String SCRUB_CASSANDRACLUSTER_RANGE_PER_REDUCER_CONFIG = "scrub.cassandracluster.com.spotify.cassandra.thrift.rangeperreducer";
  public static final String SCRUB_CASSANDRACLUSTER_REDUCERS_CONFIG = "scrub.cassandracluster.com.spotify.cassandra.thrift.reducers";
  public static final String SCRUB_CASSANDRACLUSTER_DISTRIBUTE_RANDOMLY_CONFIG = "scrub.cassandracluster.com.spotify.cassandra.thrift.distributerandomly";

  private CassandraClusterInfo clusterInfo;

  private String seedNodeHost;
  private int seedNodePort;
  private String columnFamily;
  private String keyspace;
  private Optional<Integer> rpcPort = Optional.absent();

  private String partitioner;

  private int bufferSize = 64;
  private Optional<Integer> streamThrottleMBits = Optional.absent();
  private Optional<String> compressionClass = Optional.absent();
  private Optional<Integer> mappers = Optional.absent();
  private int reducers = 0;
  private Optional<Integer> copiers = Optional.absent();
  private boolean distributeRandomly = false;
  private String schema;
  private String statement;

  /**
   * Configures CassandraProvider based on the target hdfs2cass resource URI.
   * The URI has schema:
   * (thrift|cql)://seedNodeHost[:port]/keySpace/colFamily?query_string
   * query_string keys:
   * - buffersize
   * - columnnames
   * - compressionclass
   * - copiers
   * - distributerandomly
   * - mappers
   * - reducers
   * - streamthrottlembits
   * - rpcport
   */
  private CassandraParams() {
  }

  public static CassandraParams parse(URI dataResource) {
    String queryString = Objects.firstNonNull(dataResource.getQuery(), "");
    Map<String, String> query = parseQuery(queryString);

    CassandraParams params = new CassandraParams();
    params.seedNodeHost = dataResource.getHost();
    params.seedNodePort = dataResource.getPort();
    String[] path = dataResource.getPath().split("/");
    params.keyspace = path[1];
    params.columnFamily = path[2];

    params.clusterInfo = new CassandraClusterInfo(params.seedNodeHost, params.seedNodePort);
    params.clusterInfo.init(params.keyspace, params.columnFamily);
    params.partitioner = params.clusterInfo.getPartitionerClass();

    params.schema = params.clusterInfo.getCqlSchema();
    if (query.containsKey("columnnames")) {
      String[] columnNames = query.get("columnnames").split(",");
      params.statement = params.clusterInfo.buildPreparedStatement(columnNames);
    } else {
      params.statement = params.clusterInfo.inferPreparedStatement();
    }

    if (query.containsKey("buffersize")) {
      params.bufferSize = Integer.parseInt(query.get("buffersize"));
    }

    if (query.containsKey("streamthrottlembits")) {
      params.streamThrottleMBits = Optional.of(Integer.parseInt(query.get("streamthrottlembits")));
    }

    if (query.containsKey("compressionclass")) {
      params.compressionClass = Optional.of(query.get("compressionclass"));
    }

    if (query.containsKey("mappers")) {
      params.mappers = Optional.of(Integer.parseInt(query.get("mappers")));
    }

    if (query.containsKey("reducers")) {
      params.reducers = Integer.parseInt(query.get("reducers"));
    } else {
      params.reducers = params.clusterInfo.getNumClusterNodes();
    }

    if (query.containsKey("copiers")) {
      params.copiers = Optional.of(Integer.parseInt(query.get("copiers")));
    }

    if (query.containsKey("distributerandomly")) {
      params.distributeRandomly = Boolean.parseBoolean(query.get("distributerandomly"));
    }

    if (query.containsKey("rpcport")) {
      params.rpcPort = Optional.of(Integer.parseInt(query.get("rpcport")));
    }

    if ("thrift".equals(dataResource.getScheme())) {
      params.clusterInfo.validateThriftAccessible(params.rpcPort);
    }

    return params;
  }

  public static Map<String, String> parseQuery(String query) {
    final Map<String, String> result = Maps.newHashMap();
    final String[] pairs = query.split("&");
    for (String pair : pairs) {
      if (pair.isEmpty())
        continue;

      final int idx = pair.indexOf("=");
      if (idx > -1) {
        result.put(pair.substring(0, idx), pair.substring(idx + 1));
      } else {
        result.put(pair, "true");
      }
    }
    return result;
  }

  public void configure(final JobConf conf) {
    ConfigHelper.setOutputInitialAddress(conf, this.getSeedNodeHost());
    CrunchConfigHelper.setOutputColumnFamily(conf, this.getKeyspace(), this.getColumnFamily());
    ConfigHelper.setOutputPartitioner(conf, this.getPartitioner());

    conf.set("mapreduce.output.bulkoutputformat.buffersize", String.valueOf(this.getBufferSize()));

    if (this.getStreamThrottleMBits().isPresent()) {
      conf.set("mapreduce.output.bulkoutputformat.streamthrottlembits", this.getStreamThrottleMBits().get().toString());
    }

    if (this.getCompressionClass().isPresent()) {
      ConfigHelper.setOutputCompressionClass(conf, this.getCompressionClass().get());
    }

    if (this.getMappers().isPresent()) {
      conf.setNumMapTasks(this.getMappers().get());
    }

    if (this.getCopiers().isPresent()) {
      conf.set("mapred.reduce.parallel.copies", String.valueOf(this.getCopiers().get()));
    }

    if (this.getRpcPort().isPresent()) {
      ConfigHelper.setOutputRpcPort(conf, String.valueOf(this.getRpcPort().get()));
    }

    conf.setJarByClass(BulkLoader.class);
  }

  /**
   * A Cassandra host used to fetch information about the Cassandra cluster.
   *
   * @return hostname
   */
  public String getSeedNodeHost() {
    return seedNodeHost;
  }

  /**
   * Cassandra column family hdfs2cass is imported to.
   *
   * @return column family name
   */
  public String getColumnFamily() {
    return columnFamily;
  }

  /**
   * Cassandra keyspace hdfs2cass is imported to.
   *
   * @return keyspace name
   */
  public String getKeyspace() {
    return keyspace;
  }

  /**
   * Cassandra partitioner the cluster is using.
   *
   * @return full class name
   */
  public String getPartitioner() {
    return partitioner;
  }

  /**
   * Size of SSTables built locally before streaming to Cassandra.
   *
   * @return size in MB
   */
  public int getBufferSize() {
    return bufferSize;
  }

  /**
   * Maximum throughput the streaming of SSTables can happen with.
   *
   * @return
   */
  public Optional<Integer> getStreamThrottleMBits() {
    return streamThrottleMBits;
  }

  /**
   * Compression used when writing SSTables.
   *
   * @return full or simple class name
   */
  public Optional<String> getCompressionClass() {
    return compressionClass;
  }

  /**
   * Number of map tasks for the import job.
   *
   * @return
   */
  public Optional<Integer> getMappers() {
    return mappers;
  }

  /**
   * Number of reducers for the import job
   *
   * @return
   */
  public int getReducers() {
    return reducers;
  }

  /**
   * Number of parallel transfers run by reduce during the copy(shuffle) phase.
   *
   * @return
   */
  public Optional<Integer> getCopiers() {
    return copiers;
  }

  /**
   * Override Cassandra partitioner and distribute hdfs2cass randomly.
   *
   * @return
   */
  public boolean getDistributeRandomly() {
    return distributeRandomly;
  }

  /**
   * If using CQL, get schema of table being imported.
   *
   * @return
   */
  public String getSchema() {
    return schema;
  }

  /**
   * If using CQL, get prepared statement for inserting values.
   *
   * @return
   */
  public String getStatement() {
    return statement;
  }

  public Optional<Integer> getRpcPort() {
    return rpcPort;
  }

  public GroupingOptions createGroupingOptions() {
    logger.info("GroupingOptions.numReducers: " + this.getReducers());
    GroupingOptions.Builder builder = GroupingOptions.builder()
        .partitionerClass(CassandraPartitioner.class)
        .numReducers(this.getReducers());

    final BigInteger maxToken;
    final BigInteger minToken;
    switch (clusterInfo.getPartitionerClass()) {
      case "org.apache.cassandra.dht.RandomPartitioner":
        maxToken = RandomPartitioner.MAXIMUM.subtract(BigInteger.ONE);
        minToken = RandomPartitioner.ZERO;
        break;
      case "org.apache.cassandra.dht.Murmur3Partitioner":
        maxToken = BigInteger.valueOf(Murmur3Partitioner.MAXIMUM);
        minToken = BigInteger.valueOf(Murmur3Partitioner.MINIMUM.token);
        break;
      default:
        throw new IllegalArgumentException("Unknown partitioner class: " + clusterInfo.getPartitionerClass());
    }

    final BigInteger[] rangeWidth = maxToken
        .subtract(minToken)
        .add(BigInteger.ONE)
        .divideAndRemainder(BigInteger.valueOf(this.getReducers()));
    if (!rangeWidth[1].equals(BigInteger.ZERO)) {
      rangeWidth[0] = rangeWidth[0].add(BigInteger.ONE);
    }
    BigInteger rangePerReducer = rangeWidth[0];

    ArrayList<Integer> reducerList = new ArrayList<>(this.getReducers());
    for (int i = 0; i < this.getReducers(); i++) {
      reducerList.add(i);
    }

    Collections.shuffle(reducerList, new Random());

    builder.conf(SCRUB_CASSANDRACLUSTER_PARTITIONER_CONFIG, clusterInfo.getPartitionerClass());
    builder.conf(SCRUB_CASSANDRACLUSTER_RANGE_PER_REDUCER_CONFIG, rangePerReducer.toString());
    builder.conf(SCRUB_CASSANDRACLUSTER_REDUCERS_CONFIG, StringUtils.join(reducerList, ","));
    if (this.getDistributeRandomly()) {
      builder.conf(SCRUB_CASSANDRACLUSTER_DISTRIBUTE_RANDOMLY_CONFIG, Boolean.TRUE.toString());
    }

    return builder.build();
  }
}
