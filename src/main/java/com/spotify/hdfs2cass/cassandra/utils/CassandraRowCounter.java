package com.spotify.hdfs2cass.cassandra.utils;

public enum CassandraRowCounter {

  ROWS_ADDED("Rows Added");
  private final String text;

  private CassandraRowCounter(final String text) {
    this.text = text;
  }

  @Override
  public String toString() {
    return this.text;
  }
}
