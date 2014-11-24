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
package com.spotify.hdfs2cass;

import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.crunch.CrunchRuntimeException;
import org.joda.time.DateTimeUtils;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;

/**
 * Represents tab-separated input line.
 *
 * <p>
 * This used to be the only supported format of hdfs2cass. Now it's deprecated and should not be
 * used. The format of a line is:
 *
 *     Hdfs2Cassandra\t<version>\t<rowkey>\t<colname>\t[<timestamp>]\t[<ttl>]\t<value>
 *
 * - timestamp and ttl are optional
 * - version 1 means timestamp and ttl is not present
 * - version 2 means ttl is not present
 * - version 3 means all fields are present
 * </p>
 */
public class LegacyInputFormat {

  private final String rowkey;
  private final String colname;
  private final String colvalue;
  private final long timestamp;
  private final long ttl;

  public LegacyInputFormat(String rowkey, String colname, String colvalue, long timestamp,
      long ttl) {
    this.rowkey = rowkey;
    this.colname = colname;
    this.colvalue = colvalue;
    this.timestamp = timestamp;
    this.ttl = ttl;
  }

  public static LegacyInputFormat parse (ByteBuffer row) {
    try {
      return parse(ByteBufferUtil.string(row));
    } catch (CharacterCodingException e) {
      throw new CrunchRuntimeException(e);
    }
  }

  public static LegacyInputFormat parse(String row) {
    String[] parts = row.split("\t");
    String rowkey = parts[2];
    String colname = parts[3];
    String value;
    long ts = DateTimeUtils.currentTimeMillis();
    long ttl = 0;
    if (!parts[0].equals("HdfsToCassandra")) {
      throw new CrunchRuntimeException("Found malformed row. The rows must start with 'HdfsToCassandra'");
    }
    switch (Integer.valueOf(parts[1])) {
      case 1:
        if (parts.length != 5) {
          throw new CrunchRuntimeException("Found malformed row. Check correct row format.");
        }
        value = parts[4];
        break;
      case 2:
        if (parts.length != 6) {
          throw new CrunchRuntimeException("Found malformed row. Check correct row format.");
        }
        ts = parseNumber(parts[4]);
        value = parts[5];
        break;
      case 3:
        if (parts.length != 7) {
          throw new CrunchRuntimeException("Found malformed row. Check correct row format.");
        }
        ts = parseNumber(parts[4]);
        ttl = parseNumber(parts[5]);
        value = parts[6];
        break;
      default:
        throw new CrunchRuntimeException("Unknown format version");
    }
    return new LegacyInputFormat(rowkey, colname, value, ts, ttl);
  }

  public String getRowkey() {
    return rowkey;
  }

  public String getColname() {
    return colname;
  }

  public String getColval() {
    return colvalue;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public long getTtl() {
    return ttl;
  }

  private static long parseNumber(String str) throws CrunchRuntimeException {
    try {
      return Integer.valueOf(str);
    } catch (NumberFormatException e) {
      throw new CrunchRuntimeException(e);
    }
  }

}
