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

import org.apache.crunch.CrunchRuntimeException;
import org.joda.time.DateTimeUtils;
import org.junit.Test;

import static org.junit.Assert.*;

public class LegacyInputFormatTest {

  @Test
  public void testParseValid() throws Exception {

    DateTimeUtils.setCurrentMillisFixed(42l);

    String v1 = "HdfsToCassandra\t1\tkey\tcolName\tvalue";
    LegacyInputFormat r1 = LegacyInputFormat.parse(v1);
    assertEquals("key", r1.getRowkey());
    assertEquals("colName", r1.getColname());
    assertEquals("value", r1.getColval());
    assertEquals(42l, r1.getTimestamp());
    assertEquals(0, r1.getTtl());

    String v2 = "HdfsToCassandra\t2\tkey\tcolName\t23\tvalue";
    r1 = LegacyInputFormat.parse(v2);
    assertEquals("key", r1.getRowkey());
    assertEquals("colName", r1.getColname());
    assertEquals("value", r1.getColval());
    assertEquals(23l, r1.getTimestamp());
    assertEquals(0, r1.getTtl());

    String v3 = "HdfsToCassandra\t3\tkey\tcolName\t23\t666\tvalue";
    r1 = LegacyInputFormat.parse(v3);
    assertEquals("key", r1.getRowkey());
    assertEquals("colName", r1.getColname());
    assertEquals("value", r1.getColval());
    assertEquals(23l, r1.getTimestamp());
    assertEquals(666, r1.getTtl());
  }

  @Test(expected = CrunchRuntimeException.class)
  public void testParseInvalidTooFew() {
    String v1 = "HdfsToCassandra\t1\tkey\tcolName";
    LegacyInputFormat.parse(v1);
  }

  @Test(expected = CrunchRuntimeException.class)
  public void testParseInvalidTooManyV1() {
    String v1 = "HdfsToCassandra\t1\tkey\tcolName\tvalue\tfoo";
    LegacyInputFormat.parse(v1);
  }

  @Test(expected = CrunchRuntimeException.class)
  public void testParseInvalidTooManyV2() {
    String v1 = "HdfsToCassandra\t2\tkey\tcolName\t23\tvalue\tfoo";
    LegacyInputFormat.parse(v1);
  }

  @Test(expected = CrunchRuntimeException.class)
  public void testParseInvalidTooManyV3() {
    String v1 = "HdfsToCassandra\t3\tkey\tcolName\t23\t666\tvalue\tfoo";
    LegacyInputFormat.parse(v1);
  }

  @Test(expected = CrunchRuntimeException.class)
  public void testParseInvalidNumberFormat() {
    String v1 = "HdfsToCassandra\t3\tkey\tcolName\t2a3\t666\tvalue";
    LegacyInputFormat.parse(v1);
  }

}
