package com.spotify.hdfs2cass.cassandra.utils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.apache.cassandra.serializers.DecimalSerializer;
import org.apache.cassandra.serializers.FloatSerializer;
import org.apache.cassandra.serializers.Int32Serializer;
import org.apache.cassandra.serializers.ListSerializer;
import org.apache.cassandra.serializers.MapSerializer;
import org.apache.cassandra.serializers.SetSerializer;
import org.apache.cassandra.serializers.UTF8Serializer;
import org.junit.Test;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;

public class CassandraRecordUtilsTest {

  @Test
  public void testSerializeMap() {
    Map<String, Integer> map = ImmutableMap.of("foo", 1, "bar", 2);

    ByteBuffer expected =
        MapSerializer.getInstance(UTF8Serializer.instance, Int32Serializer.instance).serialize(map);
    assertEquals(expected, CassandraRecordUtils.toByteBuffer(map));
  }

  @Test
  public void testSerializeList() {
    List<BigDecimal> list = ImmutableList.of(BigDecimal.valueOf(0),
                                             new BigDecimal("1.2"),
                                             new BigDecimal("3.4"));

    ByteBuffer expected = ListSerializer.getInstance(DecimalSerializer.instance).serialize(list);
    assertEquals(expected, CassandraRecordUtils.toByteBuffer(list));
  }

  @Test
  public void testSerializeSet() {
    Set<Float> set = ImmutableSet.of(1.0f, 2.0f, 3.0f);
    ByteBuffer expected = SetSerializer.getInstance(FloatSerializer.instance).serialize(set);
    assertEquals(expected, CassandraRecordUtils.toByteBuffer(set));
  }

}