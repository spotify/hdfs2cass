package com.spotify.hdfs2cass.misc;

import org.apache.cassandra.dht.BigIntegerToken;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

/**
 * @author anand
 */
public class TokenNodeTest {

  @Test
  public void testConstructor() throws Exception {
    final BigIntegerToken token = new BigIntegerToken("1");
    TokenNode instance = new TokenNode(token);

    assertEquals(token, instance.getStartToken());
    assertEquals(token, instance.getEndToken());
  }

  @Test
  public void testStringConstructor() throws Exception {
    TokenNode instance = new TokenNode("1:2");

    assertEquals(new BigIntegerToken("1"), instance.getStartToken());
    assertEquals(new BigIntegerToken("2"), instance.getEndToken());
  }

  @Test
  public void testEquals() throws Exception {
    TokenNode instance = new TokenNode("1:2");

    assertEquals(new TokenNode("1:2"), instance);
    assertNotEquals(new TokenNode("2:1"), instance);
  }
}
