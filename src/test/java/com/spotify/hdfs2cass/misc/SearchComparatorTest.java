package com.spotify.hdfs2cass.misc;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * @author anand
 */
public class SearchComparatorTest {

  @Test
  public void testCompare() throws Exception {
    SearchComparator comparator = new SearchComparator();
    TokenNode instance = new TokenNode("2:5");

    assertEquals(1, comparator.compare(instance, new TokenNode(("0:1"))));
    assertEquals(1, comparator.compare(instance, new TokenNode(("1:3"))));
    assertEquals(0, comparator.compare(instance, new TokenNode(("3:4"))));
    assertEquals(0, comparator.compare(instance, new TokenNode(("2:5"))));
    assertEquals(0, comparator.compare(instance, new TokenNode(("3:5"))));
    assertEquals(0, comparator.compare(instance, new TokenNode(("3:0"))));
    assertEquals(-1, comparator.compare(instance, new TokenNode(("3:6"))));
    assertEquals(-1, comparator.compare(instance, new TokenNode(("6:8"))));
  }

  @Test
  public void testCompareWraparound() throws Exception {
    SearchComparator comparator = new SearchComparator();
    TokenNode instance = new TokenNode("5:2");

    assertEquals(1, comparator.compare(instance, new TokenNode(("0:1"))));
    assertEquals(1, comparator.compare(instance, new TokenNode(("3:4"))));
    assertEquals(1, comparator.compare(instance, new TokenNode(("2:5"))));
    assertEquals(1, comparator.compare(instance, new TokenNode(("3:0"))));
    assertEquals(0, comparator.compare(instance, new TokenNode(("5:6"))));
    assertEquals(0, comparator.compare(instance, new TokenNode(("6:8"))));
    assertEquals(0, comparator.compare(instance, new TokenNode(("7:1"))));
    assertEquals(-1, comparator.compare(instance, new TokenNode(("6:3"))));
    assertEquals(-1, comparator.compare(instance, new TokenNode(("8:4"))));
  }
}
