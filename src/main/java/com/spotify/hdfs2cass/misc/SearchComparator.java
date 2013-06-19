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

import org.apache.cassandra.dht.BigIntegerToken;

import java.math.BigInteger;
import java.util.Comparator;

/*
 * Copyright (c) 2013 Spotify AB
 *
 */

/**
 * Comparator used for binary search
 *
 * @author anand
 */
public class SearchComparator implements Comparator<TokenNode> {

  private static final BigIntegerToken ZERO = new BigIntegerToken(BigInteger.ZERO);

  @Override
  public int compare(TokenNode first, TokenNode second) {
    if (first.equals(second)) {
      return 0; // the tokens are equal
    }

    final BigIntegerToken firstStartToken = first.getStartToken();
    final BigIntegerToken firstEndToken = first.getEndToken();

    final BigIntegerToken secondStartToken = second.getStartToken();
    final BigIntegerToken secondEndToken = second.getEndToken();

    if (firstStartToken.compareTo(secondStartToken) > 0) {
      return 1; // first start > second start
    }

    // covers the cases:
    //  second.start >= first.start
    //  second.start <= first.end
    //  second.end <= first.end || first.end < first.start
    if (secondStartToken.compareTo(firstStartToken) >= 0) {
      if ((secondStartToken.compareTo(firstEndToken) <= 0)
          && (secondEndToken.compareTo(firstEndToken) <= 0)) {
        return 0;
      } else {
        // special case: first.end = 0
        if (firstEndToken.compareTo(ZERO) == 0) {
          return 0;
        }
      }
    }

    // the tokens wrap around
    if (firstEndToken.compareTo(firstStartToken) < 0) {
      if (secondEndToken.compareTo(firstEndToken) <= 0) {
        return 0;
      }

      if ((secondEndToken.compareTo(firstStartToken) > 0)
          && (secondEndToken.compareTo(firstEndToken) > 0)) {
        return 0;
      }
    }

    return -1;
  }
}
