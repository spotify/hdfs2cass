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
import org.apache.commons.lang.StringUtils;

/**
 * @author anand
 */
public class TokenNode {
  private final BigIntegerToken startToken;
  private final BigIntegerToken endToken;

  public TokenNode(final BigIntegerToken token) {
    startToken = token;
    endToken = token;
  }

  public TokenNode(final String encodedStr) {
    final String[] parts = StringUtils.splitByWholeSeparatorPreserveAllTokens(encodedStr, ":");
    if ((parts == null) || (parts.length != 2)) {
      throw new RuntimeException("Unable to decode " + encodedStr);
    }

    startToken = new BigIntegerToken(parts[0]);
    endToken = new BigIntegerToken(parts[1]);
  }

  public BigIntegerToken getStartToken() {
    return startToken;
  }

  public BigIntegerToken getEndToken() {
    return endToken;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    TokenNode tokenNode = (TokenNode) o;
    return endToken.equals(tokenNode.endToken) && startToken.equals(tokenNode.startToken);

  }

  @Override
  public int hashCode() {
    int result = startToken.hashCode();
    result = 31 * result + endToken.hashCode();
    return result;
  }

  @Override
  public String toString() {
    return "TokenNode{" +
        "startToken=" + startToken +
        ", endToken=" + endToken +
        '}';
  }
}
