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
