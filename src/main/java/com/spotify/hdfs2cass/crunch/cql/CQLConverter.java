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
package com.spotify.hdfs2cass.crunch.cql;

import org.apache.crunch.Pair;
import org.apache.crunch.types.Converter;

import java.nio.ByteBuffer;

public class CQLConverter implements Converter<ByteBuffer, CQLRecord, Pair<ByteBuffer, CQLRecord>, Pair<ByteBuffer, Iterable<CQLRecord>>> {

  @Override
  public Pair<ByteBuffer, CQLRecord> convertInput(final ByteBuffer k, final CQLRecord v) {
    return Pair.of(k, v);
  }

  @Override
  public Pair<ByteBuffer, Iterable<CQLRecord>> convertIterableInput(
      final ByteBuffer k,
      final Iterable<CQLRecord> v) {
    return Pair.of(k, v);
  }

  @Override
  public ByteBuffer outputKey(final Pair<ByteBuffer, CQLRecord> value) {
    return value.first();
  }

  @Override
  public CQLRecord outputValue(final Pair<ByteBuffer, CQLRecord> value) {
    return value.second();
  }

  @Override
  public Class<ByteBuffer> getKeyClass() {
    return ByteBuffer.class;
  }

  @Override
  public Class<CQLRecord> getValueClass() {
    return CQLRecord.class;
  }

  @Override
  public boolean applyPTypeTransforms() {
    return false;
  }
}
