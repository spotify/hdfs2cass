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
import java.util.Collection;

public class CQLConverter implements Converter<Object, Collection<ByteBuffer>, Pair<Object, Collection<ByteBuffer>>, Pair<Object, Iterable<Collection<ByteBuffer>>>> {

  @Override
  public Pair<Object, Collection<ByteBuffer>> convertInput(Object k, Collection<ByteBuffer> v) {
    return Pair.of(k, v);
  }

  @Override
  public Pair<Object, Iterable<Collection<ByteBuffer>>> convertIterableInput(Object k, Iterable<Collection<ByteBuffer>> v) {
    return Pair.of(k, v);
  }

  @Override
  public Object outputKey(Pair<Object, Collection<ByteBuffer>> value) {
    return value.first();
  }

  @Override
  public Collection<ByteBuffer> outputValue(Pair<Object, Collection<ByteBuffer>> value) {
    return value.second();
  }

  @Override
  public Class<Object> getKeyClass() {
    return Object.class;
  }

  @Override
  public Class<Collection<ByteBuffer>> getValueClass() {
    return (Class<Collection<ByteBuffer>>) (Class<?>) Collection.class;
  }

  @Override
  public boolean applyPTypeTransforms() {
    return false;
  }
}
