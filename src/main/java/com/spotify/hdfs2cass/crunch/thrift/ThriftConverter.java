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
package com.spotify.hdfs2cass.crunch.thrift;

import org.apache.cassandra.thrift.Mutation;
import org.apache.crunch.Pair;
import org.apache.crunch.types.Converter;

import java.nio.ByteBuffer;
import java.util.Collection;

public class ThriftConverter implements Converter<ByteBuffer, Collection<Mutation>, Pair<ByteBuffer, Collection<Mutation>>, Pair<ByteBuffer, Iterable<Collection<Mutation>>>> {
  @Override
  public Pair<ByteBuffer, Collection<Mutation>> convertInput(final ByteBuffer key, final Collection<Mutation> value) {
    return Pair.of(key, value);
  }

  @Override
  public Pair<ByteBuffer, Iterable<Collection<Mutation>>> convertIterableInput(final ByteBuffer key, final Iterable<Collection<Mutation>> value) {
    return Pair.of(key, value);
  }

  @Override
  public ByteBuffer outputKey(final Pair<ByteBuffer, Collection<Mutation>> value) {
    return value.first();
  }

  @Override
  public Collection<Mutation> outputValue(final Pair<ByteBuffer, Collection<Mutation>> value) {
    return value.second();
  }

  @Override
  public Class<ByteBuffer> getKeyClass() {
    return ByteBuffer.class;
  }

  @Override
  public Class<Collection<Mutation>> getValueClass() {
    return (Class<Collection<Mutation>>) (Class<?>) Collection.class;
  }

  @Override
  public boolean applyPTypeTransforms() {
    return false;
  }
}
