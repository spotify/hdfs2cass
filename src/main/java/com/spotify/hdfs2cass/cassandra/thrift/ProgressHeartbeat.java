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
package com.spotify.hdfs2cass.cassandra.thrift;

import org.apache.hadoop.util.Progressable;

/**
 * Runs a heartbeat thread in the background that calls progress every SLEEP_MINS in order to keep
 * DoFns from timing out. The heartbeat will stop calling progress() after stopAfterMins.
 */
public class ProgressHeartbeat extends Thread {
  private static final int SLEEP_MINS = 1;

  private final Progressable progressable;
  private final int stopAfterMins;

  private boolean isCancelled;

  public ProgressHeartbeat(Progressable progressable, int stopAfterMins) {
    setDaemon(true);
    this.progressable = progressable;
    this.stopAfterMins = stopAfterMins;
    this.isCancelled = false;
  }

  public void startHeartbeat() {
    this.start();
  }

  public void stopHeartbeat() {
    isCancelled = true;
  }

  @Override
  public void run() {
    int minsRunning = 0;
    while (!isCancelled && minsRunning < stopAfterMins) {
      progressable.progress();
      try {
        Thread.sleep(1000L * 60L * SLEEP_MINS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      minsRunning += SLEEP_MINS;
    }
  }
}
