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

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.cassandra.streaming.ProgressInfo;
import org.apache.cassandra.streaming.SessionInfo;
import org.apache.cassandra.streaming.StreamEvent;
import org.apache.cassandra.streaming.StreamEventHandler;
import org.apache.cassandra.streaming.StreamState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.util.Progressable;


/**
 * Return true when everything is at 100%
 */
public class ProgressIndicator implements StreamEventHandler {

  private static final Logger LOG = LoggerFactory.getLogger(ProgressIndicator.class);

  private final Map<InetAddress, SessionInfo> sessionsByHost = new ConcurrentHashMap<>();
  private final Map<InetAddress, Set<ProgressInfo>> progressByHost = new ConcurrentHashMap<>();

  private long start;
  private long lastProgress;
  private long lastTime;
  private Progressable taskStatusreporter;

  public ProgressIndicator() {
    this(null);
  }

  public ProgressIndicator(Progressable reporter) {
    start = lastTime = System.nanoTime();
    taskStatusreporter = reporter;
  }

  public void onSuccess(StreamState finalState) {
  }

  public void onFailure(Throwable t) {
  }

  public void handleStreamEvent(StreamEvent event) {

    LOG.debug("Handling stream event");

    if (event.eventType == StreamEvent.Type.STREAM_PREPARED) {

      SessionInfo session = ((StreamEvent.SessionPreparedEvent) event).session;
      sessionsByHost.put(session.peer, session);
      LOG.info(String.format("Session to %s created", session.connecting.getHostAddress()));

    } else if (event.eventType == StreamEvent.Type.STREAM_COMPLETE ) {

      StreamEvent.SessionCompleteEvent completionEvent = ((StreamEvent.SessionCompleteEvent) event);
      if (completionEvent.success) {
        LOG.info(String.format("Stream to %s successful.", completionEvent.peer.getHostAddress()));
      } else {
        LOG.info(String.format("Stream to %s failed.", completionEvent.peer.getHostAddress()));
      }
    } else if (event.eventType == StreamEvent.Type.FILE_PROGRESS) {

      ProgressInfo progressInfo = ((StreamEvent.ProgressEvent) event).progress;

      // update progress
      Set<ProgressInfo> progresses = progressByHost.get(progressInfo.peer);
      if (progresses == null) {
        progresses = Sets.newSetFromMap(Maps.<ProgressInfo, Boolean>newConcurrentMap());
        progressByHost.put(progressInfo.peer, progresses);
      }
      if (progresses.contains(progressInfo)) {
        progresses.remove(progressInfo);
      }
      progresses.add(progressInfo);

      // craft status update string
      StringBuilder sb = new StringBuilder();
      sb.append("progress: ");

      long totalProgress = 0;
      long totalSize = 0;
      for (Map.Entry<InetAddress, Set<ProgressInfo>> entry : progressByHost.entrySet()) {
        SessionInfo session = sessionsByHost.get(entry.getKey());

        long size = session.getTotalSizeToSend();
        long current = 0;
        int completed = 0;
        for (ProgressInfo progress : entry.getValue()) {
          if (progress.currentBytes == progress.totalBytes) {
            completed++;
          }
          current += progress.currentBytes;
        }
        totalProgress += current;
        totalSize += size;
        sb.append("[").append(entry.getKey());
        sb.append(" ").append(completed).append("/").append(session.getTotalFilesToSend());
        sb.append(" (").append(size == 0 ? 100L : current * 100L / size).append("%)] ");
      }
      long time = System.nanoTime();
      long deltaTime = TimeUnit.NANOSECONDS.toMillis(time - lastTime);
      lastTime = time;
      long deltaProgress = totalProgress - lastProgress;
      lastProgress = totalProgress;

      sb.append("[total: ").append(totalSize == 0 ? 100L : totalProgress * 100L / totalSize).append("% - ");
      sb.append(mbPerSec(deltaProgress, deltaTime)).append("MB/s");
      sb.append(" (avg: ").append(mbPerSec(totalProgress, TimeUnit.NANOSECONDS.toMillis(time - start))).append("MB/s)]");

      LOG.info(sb.toString());
      if (taskStatusreporter != null)
        taskStatusreporter.progress();
    }
  }

  private int mbPerSec(long bytes, long timeInMs) {
    double bytesPerMs = ((double) bytes) / timeInMs;
    return (int) ((bytesPerMs * 1000) / (1024 * 2024));
  }
}
