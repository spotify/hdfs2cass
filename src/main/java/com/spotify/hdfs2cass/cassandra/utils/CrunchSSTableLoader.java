package com.spotify.hdfs2cass.cassandra.utils;

import org.apache.cassandra.io.sstable.SSTableLoader;
import org.apache.cassandra.streaming.StreamEvent;
import org.apache.cassandra.utils.OutputHandler;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

public class CrunchSSTableLoader extends SSTableLoader {

  private static final Logger LOG = LoggerFactory.getLogger(CrunchSSTableLoader.class);

  private final TaskAttemptContext context;

  public enum Counters {
    STREAM_PROGRESS_EVENT_CNT,
    STREAM_COMPLETED,
    STREAM_FAILED
  }

  public CrunchSSTableLoader(TaskAttemptContext context, File directory, Client client,
      OutputHandler outputHandler) {
    super(directory, client, outputHandler);
    this.context = context;
    LOG.debug("CrunchSSTableLoader instantiated");
  }

  @Override
  public void handleStreamEvent(StreamEvent event) {
    LOG.debug("Handling stream event " + event.toString());
    super.handleStreamEvent(event);

    context.progress();

//    if (event.eventType == StreamEvent.Type.FILE_PROGRESS) {
//      context.getCounter(Counters.STREAM_PROGRESS_EVENT_CNT).increment(1);
//    }

    if (event.eventType == StreamEvent.Type.STREAM_COMPLETE) {
      StreamEvent.SessionCompleteEvent e = (StreamEvent.SessionCompleteEvent) event;
      if (e.success) {
        context.getCounter(Counters.STREAM_COMPLETED).increment(1);
      } else {
        context.getCounter(Counters.STREAM_FAILED).increment(1);
      }
    }
  }

}
