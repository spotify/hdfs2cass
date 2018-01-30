package com.spotify.hdfs2cass.cassandra.cql;

import java.io.IOException;

import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.cassandra.hadoop.HadoopCompat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.*;

public abstract class AbstractBulkOutputFormat<K, V> extends OutputFormat<K, V>
    implements org.apache.hadoop.mapred.OutputFormat<K, V>
{
  @Override
  public void checkOutputSpecs(JobContext context)
  {
    checkOutputSpecs(HadoopCompat.getConfiguration(context));
  }

  private void checkOutputSpecs(Configuration conf)
  {
    if (ConfigHelper.getOutputKeyspace(conf) == null)
    {
      throw new UnsupportedOperationException("you must set the keyspace with setColumnFamily()");
    }
  }

  @Override
  public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException
  {
    return new NullOutputCommitter();
  }

  /** Fills the deprecated OutputFormat interface for streaming. */
  @Deprecated
  public void checkOutputSpecs(org.apache.hadoop.fs.FileSystem filesystem, org.apache.hadoop.mapred.JobConf job) throws IOException
  {
    checkOutputSpecs(job);
  }

  public static class NullOutputCommitter extends OutputCommitter
  {
    public void abortTask(TaskAttemptContext taskContext) { }

    public void cleanupJob(JobContext jobContext) { }

    public void commitTask(TaskAttemptContext taskContext) { }

    public boolean needsTaskCommit(TaskAttemptContext taskContext)
    {
      return false;
    }

    public void setupJob(JobContext jobContext) { }

    public void setupTask(TaskAttemptContext taskContext) { }
  }
}
