## hdfs2cass

*hdfs2cass* is a simple example of a Hadoop job that runs a mapreduce job to move data from HDFS into a Cassandra cluster

We use it for many terabytes a day, across datacenters, firewalls, and oceans.

## Features

* Builds SSTables and uploads them
* Uses a custom partitioner to mirror the topology in the reduce step

## Usage

```
hadoop jar spotify-hdfs2cass-1.0.jar com.spotify.hdfs2cass.BulkLoader -i <hdfs/input/path> -h <cassandra-host.site.domain> -k <keyspace> -c <column family>
```

The format of the input file is an artefact of how other things look at Spotify. It's a raw avro text input file containing tab-separated line of records

1. The string "HdfsToCassandra"
2. A version number, 1, 2 or 3
3. Row key
4. Column name
5. (optional) Timestamp (only if the version is 3, otherwise default is migration time)
6. (optional) TTL in microseconds (only if the version is 2 or 3, otherwise the default is 0 = never)
7. Column value

The format is probably not exactly what you want, but we are planning to add support for more generic formats in the futures.

You can also encode the last item in base64 if you provide the -b flag

## Problems

Because of [a bug](http://www.mail-archive.com/commits@cassandra.apache.org/msg50170.html) streaming several SSTables in parallel from a single machine is not possible until Cassandra 1.2. If you are running < 1.2, you can't use the SSTable uploading and it will not be as fast. You activate the SStable uploading by using the -s flag

Also since the SSTables are flushed at the end of each reduce task, you can't send too much data to any single reducer. A workaround is to use lots of reducers. If you start running into problems with TCP connections, you can add a pool in Hadoop so that not too many reducers run simultaneously

## Upcoming fixes

* Flush the SSTables not at the end of the reduce task, but continuously
* Support other input formats

We would love it if you send us a patch for the things above, or anything else.