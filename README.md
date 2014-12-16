# hdfs2cass

hdfs2cass is a wrapper around BulkOutputFormat(s) of Apache Cassandra (C\*). It is written using Apache Crunch's API in attempt to make moving data from Hadoop's HDFS into C\* easy.

## Quickstart

Here's a quick walkthrough of what needs to be done to successfully run hdfs2cass.

### Set up a C\* cluster

To start with, let's assume we have a C\* cluster running somewhere and one host in that cluster having a hostname of:

    cassandra-host.example.net

In that cluster, we create the following schema:

    CREATE KEYSPACE example WITH replication = {
      'class': 'SimpleStrategy', 'replication_factor': '1'};
    CREATE TABLE example.songstreams (
      user_id text,
      timestamp bigint,
      song_id text,
      PRIMARY KEY (user_id));


### Get some Avro files
    
Next, we'll need some Avro files. Check out [this tutorial](http://avro.apache.org/docs/1.7.7/gettingstartedjava.html) to see how to get started with Avro. We will assume the Avro files have this schema:

    {"namespace": "example.hdfs2cass",
     "type": "record",
     "name": "SongStream",
     "fields": [
         {"name": "user_id", "type": "string"},
         {"name": "timestamp", "type": "int"},
         {"name": "song_id", "type": "int"}
     ]
    }

We will place files of this schema on our (imaginary) Hadoop file system (HDFS) to a location

    hdfs:///example/path/songstreams


### Run hdfs2cass

Things should™ work out of the box by doing:

    $ git clone this-repository && cd this-repository
    $ mvn package
    $ JAR=target/spotify-hdfs2cass-2.0-SNAPSHOT-jar-with-dependencies.jar
    $ CLASS=com.spotify.hdfs2cass.Hdfs2Cass
    $ INPUT=/example/path/songstreams
    $ OUTPUT=cql://cassandra-host.example.net/example/songstreams?reducers=5
    $ hadoop jar $JAR $CLASS --input $INPUT --output $OUTPUT

This should run a hdfs2cass export with 5 reducers. 

### Check data in C\*

If we're lucky, we should eventually see our data in C\*:

    $ cqlsh $(cassandra-host.example.net) -e "SELECT * from example.songstreams limit 1;"
    
      user_id |  timestamp |   song_id
    ----------+------------+----------
    rincewind |   12345678 | 43e0-e12s

## Additional Arguments

[hdfs2cass](src/main/java/com/spotify/hdfs2cass/Hdfs2Cass.java) supports additional arguments:
* `--rowkey` to determine which field from the input records to use as row key, defaults to the first field in the record
* `--timestamp` to specify the timestamp of values in C\*, defaults to now
* `--ttl` to specify the TTL of values in C\*, defaults to 0
* `--ignore` to omit fields from source records, can be repeated to specify multiple fields

## Output URI Format

The format of the output URI is:

    (cql|thrift)://cassandra-host[:port]/keyspace/table?args...

The protocols in the output URI can be either `cql` or `thrift`. They are used to determine what type of C\* column family the data is imported into. The `port` is the binary protocol port C\* listens to client connections on.

The `params...` are all optional. They can be:
   * `buffersize=N` - Size of temporary SSTables built before streaming. Example `buffersize=64` will cause SSTables of size 64 MB to be built.
   * `columnnames=N1,N2` - Relevant for CQL. Used to override inferred order of columns in the prepared insert statement. See [this](src/main/java/com/spotify/hdfs2cass/crunch/cql/CQLRecord.java) for more info.
   * `compressionclass=S` - What compression to use when building SSTables. Defaults to whichever the table was created with.
   * `copiers=N` - The default number of parallel transfers run by reduce during the copy (shuffle) phase. Defaults to 5.
   * `distributerandomly` - Used in the shuffle phase. By default, data is grouped on reducers by C\*'s partitioner. This option disables that.
   * `mappers=N` - How many mappers should the job run with. By default this number is determined by magic.
   * `reducers=N` - How many reducers should the job run with. Having too few reducers for a lot of data will cause the job to fail.
   * `streamthrottlembits=N` - Maximum throughput allowed when streaming the SSTables. Defaults to C\*'s default.
   * `rpcport=N` - Port used to stream the SSTables. Defaults to the port C\* uses for streaming internally.

## More info

For more examples and information, please go ahead and [check how hdfs2cass works](src/main/java/com/spotify/hdfs2cass). You'll find examples of Apache Crunch jobs that can
serve as a source of inspiration. 
