package com.spotify.hdfs2cass;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;

public interface AvroToBTTransformer {
    Put transform(Configuration conf, GenericRecord avroRecord);
}
