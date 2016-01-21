package com.spotify.hdfs2cass;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;

public interface AvroToBTTransformer {
    BTRow transform(Configuration conf, GenericRecord avroRecord);
}
