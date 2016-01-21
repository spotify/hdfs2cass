package com.spotify.hdfs2cass;

import com.spotify.hdfs2cass.crunch.cql.CQLRecord;
import org.apache.avro.generic.GenericRecord;
import org.apache.crunch.MapFn;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;

import java.util.HashSet;
import java.util.List;

public class AvroToBT extends MapFn<GenericRecord, Put> {
    private final AvroToBTTransformer transformer;

    public AvroToBT(final AvroToBTTransformer transformer) {
        this.transformer = transformer;
    }

    @Override
    public Put map(GenericRecord record) {
        Configuration conf = getConfiguration();
        return transformer.transform(conf, record);
    }
}
