package com.spotify.hdfs2cass;


import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;

import java.nio.ByteBuffer;
import java.util.Map;

public class AvroToBTTransformAllAvroFields implements AvroToBTTransformer {
    public BTRow transform(Configuration conf, GenericRecord avroRow) {
        String rowIdField = conf.get("hdfs2bt.transform.row_id_field");
        Map<ByteBuffer, ByteBuffer> columns = Maps.newHashMap();
        ByteBuffer rowId = (ByteBuffer) avroRow.get(rowIdField);
        return new BTRow(rowId, ImmutableMap.copyOf(columns));
    }
}
