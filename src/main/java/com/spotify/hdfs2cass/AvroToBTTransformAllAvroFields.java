package com.spotify.hdfs2cass;


import com.spotify.hdfs2cass.cassandra.utils.CassandraRecordUtils;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;

import java.nio.ByteBuffer;

public class AvroToBTTransformAllAvroFields implements AvroToBTTransformer {
    public Put transform(Configuration conf, GenericRecord avroRow) {
        String rowIdField = conf.get("hdfs2bt.transform.row_id_field");
        ByteBuffer rowId = CassandraRecordUtils.toByteBuffer(avroRow.get(rowIdField));
        Put put = new Put(rowId);
        for(Schema.Field f : avroRow.getSchema().getFields()) {
            String name = f.name();
            if(!name.equals(rowIdField)) {
                String[] columnName = name.split("\\:", 1);
                byte[] columnFamily = columnName[0].getBytes();
                byte[] columnQualifier = columnName[1].getBytes();
                byte[] value = CassandraRecordUtils.toByteBuffer(avroRow.get(name)).array();
                put.addColumn(columnFamily, columnQualifier, value);
            }
        }
        return put;
    }
}
