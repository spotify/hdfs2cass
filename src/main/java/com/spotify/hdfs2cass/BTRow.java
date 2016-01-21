package com.spotify.hdfs2cass;

import com.google.common.collect.ImmutableMap;

import java.nio.ByteBuffer;
import java.util.Map;

public class BTRow {
    final private ByteBuffer rowId;
    final private ImmutableMap<ByteBuffer, ByteBuffer> columns;

    public BTRow(final ByteBuffer rowId, final ImmutableMap<ByteBuffer, ByteBuffer> columns) {
        this.rowId = rowId;
        this.columns = columns;
    }

    public ByteBuffer rowId() {
        return rowId;
    }

    public ImmutableMap<ByteBuffer, ByteBuffer> columns() {
        return columns;
    }
}
