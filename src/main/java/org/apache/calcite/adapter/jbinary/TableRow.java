package org.apache.calcite.adapter.jbinary;

import org.apache.commons.lang3.ArrayUtils;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.MappedByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Date;

public class TableRow {

    private Object[] _columns;
    private ColumnType[] _columnTypes;
    public static long _size = 0;
    private static long _rowSize = 0;

    public TableRow(Object[] columns, ColumnType[] types) {
        _columns     = columns;
        _columnTypes = types;
    }

    public Object[] Columns() {
        return _columns;
    }

    public ColumnType[] ColumnTypes() {
        return _columnTypes;
    }

    public void Serialize(FileOutputStream stream) throws IOException {
        for ( var i = 0; i < Columns().length; i++ ) {
            WriteData(_columns[ i ], _columnTypes[ i ], stream);
        }
    }

    private void WriteData(Object data, ColumnType type, FileOutputStream stream) throws IOException {
        byte[] bytes = new byte[]{};
        switch (type) {
            case LONG -> bytes = BytesConverter.LongToByteArray((long) data);
            case INTEGER -> bytes = BytesConverter.IntToByteArray((int) data);
            case FLOAT -> bytes = BytesConverter.FloatToByteArray((float) data);
            case DOUBLE -> bytes = BytesConverter.DoubleToByteArray((double) data);
            case BOOLEAN -> bytes = BytesConverter.BooleanToByteArray((boolean) data);
            case STRING -> {
                var str = (String) data;
                var strBytes = str.getBytes(StandardCharsets.UTF_8);
                var strStruct = new StringColumn(strBytes);
                bytes = strStruct.writeObject();
            }
            case DATE, TIME, DATETIME ->
                    bytes = BytesConverter.LongToByteArray((long) data);

            default -> {
            }
        }

        stream.write(bytes);
    }

    public static TableRow Deserialize(MappedByteBuffer inputBuffer, ColumnType[] types) throws IOException, ClassNotFoundException {
        _rowSize = 0;
        Object[] columns = new Object[ types.length ];
        for ( var i = 0; i < types.length; i++ ) {
            columns[ i ] = ReadData(types[ i ], inputBuffer);
        }
        if(!ArrayUtils.contains(columns, null)) {
            _size = _rowSize;
        }
        else {
            _size = -1;
        }
        if(ArrayUtils.isEmpty(columns)) return null;

        return new TableRow(columns, types);
    }

    private static Object ReadData(ColumnType type, MappedByteBuffer buffer) throws IOException, ClassNotFoundException, BufferUnderflowException, IndexOutOfBoundsException {
        byte[] buf = null;
        Object result = null;
        switch (type) {
            case LONG -> {
                buf = new byte[ Long.BYTES ];
                if(buffer.remaining() < buf.length) return null;
//                buffer.get(buf, 0, buf.length);
                result = buffer.getLong();
                //result = BytesConverter.ByteArrayToLong(buf);

            }
            case INTEGER -> {
                buf = new byte[ Integer.BYTES ];
                if(buffer.remaining() < buf.length) return null;
                buffer.get(buf, 0, buf.length);
                result = BytesConverter.ByteArrayToInt(buf);
            }
            case FLOAT -> {
                buf = new byte[ Float.BYTES ];
                if(buffer.remaining() < buf.length) return null;
                buffer.get(buf, 0, buf.length);
                result = BytesConverter.ByteArrayToFloat(buf);
            }
            case DOUBLE -> {
                buf = new byte[ Double.BYTES ];
                if(buffer.remaining() < buf.length) return null;
                buffer.get(buf, 0, buf.length);
                result = BytesConverter.ByteArrayToDouble(buf);
            }
            case BOOLEAN -> {
                buf = new byte[ 1 ];
                if(buffer.remaining() < buf.length) return null;
                buffer.get(buf, 0, buf.length);
                result = BytesConverter.ByteArrayToBoolean(buf[ 0 ]);
            }
            case STRING -> {
                buf = StringColumn.readObject(buffer);
                if(buf != null)
                    result = new String(buf, StandardCharsets.UTF_8);
                _rowSize += 4;
            }

            case DATE, TIME, DATETIME -> {
                buf = new byte[ Long.BYTES ];
                if(buffer.remaining() < buf.length) return null;
                //buffer.get(buf, 0, buf.length);
                //var ticks = BytesConverter.ByteArrayToLong(buf);
                var ticks = buffer.getLong();
                result = new Date(ticks);
            }
            default -> {
            }
        }
        if(buf != null) {
            _rowSize += buf.length;
        }
        return result;
    }
}
