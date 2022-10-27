package org.apache.calcite.adapter.jbinary;

import org.apache.calcite.avatica.util.DateTimeUtils;
import org.apache.commons.lang3.ArrayUtils;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.MappedByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.util.Date;

public class TableRow {

    private Object[] _columns;
    private ColumnType[] _columnTypes;

    private int _typesLenght;
    public static long _size = 0;
    private static long _rowSize = 0;

//    public static long _tmpFor = 0;
//    public static long _tmpIf = 0;
//
//    public static long _tmpRead = 0;


    public TableRow(Object[] columns, ColumnType[] types) {
        //_columns     = new Object[types.length];
        _columns = columns;
        _columnTypes = types;
        _typesLenght = types.length;
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
                    bytes = BytesConverter.LongToByteArray(((Date) data).getTime());

            default -> {
            }
        }

        stream.write(bytes);
    }

    public static TableRow Deserialize(MappedByteBuffer inputBuffer, ColumnType[] types) throws IOException, ClassNotFoundException {
        _rowSize = 0;
        int typesLength = types.length;
        Object[] columns = new Object[ typesLength ];
//        long _tmpTime = System.currentTimeMillis();
        for ( int i = 0; i < typesLength; i++ ) {
            columns[ i ] = ReadData(types[ i ], inputBuffer);
        }
//        _tmpFor += System.currentTimeMillis() - _tmpTime;
//        _tmpTime = System.currentTimeMillis();
        if(!ArrayUtils.contains(columns, null)) {
            _size = _rowSize;
        }
        else {
            _size = -1;
        }
//        _tmpIf += System.currentTimeMillis() - _tmpTime;
        if(ArrayUtils.isEmpty(columns))
            return null;

        return new TableRow(columns, types);
    }

    private static Object ReadData(ColumnType type, MappedByteBuffer buffer) throws IOException, ClassNotFoundException, BufferUnderflowException, IndexOutOfBoundsException {
        long tmpTime = System.currentTimeMillis();
        byte[] buf = null;
        Object result = null;
        switch (type) {
            case LONG -> {
                //buf = new byte[ Long.BYTES ];
                if(buffer.remaining() < Long.BYTES) return null;
//                buffer.get(buf, 0, buf.length);
                result = buffer.getLong();
                //result = BytesConverter.ByteArrayToLong(buf);
                _rowSize += Long.BYTES ;

            }
            case INTEGER -> {
                //buf = new byte[ Integer.BYTES ];
                if(buffer.remaining() < Integer.BYTES) return null;
                //buffer.get(buf, 0, buf.length);
                result = buffer.getInt();
                _rowSize += Integer.BYTES;
            }
            case FLOAT -> {
                //buf = new byte[ Float.BYTES ];
                if(buffer.remaining() < Float.BYTES) return null;
                //buffer.get(buf, 0, buf.length);
                //result = BytesConverter.ByteArrayToFloat(buf);
                result = buffer.getFloat();
                _rowSize += Float.BYTES;
            }
            case DOUBLE -> {
                //buf = new byte[ Double.BYTES ];
                if(buffer.remaining() < Double.BYTES) return null;
                //buffer.get(buf, 0, buf.length);
                //result = BytesConverter.ByteArrayToDouble(buf);
                result = buffer.getDouble();
                _rowSize += Double.BYTES;
            }
            case BOOLEAN -> {
                buf = new byte[ 1 ];
                if(buffer.remaining() < buf.length) return null;
                buffer.get(buf, 0, buf.length);
                result = BytesConverter.ByteArrayToBoolean(buf[ 0 ]);
                _rowSize += buf.length;
            }
            case STRING -> {
                var  pair = StringColumn.readObject(buffer);

                buf = pair != null ? pair.left : null;
                if(buf != null)
                    result = new String(buf,0,pair.right,StandardCharsets.UTF_8  );
                    //result = new String(buf, StandardCharsets.UTF_8);
                _rowSize += 4 + (pair != null ? pair.right : 0);
            }

            case TIME -> {
                //buf = new byte[ Long.BYTES ];
                if(buffer.remaining() < Long.BYTES) return null;
                //buffer.get(buf, 0, buf.length);
                //var ticks = BytesConverter.ByteArrayToLong(buf);
                var ticks = buffer.getLong();
                Date tmpD = new Date(ticks);
                result = (int) tmpD.getTime() ;
                _rowSize += Long.BYTES;
            }

            case DATE, DATETIME -> {
                //buf = new byte[ Long.BYTES ];
                if(buffer.remaining() < Long.BYTES) return null;
                //buffer.get(buf, 0, buf.length);
                //var ticks = BytesConverter.ByteArrayToLong(buf);
                var ticks = buffer.getLong();                ;
                result = (int) (ticks / DateTimeUtils.MILLIS_PER_DAY);
                _rowSize += Long.BYTES;
            }
            default -> {
                System.err.println(type);
            }
        }
        if(buf != null) {
            //_rowSize += buf.length;
        }
//        _tmpRead += System.currentTimeMillis() - tmpTime;
        return result;
    }
}
