package org.apache.calcite.adapter.jbinary;

import java.nio.ByteBuffer;

public class BytesConverter {

    public static final byte[] IntToByteArray(int value) {
        return new byte[] {
                (byte)(value >>> 24),
                (byte)(value >>> 16),
                (byte)(value >>> 8),
                (byte)value};
    }

    public static long ByteArrayToInt(byte[] bytes) {
        return ((bytes[0] & 0xFF) << 24) |
                ((bytes[1] & 0xFF) << 16) |
                ((bytes[2] & 0xFF) << 8) |
                ((bytes[3] & 0xFF) << 0);
    }

    public static byte[] LongToByteArray(long value) {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.putLong(value);
        return buffer.array();
    }

    public static long ByteArrayToLong(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.put(bytes);
        buffer.flip();//need flip
        return buffer.getLong();
    }

    public static byte [] FloatToByteArray (float value)
    {
        //Обсудить порядко
        return ByteBuffer.allocate(Float.BYTES).putFloat(value).array();
    }

    public static float ByteArrayToFloat (byte[] value)
    {
        return ByteBuffer.wrap(value).getFloat();
    }

    public static byte [] DoubleToByteArray (Double value)
    {
        return ByteBuffer.allocate(Double.BYTES).putDouble(value).array();
    }

    public static double ByteArrayToDouble (byte[] value)
    {
        return ByteBuffer.wrap(value).getDouble();
    }

    public static byte[]  BooleanToByteArray (Boolean value)
    {
        byte[] ret = new byte[1];
        ret[0] = IntToByteArray (value ? 1:0)[3];
        return ret;
    }

    public static Boolean ByteArrayToBoolean(byte value)
    {
        return value!=0;
    }
}
