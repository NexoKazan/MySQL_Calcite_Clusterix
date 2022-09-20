package org.apache.calcite.adapter.jbinary;

import java.io.IOException;
import java.io.Serializable;
import java.math.BigInteger;
import java.nio.MappedByteBuffer;

public class StringColumn implements Serializable {
   // возможно стоит переделать под RandomAccesFile

    private final int Length;
    public final byte[] StringData;

    public StringColumn(byte[] stringData) {
        Length = stringData.length;
        StringData = stringData;
    }
    public int GetLength()
    {
        return Integer.BYTES + StringData.length;
    }

    public final byte[] writeObject() throws IOException{
        //где пишем
        var bytes = new byte[GetLength()];
        byte[] intBytes = BytesConverter.IntToByteArray(Length);
        System.arraycopy(intBytes,0,bytes,0,Integer.BYTES);
        System.arraycopy(StringData,0,bytes, Integer.BYTES, Length);
        return bytes;
    }

    public static byte[] readObject(MappedByteBuffer buffer)throws IOException, ClassNotFoundException{
        var buf = new byte[Integer.BYTES];
        if ( buffer.remaining() < Integer.BYTES ) return null;
        buffer.get( buf, 0, Integer.BYTES );
        //if (fis.read(buf, 0,Integer.BYTES) != Integer.BYTES) return null;
        int len = (int) BytesConverter.ByteArrayToInt(buf);    //Возможно ошибка
        buf = new  byte[len];
        //if (fis.read(buf, 0,len) != len) return null;
        if ( buffer.remaining() < len) return null;
        buffer.get( buf, 0, len);
        return buf;
    }

    private byte[] bigIntToByteArray( final int i ) {
        BigInteger bigInt = BigInteger.valueOf(i);
        return bigInt.toByteArray();
    }
}
