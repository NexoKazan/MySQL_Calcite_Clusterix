package org.apache.calcite.adapter.jbinary;

import org.apache.calcite.util.Pair;
import org.postgresql.core.Tuple;

import java.io.IOException;
import java.io.Serializable;
import java.math.BigInteger;
import java.nio.ByteBuffer;

public class StringColumn implements Serializable {
   // возможно стоит переделать под RandomAccesFile

    private final int Length;
    public final byte[] StringData;

    private final static byte[] _BUF = new byte[1024*1024];
    private final static byte[] _IntBuf = new byte[Integer.BYTES];
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

    public static Pair<byte[], Integer> readObject(ByteBuffer buffer)throws IOException, ClassNotFoundException{

        if ( buffer.remaining() < Integer.BYTES ) return null;
        buffer.get( _IntBuf, 0, Integer.BYTES );
        //if (fis.read(buf, 0,Integer.BYTES) != Integer.BYTES) return null;
        int len = (int) BytesConverter.ByteArrayToInt(_IntBuf);    //Возможно ошибка
        //buf = new  byte[len];
        //if (fis.read(buf, 0,len) != len) return null;
        if ( buffer.remaining() < len) return null;
        buffer.get( _BUF, 0, len);
        //_BUF[len] = 0;
        return new Pair<>(_BUF, len);
    }

    private byte[] bigIntToByteArray( final int i ) {
        BigInteger bigInt = BigInteger.valueOf(i);
        return bigInt.toByteArray();
    }
}
