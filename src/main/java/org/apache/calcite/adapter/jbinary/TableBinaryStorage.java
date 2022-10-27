package org.apache.calcite.adapter.jbinary;

import com.google.gson.Gson;

import java.io.*;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;

public class TableBinaryStorage implements IDatabaseReader, AutoCloseable {

    private final String _table;
    private final String _db;
    private final String _mode;
    private final FileMeta _meta;
    //private final RandomAccessFile _file;
    private FileInputStream _fis = null;
    MappedByteBuffer _inputBuffer = null;
    FileChannel _inputChannel = null;
    private FileOutputStream _fos = null;
    public long readedSize;
    private long _channelSize;
    private long METAINT = Integer.MAX_VALUE;

    private TableRow _tmpRow;

    public static long _tmpDesTime = 0;
    public TableBinaryStorage(String table, String db, String mode) throws IOException {
        _table = table;
        _db    = db;
        if(Objects.equals(mode, "rw") || Objects.equals(mode, "r")) {
            _mode = mode;
        }
        else {
            _mode = "rw";
        }
        var path = Path.of(db, table + "_meta.json");

        BufferedReader br = new BufferedReader(new FileReader(String.valueOf(path)));
        Gson gson = new Gson();
        _meta = gson.fromJson(br, FileMeta.class);
        br.close();

//        _file = new RandomAccessFile(Path.of(db,table).toString(), mode);
        if(_mode.equals("rw")) {
            _fos = new FileOutputStream(Path.of(db, table).toString(), true);
        }
        else if(_mode.equals("r")) {
            _fis          = new FileInputStream(Path.of(db, table).toString());
            _inputChannel = _fis.getChannel();
            _channelSize  = _inputChannel.size();
//            System.err.println("GB-" + _channelSize/1024/1024/1024 + " MB-" +_channelSize/1024/1024 + " KB-" + _channelSize/1024);
            _inputBuffer = _inputChannel.map(FileChannel.MapMode.READ_ONLY, 0, CheckBufferSize());
            _tmpRow = new TableRow(_meta.Columns, _meta.Types   );

        }
        readedSize = 0;
    }

    @Override
    public FileMeta GetMeta() {
        return _meta;
    }

    public FileMeta Meta() {
        //Разницы с GetMeta нет, но пусть будет.
        return _meta;
    }

    public void AddData(Object[] cols) throws IOException {
        if(!Objects.equals(_mode, "r")) {
            TableRow row = new TableRow(cols, _meta.Types);
            row.Serialize(_fos);
        }
    }

    @Override
    public Object[] ReadNextRow() throws IOException, ClassNotFoundException {
        if(Objects.equals(_mode, "rw")) return new Object[ 0 ];
        long tmpSize = TableRow._size;
        long start = System.currentTimeMillis();
        var row = TableRow.Deserialize (_inputBuffer, _meta.Types);
        if((_channelSize != readedSize) && (TableRow._size == -1)) {
            _inputBuffer = _inputChannel.map(FileChannel.MapMode.READ_ONLY, readedSize, CheckBufferSize());
            row          = TableRow.Deserialize(_inputBuffer, _meta.Types);

        }
        _tmpDesTime += System.currentTimeMillis() - start;
        // System.out.println("AllSize - readed = remaining " + _channelSize + "-" + readedSize + "=" + (_channelSize-readedSize) + "(" + TableRow._size + ")");

        readedSize += TableRow._size;
        if(_channelSize - readedSize > 0) {
            assert row != null;
            return row.Columns();
        }
        else
            return null;
    }

    @Override
    public List<byte[]> ReadNextRowRaw() {
        return null;
    }

    @Override
    public void close() throws Exception {
        _inputChannel.close();
    }

    private long CheckBufferSize(){
        //System.out.println("AllSize - readed = remaining " + _channelSize + "-" + readedSize + "=" + (_channelSize-readedSize));
        if(_channelSize <= METAINT) {
            return _channelSize;
        }
        else {
            if(_channelSize - readedSize < METAINT) {
                return _channelSize - readedSize;
            }
            else return METAINT;
        }
    }
}
