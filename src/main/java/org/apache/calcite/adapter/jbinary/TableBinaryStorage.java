package org.apache.calcite.adapter.jbinary;

import com.google.gson.Gson;
import net.jpountz.lz4.LZ4Decompressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;
import net.jpountz.lz4.LZ4SafeDecompressor;

import java.io.*;
import java.nio.ByteBuffer;
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
    MappedByteBuffer _inputMappedBuffer = null;
    ByteBuffer _inputBuffer = null;
    FileChannel _inputChannel = null;
    private FileOutputStream _fos = null;
    public long fileReadedSize;
    public long readedSize;
    private long _channelSize;
    private long METAINT = Integer.MAX_VALUE;

    private boolean _useCompression;
    private TableRow _tmpRow;

    private LZ4Factory lz4factory;

    private final static int COMPRESSED_BUF_LEN = 16*1024*1024;
    private final static int DECOMPRESSED_BUF_LEN = COMPRESSED_BUF_LEN * 4;

    LZ4SafeDecompressor decompressor;
    public static long _tmpDesTime = 0;
    public TableBinaryStorage(String table, String db, String mode, boolean useCompression) throws IOException {
        _table = table;
        _db    = db;
        _useCompression = useCompression;
        fileReadedSize = 0;
        readedSize = 0;
        _inputBuffer = ByteBuffer.allocate(0);

        if(Objects.equals(mode, "rw") || Objects.equals(mode, "r")) {
            _mode = mode;
        }
        else {
            _mode = "rw";
        }
        var path = Path.of(db, table + "_meta.json");

        lz4factory = LZ4Factory.fastestInstance();
        decompressor = lz4factory.safeDecompressor();

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
            _inputMappedBuffer = _inputChannel.map(FileChannel.MapMode.READ_ONLY, fileReadedSize, CheckBufferSize());
            _inputBuffer = _useCompression ? DecompressChunk() : _inputMappedBuffer;
            _tmpRow = new TableRow(_meta.Columns, _meta.Types   );

        }
    }

    private ByteBuffer DecompressChunk() throws IOException {
        if (_inputMappedBuffer.remaining() < COMPRESSED_BUF_LEN &&
                fileReadedSize + _inputMappedBuffer.remaining() < _channelSize){
            _inputMappedBuffer = _inputChannel.map(FileChannel.MapMode.READ_ONLY, fileReadedSize, CheckBufferSize());
        }

        int frameLen = _inputMappedBuffer.getInt();
        byte[] compressedChunk = new byte[frameLen];
        _inputMappedBuffer.get(compressedChunk);
        fileReadedSize += frameLen + 4;
        byte[] decompressed = new byte[DECOMPRESSED_BUF_LEN];

        int bufRemaining = _inputBuffer.limit() - (int)readedSize;
        _inputBuffer.position((int)readedSize);
        _inputBuffer.get(decompressed,0, bufRemaining);
        int decompressedLen = decompressor.decompress(compressedChunk, 0, frameLen, decompressed, bufRemaining);
        readedSize = 0; // сброс счетчика чтения
        return ByteBuffer.wrap(decompressed, 0, decompressedLen + bufRemaining);
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
        if((_channelSize != fileReadedSize) && (TableRow._size == -1)) {
            _inputBuffer =  _useCompression ? DecompressChunk() : _inputChannel.map(FileChannel.MapMode.READ_ONLY, fileReadedSize, CheckBufferSize());
            row = TableRow.Deserialize(_inputBuffer, _meta.Types);
        }
        _tmpDesTime += System.currentTimeMillis() - start;
        // System.out.println("AllSize - readed = remaining " + _channelSize + "-" + readedSize + "=" + (_channelSize-readedSize) + "(" + TableRow._size + ")");

        if (!_useCompression)
            fileReadedSize += TableRow._size;
        readedSize += TableRow._size;
        if(_inputBuffer.remaining() > 0) {
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
            if(_channelSize - fileReadedSize < METAINT) {
                return _channelSize - fileReadedSize;
            }
            else return METAINT;
        }
    }
}
