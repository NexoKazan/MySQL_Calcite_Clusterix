package org.apache.calcite.adapter.jbinary;

import java.io.IOException;
import java.util.List;

public interface IDatabaseReader {
    FileMeta GetMeta();
    Object[] ReadNextRow() throws IOException, ClassNotFoundException;
    List<byte[]> ReadNextRowRaw();
    void close() throws Exception;
}
