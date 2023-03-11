package org.apache.calcite.adapter.jbinary;

public class FileMeta {

    public String[] Columns;
    public ColumnType[] Types;

    public FileMeta()
    {
        Columns = new String[0];
        Types = new ColumnType[0];
    }

    public ColumnType[] getTypes() {
        return Types;
    }

    public void setTypes(ColumnType[] types) {
        Types = types;
    }

    public String[] getColumns() {
        return Columns;
    }

    public void setColumns(String[] columns) {
        Columns = columns;
    }
}
