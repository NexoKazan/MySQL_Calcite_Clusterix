package org.apache.calcite.adapter.jbinary;

public enum ColumnType
   {
       LONG (0),
       INTEGER (1),
       FLOAT (2),
       DOUBLE (3),
       BOOLEAN (4),
       STRING (5),
       DATE (6),
       TIME (7),
       DATETIME (8);

       private final int code;
       ColumnType(int code){
           this.code = code;
       }
       public int getCode(){ return code;}
   }
