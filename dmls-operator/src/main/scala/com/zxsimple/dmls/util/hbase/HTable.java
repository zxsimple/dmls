package com.zxsimple.dmls.util.hbase;

import java.util.HashMap;

/**
 * Created by zxsimple on 2016/8/5.
 */
public class HTable {
    private Table table;
    private String rowkey;
    private HashMap<String, Columns> columns;

    public String getRowkey() {
        return rowkey;
    }

    public void setRowkey(String rowkey) {
        this.rowkey = rowkey;
    }

    public Table getTable() {
        return table;
    }

    public void setTable(Table table) {
        this.table = table;
    }

    public HashMap<String, Columns> getColumns() {
        return columns;
    }

    public void setColumns(HashMap<String, Columns> columns) {
        this.columns = columns;
    }

    @Override
    public String toString() {
        return "HTable{" +
                "table=" + table +
                ", rowkey='" + rowkey + '\'' +
                '}';
    }
}

