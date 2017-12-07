package com.zxsimple.dmls.util.hbase;

/**
 * Created by zxsimple on 2016/8/5.
 */
public class Columns {
    private String cf;
    private String col;
    private String type;

    public Columns(String cf, String col, String type) {
        this.cf = cf;
        this.col = col;
        this.type = type;
    }

    public String getCf() {
        return cf;
    }

    public void setCf(String cf) {
        this.cf = cf;
    }

    public String getCol() {
        return col;
    }

    public void setCol(String col) {
        this.col = col;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }
}
