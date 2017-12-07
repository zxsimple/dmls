package com.zxsimple.dmls.util.hbase;

/**
 * Created by zxsimple on 2016/8/5.
 */
public class Table{
    private String namespace;
    private String name;

    public String getNamespace() {
        return namespace;
    }

    public void setNamespace(String namespace) {
        this.namespace = namespace;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}
