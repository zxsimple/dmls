package com.zxsimple.dmls.model;


/**
 * Created by zxsimple on 5-10.
 */
public class DefaultValueFillBean {
    private String colName;
    private OriginalType originalType;
    private String originalCustomValue;
    private ReplacedType replacedType;
    private String replacedCustomValue;

    public String getColName() {
        return colName;
    }

    public void setColName(String colName) {
        this.colName = colName;
    }

    public OriginalType getOriginalType() {
        return originalType;
    }

    public void setOriginalType(OriginalType originalType) {
        this.originalType = originalType;
    }

    public String getOriginalCustomValue() {
        return originalCustomValue;
    }

    public void setOriginalCustomValue(String originalCustomValue) {
        this.originalCustomValue = originalCustomValue;
    }

    public ReplacedType getReplacedType() {
        return replacedType;
    }

    public void setReplacedType(ReplacedType replacedType) {
        this.replacedType = replacedType;
    }

    public String getReplacedCustomValue() {
        return replacedCustomValue;
    }

    public void setReplacedCustomValue(String replacedCustomValue) {
        this.replacedCustomValue = replacedCustomValue;

    }


}
