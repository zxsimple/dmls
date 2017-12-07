package com.zxsimple.dmls.common.metadata.model;

import com.alibaba.fastjson.JSONObject;

import javax.persistence.*;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by zxsimple on 2016/5/12.
 */
@Entity
@Table(name="model_predict")
public class ModelPredict implements Serializable {

    @Id
    @GeneratedValue
    private Long id;

    @Column(name="predict_app_id")
    private Long predictAppId;

    @Column(name="value_mapping")
    private String valueMapping;

    @Column(name="predict_result")
    private String predictResult;

    @Column(name="is_deleted")
    private int isDeleted;

    @Column(name="feature_name")
    private String featureName;

    @Column(name="feature_index")
    private int featureIndex;

    @Column(name="label_mapping")
    private String labelMapping;

    @Transient
    private HashMap<String,String> predictResultMap = new HashMap<String,String>();

    @Transient
    private HashMap<String,String> valueMappingMap = new HashMap<String,String>();

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Long getPredictAppId() {
        return predictAppId;
    }

    public void setPredictAppId(Long predictAppId) {
        this.predictAppId = predictAppId;
    }

    public String getValueMapping() {
        return valueMapping;
    }

    public void setValueMapping(String valueMapping) {
        this.valueMapping = valueMapping;
    }

    public String getPredictResult() {
        return predictResult;
    }

    public void setPredictResult(String predictResult) {
        this.predictResult = predictResult;
    }

    public int getIsDeleted() {
        return isDeleted;
    }

    public void setIsDeleted(int isDeleted) {
        this.isDeleted = isDeleted;
    }

    public HashMap<String, String> getPredictResultMap() {
        if(predictResult!=null && !predictResult.trim().equals("")){
            JSONObject jsonObject = JSONObject.parseObject(predictResult);
            for (Map.Entry<String, Object> entry : jsonObject.entrySet()) {
                predictResultMap.put(entry.getKey(), String.valueOf(entry.getValue()));
            }
        }
        return predictResultMap;
    }

    public void setPredictResultMap(HashMap<String, String> predictResultMap) {
        this.predictResultMap = predictResultMap;
    }

    public HashMap<String, String> getValueMappingMap() {
        if(predictResult!=null && !valueMapping.trim().equals("")){
            JSONObject jsonObject = JSONObject.parseObject(valueMapping);
            for (Map.Entry<String, Object> entry : jsonObject.entrySet()) {
                valueMappingMap.put(entry.getKey(), String.valueOf(entry.getValue()));
            }
        }
        return valueMappingMap;
    }

    public void setValueMappingMap(HashMap<String, String> valueMappingMap) {
        this.valueMappingMap = valueMappingMap;
    }

    public String getFeatureName() {
        return featureName;
    }

    public void setFeatureName(String featureName) {
        this.featureName = featureName;
    }

    public int getFeatureIndex() {
        return featureIndex;
    }

    public void setFeatureIndex(int featureIndex) {
        this.featureIndex = featureIndex;
    }

    public String getLabelMapping() {
        return labelMapping;
    }

    public void setLabelMapping(String labelMapping) {
        this.labelMapping = labelMapping;
    }
}
