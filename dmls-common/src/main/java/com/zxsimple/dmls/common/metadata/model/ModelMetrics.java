package com.zxsimple.dmls.common.metadata.model;

import com.alibaba.fastjson.JSONObject;

import javax.persistence.*;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by zxsimple on 2016/4/19.
 */
@Entity
@Table(name = "model_metrics")
public class ModelMetrics implements Serializable {
    @Id
    @GeneratedValue
    private Long id;

    @Column(name="train_app_id")
    private Long trainAppId;

    @Column(name="model_evaluation")
    //@JsonIgnore
    private String modelEvaluation;

    @Column(name = "is_deleted")
    private int isDeleted;

    @Transient
    private HashMap<String,String> evaluationResult = new HashMap<String,String>();

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Long getTrainAppId() {
        return trainAppId;
    }

    public void setTrainAppId(Long trainAppId) {
        this.trainAppId = trainAppId;
    }

    public String getModelEvaluation() {
        return modelEvaluation;
    }

    public void setModelEvaluation(String modelEvaluation) {
        this.modelEvaluation = modelEvaluation;
    }

    public int getIsDeleted() {
        return isDeleted;
    }

    public void setIsDeleted(int isDeleted) {
        this.isDeleted = isDeleted;
    }

    public HashMap<String, String> getEvaluationResult() {
        if(modelEvaluation!=null && !modelEvaluation.trim().equals("")){
            JSONObject jsonObject = JSONObject.parseObject(modelEvaluation);
            for (Map.Entry<String, Object> entry : jsonObject.entrySet()) {
                evaluationResult.put(entry.getKey(), String.valueOf(entry.getValue()));
            }
        }
        return evaluationResult;
    }

    public void setEvaluationResult(HashMap<String, String> evaluationResult) {
        this.evaluationResult = evaluationResult;
    }
}
