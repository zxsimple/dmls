package com.zxsimple.dmls.common.metadata.model;

import javax.persistence.*;

/**
 * Created by zxsimple on 2016/11/2.
 */
@Entity
@Table(name = "model_process_predict")
public class ModelPrecossPredict {

    @Id
    @GeneratedValue
    private Long id;

    /*@OneToOne(fetch = FetchType.EAGER)
    @JoinColumn(name = "node_vertex_instance_id")
    private NodeVertex nodeVertex;

    @OneToOne(fetch = FetchType.EAGER)
    @JoinColumn(name = "model_id")
    private Model model;*/

    @Column(name="node_vertex_instance_id")
    private String nodeVertexInstanceId;

    @Column(name="model_id")
    private Long modelId;

    @Column(name="predict_result")
    private String predictResult;

    @Column(name="is_deleted")
    private int isDeleted;

    @Column(name="feature_name")
    private String featureName;

    @Column(name="feature_index")
    private String featureIndex;

    @Column(name="label_mapping")
    private String labelMapping;

    public String getLabelMapping() {
        return labelMapping;
    }

    public void setLabelMapping(String labelMapping) {
        this.labelMapping = labelMapping;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
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

    public String getFeatureName() {
        return featureName;
    }

    public void setFeatureName(String featureName) {
        this.featureName = featureName;
    }

    public String getFeatureIndex() {
        return featureIndex;
    }

    public void setFeatureIndex(String featureIndex) {
        this.featureIndex = featureIndex;
    }

    public String getNodeVertexInstanceId() {
        return nodeVertexInstanceId;
    }

    public void setNodeVertexInstanceId(String nodeVertexInstanceId) {
        this.nodeVertexInstanceId = nodeVertexInstanceId;
    }

    public Long getModelId() {
        return modelId;
    }

    public void setModelId(Long modelId) {
        this.modelId = modelId;
    }
}
