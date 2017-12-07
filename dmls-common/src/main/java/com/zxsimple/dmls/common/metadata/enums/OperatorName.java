package com.zxsimple.dmls.common.metadata.enums;

/**
 * Created by zxsimple on 2016/10/25.
 */
public enum OperatorName {
    /**
     * ds
     */
    Load,
    Sink,

    /**
     * etl
     */
    DefaultValueFill,
    LabelSelected,
    RandomSampling,
    RepeatRemove,
    StratifiedSampling,
    Filter,
    ColMapping,
    /**
     * fe
     */
    FeatureExtract,
    FeatureNormalizer,
    FeatureStandard,
    FeatureUniformization,
    FeatureDiscretize,
    FeatureSmooth,
    MDLPDiscretizer,
    PCA,
    Product,
    SizeChange,
    SVD,

    /**
     * stat
     */
    Correlation,
    Covariance,
    Histogram,
    Impurity,
    Independent,
    KSTest,
    Match,
    PercentileBitmap,
    TableStatistics,
    /**
     * ml
     */
    CreateModel,
    LinearRegressionWithSGD,
    LogisticRegressionLBFGS,
    DecisionTree,
    NaiveBayes,
    RandomForestClassification,
    RandomForestRegression,
    Kmeans,
    ALS,
    GBTClassification,
    GBTRegression,
    IsotonicRegression,
    SVMWithSGD,
    GaussianMixture,
    LatentDirichletAllocation,
    PowerIterationClustering,
    TimeSeries,
    DecisionTreeClassification,
    DecisionTreeRegression,
    AssociationRules,
    FPGrowth,
    /**
     * 模型预测
     * mlpre
     */
    PredictModel
}
