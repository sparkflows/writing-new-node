## Class Hierarchy of Nodes

Below is the Class Hierarchy of the Nodes

- Node : Base class for all nodes
  - NodeETL : Base class for ETL nodes
    - NodeRowFilter
    - NodeColumnFilter
  - NodeDataset : Base class for all Dataset nodes
    - NodeDatasetUnstructured
      - NodeDatasetPDF
    - NodeDatasetStructured
    - NodeDatasetCSV
    - NodeDatasetAvro
  - NodePipelineStage
    - NodeModeling
      - NodeEstimator
        - NodePredictor
          - NodeRandomForestClassifier
          - NodeLogisticRegression
        - NodeKMeans
    - NodeTransformer
       - NodeBucketizer
       - NodeHashingTF
  - NodeEvaluator
    - NodeRegressionEvaluator
    - NodeBinaryClassificationEvaluator
    
    
    
  The Class Hierarchy of the Spark Machine Learning Nodes resemble close to the corresponding Spark ML Classes.
  
  https://spark.apache.org/docs/2.1.0/api/java/org/apache/spark/ml/package-tree.html
  
  
