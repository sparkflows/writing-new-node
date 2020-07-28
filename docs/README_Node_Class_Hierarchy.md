## Class Hierarchy of Nodes

Below is the Class Hierarchy of the Nodes. Only a few Nodes are captured below to give an overall idea.

- Node : Base class for all nodes
  - NodeDataset : Base class for all Dataset nodes
    - NodeDatasetUnstructured
      - NodeDatasetPDF
    - NodeDatasetStructured
    - NodeDatasetCSV
    - NodeDatasetAvro
  - NodeETL : Base class for ETL nodes
    - NodeRowFilter
    - NodeColumnFilter
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
    
    
    
The Class Hierarchy of the Apache Spark Machine Learning Nodes resemble close to the corresponding Apache Spark ML Classes.
  
https://spark.apache.org/docs/2.4.0/api/java/org/apache/spark/ml/package-tree.html  
  
