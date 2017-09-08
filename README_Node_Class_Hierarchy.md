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
          - NodeALS
          - NodeLogisticRegression
        - NodeKmeans
    - NodeTransformer
       - NodeBucketizer
       - NodeHashingTF
  - NodeEvaluator
    - NodeRegressionEvaluator
    - NodeBinaryClassificationEvaluator
