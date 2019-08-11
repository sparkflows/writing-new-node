package fire.nodes.examples

import fire.context.JobContext
import fire.workflowengine.{FireSchema, Node, Workflow}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{LongType, StructField, StructType}

import scala.beans.BeanProperty

class NodeTestZipWithIndex extends Node{

  @BeanProperty var indexColName = "rowid"

  def this(i: Int, nme: String){
    this()
    id = i
    name = nme
  }

  def this(i: Int, nme: String, indexColName: String) {
    this(i, nme)
    this.indexColName = indexColName
  }

    @Override
  override def execute(jobContext: JobContext): Unit = {

  val sqlContext = jobContext.sqlctx()

  val newSchema = new StructType(dataFrame.schema.fields ++ Array(StructField(indexColName, LongType, false)))
  // Zip on RDD level
  val rddWithId = dataFrame.rdd.zipWithIndex

  // Convert back to DataFrame
  val dfZippedWithId =  jobContext.sqlctx.createDataFrame(rddWithId.map{ case (row, index) => Row.fromSeq(row.toSeq ++ Array(index))}, newSchema)

  passDataFrameToNextNodesAndExecute(jobContext, dfZippedWithId)
  }

  override def getOutputSchema(workflow: Workflow, inputSchema: FireSchema): FireSchema = {

    inputSchema.addColumn(indexColName, FireSchema.Type.LONG ,FireSchema.MLType.NUMERIC)

    return inputSchema
  }
}
