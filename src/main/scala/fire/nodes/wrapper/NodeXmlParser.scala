package fire.nodes.wrapper

import com.fire.parse.XMLParse
import fire.context.JobContext

import scala.beans.BeanProperty
import fire.workflowengine.Node
import org.apache.spark.sql.SparkSession


class NodeXmlParserextends extends Node {

  @BeanProperty var pipelineName = "xml-parse"
  @BeanProperty var inputXmlLocation = "data/input"
  @BeanProperty var rowTag =  "TS_270"
  @BeanProperty var outputFormat = "parquet"
  @BeanProperty var outPartitionColumns = "version,year,month,day,hour"
  @BeanProperty var outputLocation = "data/output"

  def this (i: Int, nme: String) {
  this ()
  id = i
  name = nme
 }

  @Override
  override def execute (jobContext: JobContext): Unit = {

  val spark: SparkSession = jobContext.session()

    val xmlParser = new XMLParse(
      inputXMLLocation = inputXmlLocation,
      rowTag = rowTag,
      outputFormat = outputFormat,
      outPartitionColumns = outPartitionColumns,
      outputLocation = outputLocation
    )

    xmlParser.performParse(spark)

  passDataFrameToNextNodesAndExecute (jobContext, dataFrame)
 }
}
