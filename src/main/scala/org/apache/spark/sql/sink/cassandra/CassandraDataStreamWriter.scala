package org.apache.spark.sql.sink.cassandra

import java.util.Locale

import com.datastax.spark.connector.ColumnSelector
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.writer.{RowWriterFactory, WriteConf}
import org.apache.spark.annotation.InterfaceStability
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.catalyst.streaming.InternalOutputModes
import org.apache.spark.sql.execution.command.DDLUtils
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery, Trigger}
import org.apache.spark.sql.{AnalysisException, Dataset, ForeachWriter}

import scala.collection.JavaConverters._

/**
  * Interface used to write a streaming `Dataset` to external storage systems (e.g. file systems,
  * key-value stores, etc). Use `Dataset.writeStream` to access this.
  *
  * @since 2.0.0
  */
@InterfaceStability.Evolving
class CassandraDataStreamWriter[T: RowWriterFactory](
    ds: Dataset[T],
    cassandraConnectorBroadcast: Broadcast[CassandraConnector],
    keyspaceName: String,
    tableName: String,
    columnNames: ColumnSelector,
    writeConf: WriteConf,
    checkPartitionKey: Boolean = false) {

  private val df = ds.toDF()

  /**
    * Specifies how data of a streaming DataFrame/Dataset is written to a streaming sink.
    *   - `OutputMode.Append()`: only the new rows in the streaming DataFrame/Dataset will be
    * written to the sink
    *   - `OutputMode.Complete()`: all the rows in the streaming DataFrame/Dataset will be written
    * to the sink every time these is some updates
    *   - `OutputMode.Update()`: only the rows that were updated in the streaming DataFrame/Dataset
    * will be written to the sink every time there are some updates. If
    * the query doesn't contain aggregations, it will be equivalent to
    * `OutputMode.Append()` mode.
    *
    * @since 2.0.0
    */
  def outputMode(outputMode: OutputMode): CassandraDataStreamWriter[T] = {
    this.outputMode = outputMode
    this
  }

  /**
    * Specifies how data of a streaming DataFrame/Dataset is written to a streaming sink.
    *   - `append`:   only the new rows in the streaming DataFrame/Dataset will be written to
    * the sink
    *   - `complete`: all the rows in the streaming DataFrame/Dataset will be written to the sink
    * every time these is some updates
    *   - `update`:   only the rows that were updated in the streaming DataFrame/Dataset will
    * be written to the sink every time there are some updates. If the query doesn't
    * contain aggregations, it will be equivalent to `append` mode.
    *
    * @since 2.0.0
    */
  def outputMode(outputMode: String): CassandraDataStreamWriter[T] = {
    this.outputMode = InternalOutputModes(outputMode)
    this
  }

  /**
    * Set the trigger for the stream query. The default value is `ProcessingTime(0)` and it will run
    * the query as fast as possible.
    *
    * Scala Example:
    * {{{
    *   df.writeStream.trigger(ProcessingTime("10 seconds"))
    *
    *   import scala.concurrent.duration._
    *   df.writeStream.trigger(ProcessingTime(10.seconds))
    * }}}
    *
    * Java Example:
    * {{{
    *   df.writeStream().trigger(ProcessingTime.create("10 seconds"))
    *
    *   import java.util.concurrent.TimeUnit
    *   df.writeStream().trigger(ProcessingTime.create(10, TimeUnit.SECONDS))
    * }}}
    *
    * @since 2.0.0
    */
  def trigger(trigger: Trigger): CassandraDataStreamWriter[T] = {
    this.trigger = trigger
    this
  }

  /**
    * Specifies the name of the [[org.apache.spark.sql.streaming.StreamingQuery]] that can be started with `start()`.
    * This name must be unique among all the currently active queries in the associated SQLContext.
    *
    * @since 2.0.0
    */
  def queryName(queryName: String): CassandraDataStreamWriter[T] = {
    this.extraOptions += ("queryName" -> queryName)
    this
  }

  /**
    * Partitions the output by the given columns on the file system. If specified, the output is
    * laid out on the file system similar to Hive's partitioning scheme. As an example, when we
    * partition a dataset by year and then month, the directory layout would look like:
    *
    *   - year=2016/month=01/
    *   - year=2016/month=02/
    *
    * Partitioning is one of the most widely used techniques to optimize physical data layout.
    * It provides a coarse-grained index for skipping unnecessary data reads when queries have
    * predicates on the partitioned columns. In order for partitioning to work well, the number
    * of distinct values in each column should typically be less than tens of thousands.
    *
    * @since 2.0.0
    */
  @scala.annotation.varargs
  def partitionBy(colNames: String*): CassandraDataStreamWriter[T] = {
    this.partitioningColumns = Option(colNames)
    this
  }

  /**
    * Adds an output option for the underlying data source.
    *
    * You can set the following option(s):
    * <ul>
    * <li>`timeZone` (default session local timezone): sets the string that indicates a timezone
    * to be used to format timestamps in the JSON/CSV datasources or partition values.</li>
    * </ul>
    *
    * @since 2.0.0
    */
  def option(key: String, value: String): CassandraDataStreamWriter[T] = {
    this.extraOptions += (key -> value)
    this
  }

  /**
    * Adds an output option for the underlying data source.
    *
    * @since 2.0.0
    */
  def option(key: String, value: Boolean): CassandraDataStreamWriter[T] =
    option(key, value.toString)

  /**
    * Adds an output option for the underlying data source.
    *
    * @since 2.0.0
    */
  def option(key: String, value: Long): CassandraDataStreamWriter[T] =
    option(key, value.toString)

  /**
    * Adds an output option for the underlying data source.
    *
    * @since 2.0.0
    */
  def option(key: String, value: Double): CassandraDataStreamWriter[T] =
    option(key, value.toString)

  /**
    * (Scala-specific) Adds output options for the underlying data source.
    *
    * You can set the following option(s):
    * <ul>
    * <li>`timeZone` (default session local timezone): sets the string that indicates a timezone
    * to be used to format timestamps in the JSON/CSV datasources or partition values.</li>
    * </ul>
    *
    * @since 2.0.0
    */
  def options(options: scala.collection.Map[String, String])
    : CassandraDataStreamWriter[T] = {
    this.extraOptions ++= options
    this
  }

  /**
    * Adds output options for the underlying data source.
    *
    * You can set the following option(s):
    * <ul>
    * <li>`timeZone` (default session local timezone): sets the string that indicates a timezone
    * to be used to format timestamps in the JSON/CSV datasources or partition values.</li>
    * </ul>
    *
    * @since 2.0.0
    */
  def options(
      options: java.util.Map[String, String]): CassandraDataStreamWriter[T] = {
    this.options(options.asScala)
    this
  }

  /**
    * Starts the execution of the streaming query, which will continually output results to the given
    * path as new data arrives. The returned [[StreamingQuery]] object can be used to interact with
    * the stream.
    *
    * @since 2.0.0
    */
  def start(path: String): StreamingQuery = {
    option("path", path).start()
  }

  /**
    * Starts the execution of the streaming query, which will continually output results to the given
    * path as new data arrives. The returned [[org.apache.spark.sql.streaming.StreamingQuery]] object can be used to interact with
    * the stream.
    *
    * @since 2.0.0
    */
  def start(): StreamingQuery = {
    val sink = new CassandraSink[T](cassandraConnectorBroadcast.value,
                                    keyspaceName,
                                    tableName,
                                    columnNames,
                                    writeConf,
                                    checkPartitionKey,
                                    ds.exprEnc)
    df.sparkSession.sessionState.streamingQueryManager.startQuery(
      extraOptions.get("queryName"),
      extraOptions.get("checkpointLocation"),
      df,
      extraOptions.toMap,
      sink,
      outputMode,
      false,
      recoverFromCheckpointLocation = true,
      trigger = trigger
    )
  }

  private def normalizedParCols: Option[Seq[String]] = partitioningColumns.map {
    cols =>
      cols.map(normalize(_, "Partition"))
  }

  /**
    * The given column name may not be equal to any of the existing column names if we were in
    * case-insensitive context. Normalize the given column name to the real one so that we don't
    * need to care about case sensitivity afterwards.
    */
  private def normalize(columnName: String, columnType: String): String = {
    val validColumnNames = df.logicalPlan.output.map(_.name)
    validColumnNames
      .find(df.sparkSession.sessionState.analyzer.resolver(_, columnName))
      .getOrElse(
        throw new AnalysisException(
          s"$columnType column $columnName not found in " +
            s"existing columns (${validColumnNames.mkString(", ")})"))
  }

  private def assertNotPartitioned(operation: String): Unit = {
    if (partitioningColumns.isDefined) {
      throw new AnalysisException(s"'$operation' does not support partitioning")
    }
  }

  ///////////////////////////////////////////////////////////////////////////////////////
  // Builder pattern config options
  ///////////////////////////////////////////////////////////////////////////////////////

  private var source: String =
    df.sparkSession.sessionState.conf.defaultDataSourceName

  private var outputMode: OutputMode = OutputMode.Append

  private var trigger: Trigger = Trigger.ProcessingTime(0L)

  private var extraOptions =
    new scala.collection.mutable.HashMap[String, String]

  private var partitioningColumns: Option[Seq[String]] = None
}
