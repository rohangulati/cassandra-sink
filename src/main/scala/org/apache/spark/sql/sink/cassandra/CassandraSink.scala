package org.apache.spark.sql.sink.cassandra

import com.datastax.spark.connector.ColumnSelector
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.writer.{
  CassandraTableWriter,
  RowWriterFactory,
  WriteConf
}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.encoders.encoderFor
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.{DataFrame, Encoder}

private class CassandraSink[T: RowWriterFactory](connector: CassandraConnector,
                                                 keyspaceName: String,
                                                 tableName: String,
                                                 columnNames: ColumnSelector,
                                                 writeConf: WriteConf,
                                                 checkPartitionKey: Boolean =
                                                   false,
                                                 e: Encoder[T])
    extends Sink
    with Logging
    with Serializable {

  @volatile private var latestBatchId = -1L

  override def addBatch(batchId: Long, data: DataFrame): Unit = {
    if (batchId <= latestBatchId) {
      logInfo(s"Skipping already committed batch $batchId")
    } else {
      val encoder = encoderFor[T](e).resolveAndBind(
        data.logicalPlan.output,
        data.sparkSession.sessionState.analyzer)
      val writer = CassandraTableWriter[T](connector,
                                           keyspaceName,
                                           tableName,
                                           columnNames,
                                           writeConf,
                                           checkPartitionKey)
      data.queryExecution.toRdd.foreachPartition { iter =>
        writer.write(iter.map(encoder.fromRow))
      }
      latestBatchId = batchId
    }
  }

  override def toString: String = "cassandra"
}
