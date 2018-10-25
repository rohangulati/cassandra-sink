package org.apache.spark.sql.sink.cassandra

import com.datastax.spark.connector.ColumnSelector
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.writer.{WriteConf, _}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.catalyst.analysis._

class CassandraDataset[T: RowWriterFactory](dataset: Dataset[T]) {

  def cassandraTable(
      cassandraConnectorBroadcast: Broadcast[CassandraConnector],
      keyspaceName: String,
      tableName: String,
      columnNames: ColumnSelector,
      writeConf: WriteConf,
      checkPartitionKey: Boolean = false): CassandraDataStreamWriter[T] = {
    if (!dataset.isStreaming) {
      dataset.logicalPlan.failAnalysis(
        "'writeStream' can be called only on streaming Dataset/DataFrame")
    }
    new CassandraDataStreamWriter[T](dataset,
                                     cassandraConnectorBroadcast,
                                     keyspaceName,
                                     tableName,
                                     columnNames,
                                     writeConf,
                                     checkPartitionKey)
  }

}
