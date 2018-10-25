package org.apache.spark.sql.sink

import com.datastax.spark.connector.writer.RowWriterFactory
import org.apache.spark.sql.Dataset

import scala.reflect.ClassTag

package object cassandra {
  implicit def toCassandraDataset[T: RowWriterFactory: ClassTag](
      ds: Dataset[T]): CassandraDataset[T] =
    new CassandraDataset[T](ds)
}
