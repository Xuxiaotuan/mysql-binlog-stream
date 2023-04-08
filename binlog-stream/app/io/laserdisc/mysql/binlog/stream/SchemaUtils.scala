package io.laserdisc.mysql.binlog.stream

import cats.effect.IO
import io.laserdisc.mysql.binlog.config.BinLogConfig
import io.laserdisc.mysql.binlog.database
import io.laserdisc.mysql.binlog.models.SchemaMetadata

object SchemaUtils {

  def buildSchemaMetadata(config: BinLogConfig, tableName: String): IO[SchemaMetadata] =
    database.transactor[IO](config).use { implicit xa =>
      for {
        schemaMetadata <- SchemaMetadata.buildSchemaMetadata(config.schema, Some(tableName))
      } yield schemaMetadata
    }
}

