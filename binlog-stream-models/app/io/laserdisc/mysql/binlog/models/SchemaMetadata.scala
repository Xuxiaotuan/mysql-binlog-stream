package io.laserdisc.mysql.binlog.models

import cats.effect.MonadCancel
import doobie._
import doobie.implicits._

import scala.collection.concurrent.TrieMap

case class Metadata(
                     table_name: String,
                     column_name: String,
                     data_type: String,
                     character_set_name: Option[String],
                     ordinal_position: Int,
                     column_type: String,
                     datetime_precision: Option[Long],
                     column_key: String,
                     is_pk: Boolean
                   )

case class SchemaMetadata(
                           tables: TrieMap[String, TableMetadata],
                           idToTable: TrieMap[Long, String]
                         )

case class ColumnMetadata(name: String, dataType: String, ordinal: Int, isPk: Boolean)
case class TableMetadata(name: String, columns: Map[Int, ColumnMetadata])
object SchemaMetadata {
  def getMetadata(schema: String) =
    sql"""SELECT COLUMNS.TABLE_NAME,
         |  COLUMNS.COLUMN_NAME,
         |  DATA_TYPE,
         |  CHARACTER_SET_NAME,
         |  COLUMNS.ORDINAL_POSITION,
         |  COLUMN_TYPE,
         |  DATETIME_PRECISION,
         |  COLUMN_KEY,
         |  CASE
         |    WHEN KEY_COLUMN_USAGE.ORDINAL_POSITION is null then false
         |    ELSE true
         |  end as is_pk
         |FROM information_schema.COLUMNS
         |  left join information_schema.KEY_COLUMN_USAGE on COLUMNS.TABLE_SCHEMA = KEY_COLUMN_USAGE.TABLE_SCHEMA
         |    and COLUMNS.TABLE_NAME = KEY_COLUMN_USAGE.TABLE_NAME
         |    and CONSTRAINT_NAME = 'PRIMARY'
         |    and COLUMNS.COLUMN_NAME = KEY_COLUMN_USAGE.COLUMN_NAME
         |WHERE COLUMNS.TABLE_SCHEMA = $schema
         |ORDER BY TABLE_NAME, ORDINAL_POSITION""".stripMargin.query[Metadata]

  def getMetadataByTableName(schema: String, tableName: String) =
    sql"""SELECT COLUMNS.TABLE_NAME,
         |  COLUMNS.COLUMN_NAME,
         |  DATA_TYPE,
         |  CHARACTER_SET_NAME,
         |  COLUMNS.ORDINAL_POSITION,
         |  COLUMN_TYPE,
         |  DATETIME_PRECISION,
         |  COLUMN_KEY,
         |  CASE
         |    WHEN KEY_COLUMN_USAGE.ORDINAL_POSITION is null then false
         |    ELSE true
         |  end as is_pk
         |FROM information_schema.COLUMNS
         |  left join information_schema.KEY_COLUMN_USAGE on COLUMNS.TABLE_SCHEMA = KEY_COLUMN_USAGE.TABLE_SCHEMA
         |    and COLUMNS.TABLE_NAME = KEY_COLUMN_USAGE.TABLE_NAME
         |    and CONSTRAINT_NAME = 'PRIMARY'
         |    and COLUMNS.COLUMN_NAME = KEY_COLUMN_USAGE.COLUMN_NAME
         |WHERE COLUMNS.TABLE_SCHEMA = $schema and COLUMNS.TABLE_NAME  = $tableName
         |ORDER BY TABLE_NAME, ORDINAL_POSITION""".stripMargin.query[Metadata]


  def buildSchemaMetadata[F[_]](schema: String, tableName: Option[String] = None)(implicit xa: Transactor[F], ev: MonadCancel[F, Throwable]): F[SchemaMetadata] = {
    tableName match {
      case Some(table) => getMetadataByTableName(schema, table)
      case None => getMetadata(schema)
    }
  }.to[List]
    .map(metaToSchema)
    .transact(xa)

  def metaToSchema(metadata: List[Metadata]): SchemaMetadata = {
    val tables: Map[String, TableMetadata] = metadata
      .groupBy(m => m.table_name)
      .map { case (tableName, tableInfo) =>
        val columns = tableInfo.map(columnInfo =>
          ColumnMetadata(
            columnInfo.column_name,
            columnInfo.data_type,
            columnInfo.ordinal_position,
            columnInfo.is_pk
          )
        )
        tableName -> TableMetadata(
          tableInfo.head.table_name,
          columns.groupBy(_.ordinal).map { case (ord, columns) =>
            ord -> columns.head
          }
        )
      }
    SchemaMetadata(TrieMap.from(tables), TrieMap.empty)
  }

  def empty: SchemaMetadata = SchemaMetadata(TrieMap.empty, TrieMap.empty)

}
