package main


import cats.effect.{ExitCode, IO, IOApp}
import doobie._
import doobie.implicits._
import io.laserdisc.mysql.binlog.models.Metadata

/**
 * @author : XuJiaWei
 * @since : 2023-03-31 18:24
 */

object doobie_1 extends IOApp {
  def run(args: List[String]): IO[ExitCode] = {
    val xa = Transactor.fromDriverManager[IO](
      "com.mysql.cj.jdbc.Driver",
      "jdbc:mysql://localhost:3306/faker",
      "root",
      "root"
    )
    val chunkSize = 1000

    def getMetadataByOffset(schema: String, offset: Int, chunkSize: Int) =
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
           |ORDER BY TABLE_NAME, ORDINAL_POSITION  LIMIT $chunkSize OFFSET $offset""".stripMargin.query[Metadata]

    def processChunk(offset: Int): IO[List[Metadata]] = {
      getMetadataByOffset("faker", offset, 1000)
        .stream
        .transact(xa)
        .compile
        .toList
    }

    def processChunks(offset: Int, accumulated: List[Metadata]): IO[List[Metadata]] = {
      processChunk(offset).flatMap { chunkData: Seq[Metadata] =>
        if (chunkData.nonEmpty) {
          processChunks(offset + chunkSize, accumulated ++ chunkData)
        } else {
          IO.pure(accumulated)
        }
      }
    }

    // 使用 processChunks(0, List.empty) 启动递归，开始数据收集
    processChunks(0, List.empty)
      .map(e => println(e))
      .as(ExitCode.Success)
  }
}



