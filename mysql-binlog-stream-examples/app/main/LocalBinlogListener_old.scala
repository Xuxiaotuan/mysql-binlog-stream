package main

import cats.effect.{ExitCode, IO, IOApp}
import cats.implicits._
import ciris._
import ciris.refined._
import eu.timepit.refined.auto._
import eu.timepit.refined.types.string.TrimmedString
//import io.laserdisc.mysql.binlog.checkpoint.BinlogOffset
import io.laserdisc.mysql.binlog.config.BinLogConfig
import io.laserdisc.mysql.binlog.models.SchemaMetadata
import io.laserdisc.mysql.binlog.stream.{MysqlBinlogStream, TransactionState, streamEvents}
import io.laserdisc.mysql.binlog.{client, database}
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

object LocalBinlogListener_old extends IOApp {

  override def run(args: List[String]): IO[ExitCode] = {

    val DB_HOST = "127.0.0.1"
    val DB_PORT = 3306
    val DB_USER = "root"
    val DB_PASSWORD = "root"
    val DB_URL = "jdbc:mysql://127.0.0.1:3306/faker?useSSL=false"
    val DB_SCHEMA = "faker"

    val USE_SSL     = false

    val conf: IO[BinLogConfig] =
      (
        ConfigValue.loaded(ConfigKey.env("DB_HOST"), DB_HOST).as[TrimmedString],
        ConfigValue.loaded(ConfigKey.env("DB_PORT"), DB_PORT).as[Int],
        ConfigValue.loaded(ConfigKey.env("DB_USER"), DB_USER).as[TrimmedString],
        ConfigValue.loaded(ConfigKey.env("DB_PASSWORD"), DB_PASSWORD),
        ConfigValue.loaded(ConfigKey.env("DB_URL"), DB_URL).option,
        ConfigValue.loaded(ConfigKey.env("DB_SCHEMA"), DB_SCHEMA),
        ConfigValue.loaded(ConfigKey.env("USE_SSL"), USE_SSL).as[Boolean]
      ).parMapN { case (host, port, user, password, url, schema, useSSL) =>
        BinLogConfig(
          host,
          port,
          user,
          password,
          schema,
          poolSize = 1,
          useSSL = useSSL,
          urlOverride = url
        )
      }.load[IO]
    //    val binlogOffset = Some(BinlogOffset(null, "mysql-bin.000050", 245530144))

    conf
      .flatMap { config =>
        database.transactor[IO](config).use { _ =>
          for {
            implicit0(logger: Logger[IO]) <- Slf4jLogger.fromName[IO]("application")
            // Here we do not provide binlog offset, client will be initialized with default file and offset
            //            binlogClient <- client.createBinLogClient[IO](config, binlogOffset)
            binlogClient   <- client.createBinLogClient[IO](config)
            //            schemaMetadata   <- SchemaMetadata.buildSchemaMetadata(config.schema, Some("f_test01表单_26f126fbe977c467_fe630"))
            transactionState <- TransactionState.createTransactionState[IO](SchemaMetadata.empty, binlogClient, config)
            _ <- {
              MysqlBinlogStream
                .rawEvents[IO](binlogClient)
                .evalTap(msg => logger.info(s"received $msg"))
                //                .through(streamEvents[IO](transactionState, config.schema))
                .through(streamEvents[IO](transactionState, config.schema))
                // Here you should do the checkpoint
                .compile
                .drain
            }
          } yield (ExitCode.Success)
        }
      }
  }

}