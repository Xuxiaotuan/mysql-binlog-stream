package io.laserdisc.mysql.binlog.stream

import cats.effect._
import cats.effect.std.{Dispatcher, Queue}
import cats.implicits._
import com.github.shyiko.mysql.binlog.BinaryLogClient
import com.github.shyiko.mysql.binlog.event.Event
import fs2.Stream
import org.typelevel.log4cats.Logger

import scala.concurrent.duration.{DurationInt, FiniteDuration}

class MysSqlBinlogEventProcessor[F[_]: Async: Logger](
    binlogClient: BinaryLogClient,
    queue: Queue[F, Option[Event]],
    dispatcher: Dispatcher[F],
    monitorName: String = "default"
) {

  def run(): Unit = {

    binlogClient.registerEventListener { event =>
      dispatcher.unsafeRunSync(queue.offer(Some(event)))
    }

    binlogClient.registerLifecycleListener(new BinaryLogClient.LifecycleListener {
      override def onConnect(client: BinaryLogClient): Unit =
        dispatcher.unsafeRunAndForget(Logger[F].info(s"${monitorName} Connected"))

      override def onCommunicationFailure(client: BinaryLogClient, ex: Exception): Unit = {
//        dispatcher.unsafeRunAndForget(
//          Logger[F].error(ex)(s"${monitorName} communication failed with") >> queue.offer(None)
//        )
        // todo filter this out for now, don't want to stop service
        dispatcher.unsafeRunAndForget(Logger[F].error(ex)(s"${monitorName} communication failed with" + ex.printStackTrace()))
      }

      override def onEventDeserializationFailure(client: BinaryLogClient, ex: Exception): Unit = {
      //        dispatcher.unsafeRunAndForget(
      //          Logger[F].error(ex)("failed to deserialize event") >> queue.offer(None)
      //        )
      // todo filter this out for now, don't want to stop service
        dispatcher.unsafeRunAndForget(Logger[F].error(ex)(s"${monitorName} failed to deserialize event" + ex.printStackTrace()))
      }

      override def onDisconnect(client: BinaryLogClient): Unit = {
        dispatcher.unsafeRunAndForget(Logger[F].error(s"${monitorName} Disconnected  queue size: ${queue.size}") >> queue.offer(None))
        throw new Exception("binlogClient disconnected")
      }
    })

    binlogClient.connect()
  }
}

object MysqlBinlogStream {

  def rawEvents[F[_]: Async: Logger: LiftIO](
      client: BinaryLogClient,
      checkInterval: FiniteDuration = 10.seconds,
      monitorName: String = "default"
  ): Stream[F, Event] = {
    for {
      d <- Stream.resource(Dispatcher[F])
      q <- Stream.eval(Queue.bounded[F, Option[Event]](10000))
      proc = new MysSqlBinlogEventProcessor[F](client, q, d, monitorName)
      /* some difficulties here during the cats3 migration.  Basically, we would have used:
       * .eval(Async[F].interruptible(many = true)(proc.run()))
       * instead of the below code to start `proc`.  Unfortunately, the binlogger library uses SocketStream.read
       * which blocks and can't be terminated normally.  See https://github.com/typelevel/fs2/issues/2362 */
      procStream = Stream
        .eval(
          LiftIO[F].liftIO(
            IO.delay[Unit](proc.run()).start.flatMap(_.joinWithNever)
          )
        )
      monitorStream = Stream
        .fixedRate[F](checkInterval)
        .evalMap { _ =>
          Async[F].delay(client.isConnected).flatMap { connected =>
            if (connected) {
              Logger[F].info(s"Binlog Stream ${monitorName} Monitor is running")
            } else {
              Logger[F].error(s"Binlog Stream ${monitorName} Monitor is stopping") >>
                Async[F].delay(client.disconnect())
            }
          }
        }

      evtStream <- Stream
        .fromQueueNoneTerminated(q)
        .concurrently(procStream)
        .concurrently(monitorStream)
        .onFinalize(Async[F].delay(client.disconnect()))
    } yield evtStream
  }

}
