package ssv.wikiedits

import java.sql.Timestamp
import java.util.concurrent.TimeUnit

import com.typesafe.scalalogging.StrictLogging
import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeinfo.TypeInformation._
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.api.scala.typeutils.Types.{of => _, _}
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.datastream.DataStreamSink
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.wikiedits.{WikipediaEditEvent, WikipediaEditsSource}
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.types.Row


object WikiEditsFlink extends App with StrictLogging{

  import org.apache.flink.table.api.EnvironmentSettings

  val env = StreamExecutionEnvironment.createLocalEnvironment(3)
  val bsSettings: EnvironmentSettings = EnvironmentSettings.newInstance.useBlinkPlanner.inStreamingMode.build
  val bsTableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env, bsSettings)


  val wikiTypes: Array[TypeInformation[_]] =
    Array(SQL_TIMESTAMP,STRING,STRING,STRING,STRING,INT,STRING,BOOLEAN,BOOLEAN,BOOLEAN,BOOLEAN)

  val wikiTypeNames: Array[String] =
    Array("fetchTime","channel","title","diffUrl","user","byteDiff","summary","isNew","isBotEdit","isMinor","isTalk")

  val wikiSourceStream =
    env.addSource(new WikipediaEditsSource())(of(classOf[WikipediaEditEvent]))
      .map { wikiEvent =>
        logger.info("Received: " + wikiEvent.toString)
        Row.of(new Timestamp(wikiEvent.getTimestamp), wikiEvent.getChannel, wikiEvent.getTitle,
          wikiEvent.getDiffUrl, wikiEvent.getUser, Int.box(wikiEvent.getByteDiff),
          wikiEvent.getSummary, Boolean.box(wikiEvent.isNew), Boolean.box(wikiEvent.isBotEdit),
          Boolean.box(wikiEvent.isMinor), Boolean.box(wikiEvent.isTalk))
      }(new RowTypeInfo(wikiTypes,wikiTypeNames))



  val sourceTable: Table = bsTableEnv.fromDataStream(wikiSourceStream)

  bsTableEnv.executeSql(s"DESCRIBE $sourceTable").print

  val sourceTableWithTime: Table = bsTableEnv.sqlQuery(s"select fetchTime, summary, user, title, proctime() as ts from $sourceTable")

  bsTableEnv.executeSql(s"DESCRIBE $sourceTableWithTime").print

  val sourceStream: DataStream[Row] = bsTableEnv.toAppendStream(sourceTableWithTime)(of(classOf[Row]))

  val groupByTable: Table = bsTableEnv
    .sqlQuery(
      s"""SELECT title,
         | count(1) as edits
         | FROM $sourceTableWithTime
         | group by
         | title,
         | TUMBLE(ts,INTERVAL '1' MINUTE)
         | """.stripMargin)

  bsTableEnv.executeSql(s"DESCRIBE $groupByTable").print

  val groupStream: DataStream[(Boolean, Row)] = bsTableEnv.toRetractStream(groupByTable)(of(classOf[Row]))


  val fileSink: StreamingFileSink[String] = StreamingFileSink
    .forRowFormat(new Path("./src/main/resources/wiki"), new SimpleStringEncoder[String]("UTF-8"))
    .withRollingPolicy(
      DefaultRollingPolicy.builder()
        .withRolloverInterval(TimeUnit.MINUTES.toMillis(15))
        .withInactivityInterval(TimeUnit.MINUTES.toMillis(5))
        .withMaxPartSize(1024 * 1024 * 1024)
        .build())
    .build()

  val flow: DataStreamSink[String] = groupStream.map { entry =>
    logger.info(s"publishing: ${entry.toString}")
    entry.toString
  }(of(classOf[String]))
    .addSink(fileSink)

  env.execute()

}