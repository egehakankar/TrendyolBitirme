import java.util.Properties

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.streaming.util.serialization._

object FlinkTrendyol {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("group.id", "test")
    val stream = env.addSource(new FlinkKafkaConsumer("Orders", new JSONKeyValueDeserializationSchema(true), properties)).print()

    env.execute("FlinkTre")
  }
}