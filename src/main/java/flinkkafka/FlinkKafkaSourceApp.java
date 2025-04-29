package flinkkafka;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

public class FlinkKafkaSourceApp {

    public static void main(String[] args) throws Exception {
        // Set up the streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Configure Kafka consumer properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "flink-consumer-group");

        // Create Kafka Consumer
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(
                "topic-flink",                     // Kafka topic
                new SimpleStringSchema(),          // Deserialization schema
                properties                          // Kafka properties
        );


        // Set to start from the latest offset
        consumer.setStartFromLatest();

        // Add Kafka source to the environment
        DataStream<String> stream = env.addSource(consumer);


        // Print the incoming data to stdout
        stream.print();

        // Execute the Flink job
        env.execute("Flink Kafka Consumer Example");
    }
}
