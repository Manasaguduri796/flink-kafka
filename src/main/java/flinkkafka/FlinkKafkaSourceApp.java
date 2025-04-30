package flinkkafka;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
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
                "Dataset-topics",
                new SimpleStringSchema(),
                properties
        );
        consumer.setStartFromLatest();

        // Read from Kafka
        DataStream<String> input = env.addSource(consumer);

        // Add a "message" field to each JSON object
        DataStream<String> output = input.map(value -> {
            try {
                ObjectMapper mapper = new ObjectMapper();
                ObjectNode jsonNode = (ObjectNode) mapper.readTree(value);
                jsonNode.put("message", "Data processed Successfully");
                return jsonNode.toString();
            } catch (Exception e) {
                e.printStackTrace();
                return null;
            }
        });

        // Create Kafka Producer (correct constructor)
        FlinkKafkaProducer<String> producer = new FlinkKafkaProducer<>(
                "processed-Dataset-topics",           // Output topic
                new SimpleStringSchema(),             // Serialization schema
                properties                      // Kafka producer config

        );

        // Send processed data to output Kafka topic
        output.addSink(producer);

        env.execute("Flink Kafka Consumer-Producer Example");
    }
}
