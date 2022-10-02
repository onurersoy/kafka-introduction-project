package kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemo {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemo.class.getSimpleName());

    public static void main(String[] args) {
        //System.out.println("Hello world!");
        log.info("Hello world!");

        /*
        Kafka Producer: Java API - Basics
            1. Writing a basic producer to send data to Kafka
            2. View basic configuration parameters
            3.Confirm you receive the data in a Kafka Console Consumer
        */

        //1. Create Producer Properties:
        Properties properties = new Properties();
        //codeLine: properties.setProperty("bootstrap.servers", "localhost:9092");
        //To avoid typo mistakes, you can import ProducerConfig and update the above line as:
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //2. Create the Producer:
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        //3. Create a Producer record:
        ProducerRecord<String, String> producerRecord =
                new ProducerRecord<>("demo_java", "hello world");

        //4. Send data - asynchronous type of operation:
        producer.send(producerRecord);

        //5. Flush data - synchronous type of operation
        producer.flush();

        //6. Flush and close Producer:
        producer.close();
        /* Using 'close' also calls 'flush' for you so 5th step is not necessary;
        it is there just to show that a 'flush' command separately exists^^ */
    }
}
