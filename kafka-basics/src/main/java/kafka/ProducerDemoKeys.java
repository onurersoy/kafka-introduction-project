package kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoKeys {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemoKeys.class.getSimpleName());

    public static void main(String[] args) {
        log.info("I am a Kafka Producer");

        /*
        Java Programming 101:
            i. Java Producer With Keys
        */

        //Create Producer Properties:
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //Create the Producer:
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        //2. To send multiple data fast, let's create a for loop:
        for (int i = 0; i < 10; i++) {

            String topic = "demo_java";
            String value = "Hello World " + i;
            String key = "id " + i;

            //Create a Producer record:
            ProducerRecord<String, String> producerRecord =
                    new ProducerRecord<>(topic, key, value);

            //Send data - asynchronous type of operation:
            //1. Java Producer Callbacks:
            producer.send(producerRecord, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception e) {
                    //Executes every time a record is succesfully sent or an exception is thrown^^

                    //If the record was successfully sent:
                    if (e == null) {
                        log.info("Received new metadata. \n" +
                                "Topic: " + metadata.topic() + "\n" +
                                "Key: " + producerRecord.key() + "\n" +
                                "Partition: " + metadata.partition() + "\n" +
                                "Offset: " + metadata.offset() + "\n" +
                                "Timestamp: " + metadata.timestamp());

                        //To see what we sent to our topic on terminal:
                        log.info(String.valueOf(producerRecord));

                    } else {
                        log.error("Error wile producing", e);
                    }
                }
            });
        }
        //Flush data - synchronous type of operation
        producer.flush();

        //Flush and close Producer:
        producer.close();
    }
}
