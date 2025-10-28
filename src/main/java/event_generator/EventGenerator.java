package event_generator;

import java.time.Duration;
import java.util.Properties;
import java.util.Random;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class EventGenerator {

    public static void main(String[] args) {

        /// !!!!!!!!!!!!!!!!!!!!!! Name of group:
        String group = "group1"; //// CHANGE ME!!!!

        // Name of the Kafka topic to publish the events to (please keep the group name
        // as prefix to prevent conflicts with other groups)
        String topic = group + "__sales_by_category"; // note: on the first run with a new topic name you will get a waring
        // regarding a failure to fetch metadata. This happens as the stream
        // is only created after the first message was sent.

        // connect to Kafka and create a producer that lets us send events
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.111.10:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        @SuppressWarnings("resource")
        Producer<String, String> producer = new KafkaProducer<>(props);

        // Now lets send some events:

        // lets assume that we publish an event whenever a product is being sold. The
        // event is structured as follows:
        // - The Event ID is the product ID
        // - The Event content/value is the amount that was sold

        // Liste der Produktkategorien
        String[] categories = {
                "Notebook", "PC", "Camera", "Tablet", "Audio", "Peripherals", "Display"
        };

        Random random = new Random();

        for (;;) { // endless loop

            // zufällige Kategorie auswählen
            String category = categories[random.nextInt(categories.length)];

            // zufälligen Umsatz zwischen 100 und 5000 generieren
            //int revenue = 100 + random.nextInt(4901);
            int product_id = 1 + random.nextInt(9);
            int amount = 1 + random.nextInt(3);

            //String key = category; // Kategorie als Schlüssel
            //String value = String.valueOf(revenue); // Umsatz als String-Wert

            String key = String.valueOf(product_id); // Kategorie als Schlüssel
            String value = String.valueOf(amount); // Umsatz als String-Wert

            // now create and send the event to Kafka
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
            producer.send(record, (RecordMetadata metadata, Exception exception) -> {
                if (exception != null) {
                    exception.printStackTrace();
                } else {
                    System.out.printf("Sent event(key=%s value=%s)%n", key, value);
                }
            });

            // wait a little
            try {
                Thread.sleep(Duration.ofSeconds(2));
            } catch (InterruptedException e) {
            }
        }
    }

}
