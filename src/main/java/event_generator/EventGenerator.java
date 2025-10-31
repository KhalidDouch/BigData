package event_generator;

import java.time.Duration;
import java.util.*;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class EventGenerator {

    public static void main(String[] args) {

        /// !!!!!!!!!!!!!!!!!!!!!! Name of group:
        String group = "group1";

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


        // Liste mit allen Produktkategorien
        Map<String, List<Integer>> categories = new HashMap<>();

        categories.put("Monitor", new ArrayList<>(Arrays.asList(101, 102)));
        categories.put("Tastatur", new ArrayList<>(Arrays.asList(201, 202)));
        categories.put("Notebook", new ArrayList<>(Arrays.asList(303, 308, 410)));
        categories.put("Audio", new ArrayList<>(Arrays.asList(304, 305, 306)));
        categories.put("Camera", new ArrayList<>(Arrays.asList(307)));
        categories.put("Tablet", new ArrayList<>(Arrays.asList(302)));
        categories.put("PC", new ArrayList<>(Arrays.asList(898, 899, 888)));

        //Objekt für Random-Berechnungen
        Random random = new Random();

        for (; ; ) { // endless loop

            // zufällige Kategorie auswählen
            List<String> kategorienNamen = new ArrayList<>(categories.keySet());
            String zufallsKategorie = kategorienNamen.get(random.nextInt(kategorienNamen.size()));


            //Zufällige Produkt-ID aus dieser Kategorie auswählen
            List<Integer> produkte = categories.get(zufallsKategorie);
            int zufallsProdukt = produkte.get(random.nextInt(produkte.size()));


            //Zufällige Bestellmenge von 1 bis 3
            int amount = 1 + random.nextInt(3);

            // Kategorienamen als Schlüssel nehmen
            String key = String.valueOf(zufallsProdukt);


            // Als Value wird ein JSON-Format genommen
            String value = String.format(
                    "{\"product_id\": %d, \"amount\": %d, \"category\": \"%s\"}",
                    zufallsProdukt, amount, zufallsKategorie
            );

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
