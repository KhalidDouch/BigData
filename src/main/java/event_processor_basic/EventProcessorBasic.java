package event_processor_basic;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.DeserializationFeature;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.apache.kafka.common.serialization.Serdes;

public class EventProcessorBasic {

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    public static void main(String[] args) {
        /// !!!!!!!!!!!!!!!!!!!!!! Name of group:
        String group = "group1"; //// CHANGE ME!!!!

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "stream_processor-" + group);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.111.10:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        System.out.println("*** NOTE: it may take a while until the first events arive");

        final StreamsBuilder builder = new StreamsBuilder();
        final String inputTopic = group + "__sales_by_category";


        // Original-Stream: <String key=product_id, String value=json>
        KStream<String, String> source = builder.stream(inputTopic);

        // Aus den JSON-Values wird ein Event-Objekt erstellt
        KStream<String, Event> events = source.flatMapValues(value -> {
            try {
                //Für jeden Value wird ein Event-Objekt erstellt
                Event e = MAPPER.readValue(value, Event.class); // ObjectMapper konvertiert JSON-String in ein Event-Objekt
                return Collections.singletonList(e);
            } catch (Exception ex) {
                System.err.println("Fehler: " + ex.getMessage());
                return Collections.emptyList(); // Falls JSON ungültig leere Liste zurückgeben
            }
        });

        //Die Top 3 Kategorien nach denen gefiltert werden soll
        List<String> top_categories = Arrays.asList("Audio", "Notebook", "PC");

        // Filtern nach den Top 3 Kategorien
        KStream<String, Event> filtered = events.filter((key, e) -> top_categories.contains(e.getCategory()));

        filtered.foreach(new MyProcessor()); //Jedes Event wird im MyProcessor weiterverarbeitet

        final Topology topology = builder.build();
        final KafkaStreams streams = new KafkaStreams(topology, props);
        final CountDownLatch latch = new CountDownLatch(1);

        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(1);
        }
        System.exit(0);
    }


}
