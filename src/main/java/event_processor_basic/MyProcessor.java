package event_processor_basic;

import org.apache.kafka.streams.kstream.ForeachAction;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class MyProcessor implements ForeachAction<String, Event> {

    //Hashmap mit Kategorie als Key und einer Hashmap als Value (productId & totalAmount)
    private final Map<String, Map<Integer, Integer>> categoryMap = new HashMap<>();

    @Override
    public void apply(String key, Event e) {

        //Kategorie, ID und Bestellmenge des Event-Objekts auslesen
        String category = e.getCategory();
        int productId = e.getProduct_id();
        int amount = e.getAmount();


        //Prüft, ob in der HashMap categoryMap bereits ein Schlüssel mit dem Produktnamen exisitert.
        //Wenn ja, wird die bereits existierende HashMap zurückgegeben.
        //Wenn nein, erstellt es eine neue HashMap und fügt sie unter diesem Schlüssel ein.
        Map<Integer, Integer> productMap = categoryMap.computeIfAbsent(category, k -> new HashMap<>());


        //Hier werden die Bestellmengen in den HashMaps addiert
        //Falls das Produkt noch keine Menge hat wird einfach 0 als default genommen
        productMap.put(productId, productMap.getOrDefault(productId, 0) + amount);


        // Top-Sellers der Kategorien ausgeben
        printTopSellers();

    }

    private void printTopSellers() {

        //Neue leere Zeilen ausgeben
        System.out.println("\n \n \n \n");
        //Infozeile ausgeben
        System.out.println("=== Aktuelle Topseller der Top-3 Kategorien ===");

        //Iteration über alle Kategorien
        for (Map.Entry<String, Map<Integer, Integer>> categoryEntry : categoryMap.entrySet()) {

            //Kategoriennamen auslesen
            String category = categoryEntry.getKey();
            //Hashmap mit ProduktID und Bestellmenge auslesen
            Map<Integer, Integer> productMap = categoryEntry.getValue();

            int maxProductId = -1; //Platzhalter für Topseller
            int maxAmount = 0; //Platzhalter für aktuelle Bestellmenge des Topsellers

            //Iteration über die Produkte in der Produktkategorie
            for (Map.Entry<Integer, Integer> entry : productMap.entrySet()) {

                //Überprüfen ob aktuelle Bestellmenge die höchste ist
                if (entry.getValue() > maxAmount) {
                    //Falls ja
                    maxAmount = entry.getValue();//die aktuelle Bestellmenge als Höchstmenge speichern
                    maxProductId = entry.getKey(); //Aktuelles Produkt als Topseller speichern
                }
            }

            //Top-Sellers im Stringformat ausgeben
            System.out.printf("Kategorie: %-10s | Topseller: %-5d | Gesamtmenge: %d%n", category, maxProductId, maxAmount);
        }
        System.out.println("================================================\n");
    }


}
