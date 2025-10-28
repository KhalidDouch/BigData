package event_processor_basic;

import org.apache.kafka.streams.kstream.ForeachAction;

import java.util.HashMap;
import java.util.Map;

public class MyProcessor implements ForeachAction<String, String> {

    int maxProduktId = 0;
    int maxAnzahl = 0;
    Map<Integer, Integer> produkte = new HashMap<>();

    @Override
    public void apply(String key, String value) {
        // key = Produktkategorie, value = Umsatz (String, z.B. "3570")
        //System.out.printf("HighRevenue Event  |  Category: %-12s | Revenue: %s%n", key, value);



        // Neues Produkt hinzufügen oder Menge erhöhen:
        int produktId = Integer.parseInt(key);
        int anzahl = Integer.parseInt(value);

        // Falls Produkt schon existiert, erhöhe Menge:
        produkte.put(produktId, produkte.getOrDefault(produktId, 0) + anzahl);
        System.out.println("Neuste Bestellung ist: Produkt-ID: " + produktId + ", Anzahl: " + anzahl);

        /*
        for (Map.Entry<String, Integer> eintrag : produkte.entrySet()) {
            System.out.println("Produkt-ID: " + eintrag.getKey() + ", Anzahl: " + eintrag.getValue());
        }
         */

        printTopSeller(produkte);

    }

    public void printTopSeller(Map<Integer, Integer> produkte) {
        // Variablen für das Maximum


        // Über alle Einträge iterieren
        for (Map.Entry<Integer, Integer> eintrag : produkte.entrySet()) {
            if (eintrag.getValue() > maxAnzahl) {
                maxAnzahl = eintrag.getValue();
                maxProduktId = eintrag.getKey();
            }
        }
        System.out.println("Aktueller Topseller ist, Produkt-ID: " + maxProduktId + ", Anzahl: " + maxAnzahl);
        System.out.println();
    }

}
