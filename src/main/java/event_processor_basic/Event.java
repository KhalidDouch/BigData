package event_processor_basic;


public class Event {

    //Die Klasse dient dazu die empfangenen Werte von Kafka als ein Objekt zu instanziieren

    //Variablen für die eingehenden Events
    private Integer product_id;
    private Integer amount;
    private String category;

    //Getters für die Variablen
    public Integer getProduct_id() { return product_id; }
    public void setProduct_id(Integer product_id) { this.product_id = product_id; }

    public Integer getAmount() { return amount; }
    public void setAmount(Integer amount) { this.amount = amount; }

    public String getCategory() { return category; }
    public void setCategory(String category) { this.category = category; }
}