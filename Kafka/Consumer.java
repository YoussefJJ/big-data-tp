import java.util.Properties;
import java.io.IOException;
import java.io.FileWriter;
import java.util.Arrays;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class Consumer {
  public static void main(String[] args) throws Exception {
    if(args.length == 0){
       System.out.println("Entrer le nom du topic");
       return;
    }
    String topicName = args[0].toString();
    Properties props = new Properties();

    props.put("bootstrap.servers", "localhost:9092");
    props.put("group.id", "test");
    props.put("enable.auto.commit", "true");
    props.put("auto.commit.interval.ms", "1000");
    props.put("session.timeout.ms", "30000");
    props.put("key.deserializer",
       "org.apache.kafka.common.serialization.StringDeserializer");
    props.put("value.deserializer",
       "org.apache.kafka.common.serialization.StringDeserializer");

    KafkaConsumer<String, String> consumer = new KafkaConsumer
       <String, String>(props);

    // Kafka Consumer va souscrire a la liste de topics ici
    consumer.subscribe(Arrays.asList(topicName));

    // Afficher le nom du topic
    System.out.println("Subscribed to topic " + topicName);
    int i = 0;
    try {
      FileWriter myWriter = new FileWriter("out.txt");
      System.out.println("Successfully wrote to the file.");

    while (true && i<12) {
       ConsumerRecords<String, String> records = consumer.poll(100);
       for (ConsumerRecord<String, String> record : records) {
       // Afficher l'offset, clef et valeur des enregistrements du consommateur
	System.out.println(record.key() + "," + record.value());       
	myWriter.write(
          record.key() + "," + record.value() + "\n");
	}
	i++;
	myWriter.flush();
    }
	} catch(IOException e) {
	System.out.println("An error occured.");
	e.printStackTrace();
	}
  }
}
