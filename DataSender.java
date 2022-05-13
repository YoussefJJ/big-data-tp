import java.io.File;
import java.util.Properties;
import java.util.Scanner;
import java.lang.Thread;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class DataSender {

    public static void main(String[] args) throws Exception{


      if (args.length == 0){
         System.out.println("Entrer le nom du topic");
         return;
      }

      // Assigner topicName a une variable
      	String topicName = args[0].toString();
	Properties props = new Properties();

        // Assigner l'identifiant du serveur kafka
        props.put("bootstrap.servers", "localhost:9092");

        // Definir un acquittement pour les requetes du producteur
        props.put("acks", "all");

        // Si la requete echoue, le producteur peut reessayer automatiquemt
        props.put("retries", 0);

        // Specifier la taille du buffer size dans la config
        props.put("batch.size", 32768);

        // buffer.memory controle le montant total de memoire disponible au producteur pour le buffering
        props.put("buffer.memory", 33554432);

        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer
                <String, String>(props);
        Scanner sc = new Scanner(new File("processed_data"));
        sc.useDelimiter("\t");   //sets the delimiter pattern
	sc.nextLine();         // read the headers
        while (sc.hasNext())  //returns a boolean value
        {
            String line= sc.nextLine();
            String[] data=line.split("\t");
            producer.send(new ProducerRecord<String, String>(topicName,data[0],data[1]));
        }
        producer.close();
        sc.close();  //closes the scanner
    }

   }
