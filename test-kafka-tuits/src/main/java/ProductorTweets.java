import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

public class ProductorTweets {
  public final static String TOPIC_NAME ="topic-tweets";
  public static ObjectMapper objectMapper = new ObjectMapper();

  public static void main(String[] args) {

    Properties props = new Properties();
    props.put("acks", "1");
    props.put("retries", 3);
    props.put("batch.size", 16384);
    props.put("buffer.memory", 33554432);
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,  "localhost:29092,localhost:9093,localhost:9094");

   final KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);

   //leer fichero tweets
    try (BufferedReader br =  new BufferedReader(new FileReader("tweets.txt"))){
      String line;

      while ((line = br.readLine()) != null) {
        JsonNode root;
        try {
          root = objectMapper.readTree(line);
          JsonNode hashtagsNode = root.path("entities").path("hashtags");
          if (!hashtagsNode.toString().equals("")) {
            String value = root.toString();
            String lang = root.path("lang").toString();
            producer.send(new ProducerRecord<>(ProductorTweets.TOPIC_NAME, lang, value));
            //System.out.println("tweet: " + value);
          }
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
    }

  }



}
