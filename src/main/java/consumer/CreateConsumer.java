package consumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class CreateConsumer {
    public static void main(String[] args){
        Properties props =new Properties();
        props.put("bootstrap.servers","localhost:9092");
        props.put("group.id", "test");
        props.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");

        Consumer<String ,String> consumer = new KafkaConsumer<String, String>(props);

        consumer.subscribe(Collections.singletonList("test"));

        try{
            while(true){
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for(ConsumerRecord<String,String> record :records){
                    System.out.printf("Topic = %s, Partition = %s, Offset = %s, Value = %s\n",record.topic(),record.partition(),record.offset(),record.value());
                }
            }
        }
        finally {
            consumer.close();
        }
    }
}
