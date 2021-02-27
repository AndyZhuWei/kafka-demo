package cn.andy.kafka.kafakdemo.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

/**
 * @Author zhuwei
 * @Date 2020/7/5 6:03
 * @Description: 消费者实例
 */
public class ConsumerTest {

    public static void main(String[] args) {
        String topicName = "test-topic";
        String groupID = "test-group";

        Properties props = new Properties();
        //必须指定
        props.put("bootstrap.servers","192.168.80.100:9092");
        //必须指定
        props.put("group.id",groupID);
        props.put("enable.auto.commit","true");
        props.put("auto.commit.interval.ms","1000");
        //从最早的消息开始读取
        props.put("auto.offset.reset","earliest");
        //必须指定
        props.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        //必须指定
        props.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        //创建consumer实例
        KafkaConsumer<String,String> consumer = new KafkaConsumer<>(props);
        //订阅topic
        consumer.subscribe(Arrays.asList(topicName));
        try {
            while(true) {
                ConsumerRecords<String,String> records = consumer.poll(1000);
                for(ConsumerRecord<String,String> record:records) {
                    System.out.printf("offset = %d,key=%s,value=%s%n",record.offset(),record.key(),record.value());
                }
            }

        } finally {
            consumer.close();
        }
    }

}
