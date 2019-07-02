package cn.andy.kafka.kafakdemo.chapter01;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

/**
 * @Author: zhuwei
 * @Date:2019/6/24 20:23
 * @Description: 消费者客户端示例
 */
public class ConsumerFastStart {
    public static final String brokerList = "192.168.80.100:9092";
    public static final String topic = "topic-demo";
    public static final String groupId = "group.demo";

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("bootstrap.servers",brokerList);
        //设置消费组名称
        properties.put("group.id",groupId);
        //创建一个消费者客户端实例
        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(properties);
        //订阅主题
        consumer.subscribe(Collections.singleton(topic));
        //循环消费消息
        while(true) {
            ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(1000));
            for(ConsumerRecord<String,String> record : records) {
                System.out.println(record.value());
            }
        }
    }
}
