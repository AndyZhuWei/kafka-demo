package cn.andy.kafka.kafakdemo.chapter03;

import ch.qos.logback.classic.Logger;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @Author: zhuwei
 * @Date:2019/6/25 11:17
 * @Description:
 */
public class KafkaConsumerAnalysis {
    public static final String brokerList = "192.168.80.100:9092";
    public static final String topic = "topic-demo";
    public static final String groupId = "group.demo";
    public static final AtomicBoolean isRunning = new AtomicBoolean(true);

    public static Properties initConfig() {
        Properties props = new Properties();
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,brokerList);
        props.put(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG,"consumer.client.id.demo");
        return props;
    }


    public static void main(String[] args) {
        Properties props = initConfig();
        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(props);
        consumer.subscribe(Arrays.asList(topic));

        try {
            while(isRunning.get()) {
                //一次拉取操作所获得的消息集
                ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(1000));
                for(ConsumerRecord<String,String> record : records) {
                    System.out.println("topic="+record.topic()+",partition="+record.partition()+",offset="+record.offset());
                    System.out.println("key="+record.key()+",value="+record.value());
                }

                //按照分区维度来进行消费
               /* for(TopicPartition tp : records.partitions()) {
                    for(ConsumerRecord<String,String> record : records.records(tp)) {
                        System.out.println("topic="+record.topic()+",partition="+record.partition()+",offset="+record.offset());
                        System.out.println("key="+record.key()+",value="+record.value());
                    }
                }*/

               //按照主题维度来进行消费
               /* for(String t : Arrays.asList(topic)) {
                    for(ConsumerRecord<String,String> record : records.records(t)) {
                        System.out.println("topic="+record.topic()+",partition="+record.partition()+",offset="+record.offset());
                        System.out.println("key="+record.key()+",value="+record.value());
                    }
                }*/
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            consumer.close();
        }
    }

}
