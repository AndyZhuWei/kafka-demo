package cn.andy.kafka.kafakdemo.other;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

/**
 * @Author: zhuwei
 * @Date:2019/8/15 17:33
 * @Description:
 */
public class ConsumerSample {
    public static void main(String[] args) {
        String topic = "test-topic";

        Properties props = new Properties();
        props.put("bootstrap.servers","192.168.80.100:9092");
        //group.id表示消费者的分组ID
        props.put("group.id","testGroup1");
        //enable.auto.commit表示Consumer的offset是否自动提交
        props.put("enable.auto.commit","true");
        //auto.commit.interval.ms用于设置自动提交offset到ZooKeeper的时间间隔，时间单位是毫秒
        props.put("auto.commit.interval.ms","1000");
        props.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");

        Consumer<String,String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(topic));
        while(true) {
            ConsumerRecords<String,String> records = consumer.poll(100);
            for(ConsumerRecord<String,String> record : records) {
                System.out.printf("partition=%d,offset=%d,key=%s,value=%s%n",
                        record.partition(),record.offset(),record.key(),record.value());
            }
        }





    }
}
