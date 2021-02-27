package cn.andy.kafka.kafakdemo.standaloneConsumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;

import javax.servlet.http.Part;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * @Author zhuwei
 * @Date 2020/7/8 8:45
 * @Description: 独立consumer
 *
 * 使用assign固定地为consumer指定要消费的分区。如果发生多次assign调用，最后一次assign调用的分配生效，之前的都会被覆盖调。
 * assign和subscribe一定不要混用
 */
public class Main {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers","192.168.80.100:9092");
        props.put("auto.offset.reset","earliest");
        props.put("enable.auto.commit","false");
        props.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(props);

        List<TopicPartition> partitions = new ArrayList<>();
        List<PartitionInfo> allPartitions = consumer.partitionsFor("test-topic");
        if(allPartitions != null && !allPartitions.isEmpty()) {
            for(PartitionInfo partitionInfo : allPartitions) {
                partitions.add(new TopicPartition(partitionInfo.topic(),partitionInfo.partition()));
            }
            consumer.assign(partitions);
        }

        try {
            while (true) {
                ConsumerRecords<String,String> records = consumer.poll(Long.MAX_VALUE);
                for(ConsumerRecord<String,String> record : records) {
                    System.out.println(String.format("topic=%s,partition=%d,offset=%d",record.topic(),record.partition(),record.offset()));
                }
                consumer.commitSync();
            }
        } catch (WakeupException e) {
            //此处忽略此异常的处理
        } finally {
            consumer.commitSync();
            consumer.close();
        }
    }
}
