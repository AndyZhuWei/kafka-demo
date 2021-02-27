package cn.andy.kafka.kafakdemo.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;


/**
 * @Author zhuwei
 * @Date 2020/7/7 9:21
 * @Description: 消费线程类，执行真正的消费任务
 */
public class ConsumerRunnable implements Runnable{

    //每个线程维护私有的KafkaConsumer实例
    private final KafkaConsumer<String,String> consumer;

    public ConsumerRunnable(String brokerList,String groupId,String topic) {
        Properties props = new Properties();
        props.put("bootstrap.servers",brokerList);
        props.put("group.id",groupId);
        //本例使用自动提交位移
        props.put("enable.auto.commit","true");
        props.put("auto.commit.interval.ms","1000");
        props.put("session.timeout.ms","30000");
        props.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        this.consumer = new KafkaConsumer<>(props);
        //本例使用分区副本自动分配策略
        consumer.subscribe(Arrays.asList(topic));

    }
    @Override
    public void run() {
        while(true) {
            ConsumerRecords<String,String> records = consumer.poll(200);
            //本例使用200毫秒作为获取的超时时间
            for(ConsumerRecord<String,String> record : records) {
                //这里面写处理消息的逻辑，本例中只是简单地打印消息
                System.out.println(Thread.currentThread().getName() + " consumed " + record.partition() + "th message with offset:"+record.offset());

            }
        }
    }
}
