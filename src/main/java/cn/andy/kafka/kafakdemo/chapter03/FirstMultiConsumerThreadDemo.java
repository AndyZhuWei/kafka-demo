package cn.andy.kafka.kafakdemo.chapter03;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

/**
 * @Author: zhuwei
 * @Date:2019/6/27 17:05
 * @Description: 多线程消费实现方式
 *
 * KafkaConsumerThread代表消费线程，其内部包裹着一个独立的KafkaConsumer实例。外部类的main()方法来启动多个消费线程，消费线程的数量由consumerThreadNum
 * 变量指定。一般一个主题的分区数实现可以知晓，可以将consumerThreadNum设置成不大于分区数的值。
 * 如果不知道主题的分区数，可以通过KafkaConsumer类的partitionsFor方法来间接获取
 * 这种方式和开启多个消费进程的方式没有本质区别，它的优点是每个线程可以按顺序消费各个分区的消息
 * 缺点是消费线程需要维护一个独立的TCP连接，如果分区数和consumerThreadNum值都很大，则会造成不小的系统开销
 *
 */
public class FirstMultiConsumerThreadDemo {
    public static final String brokerList = "192.168.80.100:9092";
    public static final String topic = "topic-demo";
    public static final String groupId = "group.demo";


    public static Properties initConfig() {
        Properties props = new Properties();
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,brokerList);
        props.put(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,true);
        return props;
    }

    public static void main(String[] args) {
        Properties props = initConfig();
        int consumerThreadNum = 4;
        for(int i=0;i<consumerThreadNum;i++) {
            new KafkaConsumerThread(props,topic).start();
        }
    }

    public static class KafkaConsumerThread extends Thread {
        private KafkaConsumer<String,String> kafkaConsumer;

        public KafkaConsumerThread(Properties props,String topic) {
            this.kafkaConsumer = new KafkaConsumer<String, String>(props);
            this.kafkaConsumer.subscribe(Arrays.asList(topic));
        }

        @Override
        public void run() {
            try {
                while(true) {
                    ConsumerRecords<String,String> records = kafkaConsumer.poll(Duration.ofMillis(100));
                    for(ConsumerRecord<String,String> record : records) {
                        //处理消息模块
                        // 如果这里对消息的处理非常的迅速，那么poll拉取的频次也会更高，进而整体消费的性能
                        // 也会提升，相反，如果这里处理很缓慢，比如进行一个事务性操作，或者等待RPC的同步响应，那么poll拉取的频次就会降低
                        //一般而言，poll()拉取消息的速度是相当快的，而整体消费的瓶颈也正是在处理消息这一块，如果我们通过一定的方式来改进这一部分，那么我们
                        //就能带动整体消费性能的提升，参考ThirdMultiConsumerThreadDemo

                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                kafkaConsumer.close();
            }
        }
    }

}
