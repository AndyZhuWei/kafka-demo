package cn.andy.kafka.kafakdemo.chapter03;

import com.sun.scenario.effect.Offset;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @Author: zhuwei
 * @Date:2019/6/27 17:35
 * @Description:
 *
 * KafkaConsumer非线程安全并不意味着我们再消费消息的时候只能以单线程的方式执行。
 * 如果生产者发送消息的速度大于消费者处理消息的速度，那么就会有越来越多的消息得不到及时的消费，造成了一定的延迟。
 * 除此之外，由于Kafka中消息保留机制的作用，有些消息有可能在被消费之前就被清理了，从而造成消息的丢失。
 * 多线程的实现方式有多种，第一种也是最常见的方式：线程封闭，即为每个线程实例化一个KafkaConsumer对象
 *
 * 一个线程对应一个KafkaConsumer实例，我们可以称之为消费线程。一个消费线程可以消费一个或多个分区中的消息，所有的消费线程都隶属于同一个消费组。
 * 这种实现方式的并发度受限于分区的实际个数，根据之前的知识点，当消费线程的个数大于分区数时，就有部分消费线程一直处于空闲的状态。
 *
 * 与此对应的第二种方式时多个消费者线程同时消费同一个分区，这个通过assign()、seek()等方法实现。这样可以打破原有的消费者线程的个数不能超过分区数的限制
 * ，进一步提高了消费的能力。不过这种实现方式对于位移的提交和顺序控制的处理就会变得非常复杂，实际应用中使用的极少，并不推荐。一般而言，分区是消费线程
 * 的最小划分单位。
 *
 *
 *
 *
 *
 */
public class ThirdMultiConsumerThreadDemo {
    public static final String brokerList = "192.168.80.100:9092";
    public static final String topic = "topic-demo";
    public static final String groupId = "group.demo";
    private static Map<TopicPartition, OffsetAndMetadata> offsets;

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
        KafkaConsumerThread consumerThread = new KafkaConsumerThread(props,topic,Runtime.getRuntime().availableProcessors());
        consumerThread.start();
    }

    public static class KafkaConsumerThread extends Thread {
        private KafkaConsumer<String,String> kafkaConsumer;
        private ExecutorService executorService;
        private int threadNumber;

        public KafkaConsumerThread(Properties props,String tipic,int threadNumber) {
            kafkaConsumer = new KafkaConsumer<String, String>(props);
            kafkaConsumer.subscribe(Collections.singletonList(topic));
            this.threadNumber=threadNumber;
            executorService = new ThreadPoolExecutor(threadNumber,threadNumber,0L, TimeUnit.MILLISECONDS,new ArrayBlockingQueue<>(1000),
                    new ThreadPoolExecutor.CallerRunsPolicy());
        }

        @Override
        public void run() {
            try {
                while (true) {
                    ConsumerRecords<String,String> records =
                            kafkaConsumer.poll(Duration.ofMillis(100));
                    if(!records.isEmpty()) {
                        executorService.submit(new RecordsHandler(records));
                    }
                    synchronized (offsets) {
                        if(!offsets.isEmpty()) {
                            kafkaConsumer.commitSync(offsets);
                            offsets.clear();
                        }
                    }

                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                kafkaConsumer.close();
            }
        }

    }
    public static class RecordsHandler extends Thread {
        public final ConsumerRecords<String,String> records;

        public RecordsHandler(ConsumerRecords<String,String> records) {
            this.records = records;
        }

        @Override
        public void run() {
            //处理records
            for(TopicPartition tp : records.partitions()) {
                List<ConsumerRecord<String,String>> tpRecords = records.records(tp);
                //处理tpRecords
                long lastConsumedOffset = tpRecords.get(tpRecords.size()-1).offset();
                synchronized (offsets) {
                    if(!offsets.containsKey(tp)) {
                        offsets.put(tp,new OffsetAndMetadata(lastConsumedOffset+1));
                    } else {
                        long position = offsets.get(tp).offset();
                        if(position < lastConsumedOffset + 1) {
                            offsets.put(tp,new OffsetAndMetadata(lastConsumedOffset+1));
                        }
                    }
                }
            }
        }
    }

}
