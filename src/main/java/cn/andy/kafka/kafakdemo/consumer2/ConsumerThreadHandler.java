package cn.andy.kafka.kafakdemo.consumer2;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @Author zhuwei
 * @Date 2020/7/7 10:14
 * @Description: consumer多线程管理类，用于创建线程池以及为每个线程分配消息集合。另外consumer位移提交也在该类中完成
 */
public class ConsumerThreadHandler<K,V> {

    private final KafkaConsumer<K,V> consumer;
    private ExecutorService executors;
    private final Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();

    public ConsumerThreadHandler(String brokerList,String groupId,String topic) {
        Properties props = new Properties();
        //必须指定
        props.put("bootstrap.servers",brokerList);
        props.put("group.id",groupId);
        props.put("enable.auto.commit","false");
        props.put("auto.offset.reset","earliest");
        props.put("auto.offset.reset","earliest");
        props.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        consumer = new KafkaConsumer<K, V>(props);
        consumer.subscribe(Arrays.asList(topic), new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> collection) {
                //提交位移
                consumer.commitSync(offsets);
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> collection) {
                offsets.clear();
            }
        });
    }


    public void consume(int threadNumber) {
        executors = new ThreadPoolExecutor(
                threadNumber,
                threadNumber,
                0L,
                TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(1000),
                new ThreadPoolExecutor.CallerRunsPolicy());
        try {
            while(true) {
                ConsumerRecords<K,V> records = consumer.poll(1000L);
                if(!records.isEmpty()) {
                    executors.submit(new ConsumerWorker<>(records,offsets));
                }
                commitOffsets();
            }
        } catch (WakeupException e) {
            //此处忽略此异常的处理
        } finally {
            commitOffsets();
            consumer.close();
        }
    }

    private void commitOffsets() {
        //尽量降低synchronized块对offsets锁定的时间
        Map<TopicPartition,OffsetAndMetadata> unmodfiedMap;
        synchronized (offsets) {
            if(offsets.isEmpty()) {
                return;
            }
            unmodfiedMap = Collections.unmodifiableMap(new HashMap<>(offsets));
            offsets.clear();
        }
        consumer.commitSync(unmodfiedMap);
    }

    public void close() {
        consumer.wakeup();
        executors.shutdown();
    }


}
