package cn.andy.kafka.kafakdemo.serializer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * @Author zhuwei
 * @Date 2020/6/30 17:22
 * @Description: kafka发送端producer示例代码
 */
public class ProducerTest {

    public static void main(String[] args) {
        Properties props = new Properties();
        //必须指定（如果broker是集群，不必指定所有的broker,因为通过其中一台broker可以找到所有的）
        props.put("bootstrap.servers", "192.168.80.100:9092,192.168.80.100:9093,192.168.80.100:9094");
        //必须指定（即使发送的消息没有指定key）
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        //必须指定,指定自定的序列化器
        props.put("value.serializer", "cn.andy.kafka.kafakdemo.serializer.UserSerializer");

        //使用自定义的partitioner
        props.put("partitioner.class", "cn.andy.kafka.kafakdemo.partitioner.AuditPartitioner");

        //用于控制producer生产消息的持久性(durability),acks指定了在给producer发送响应前，leader broker必须要确保已成功写入
        //该消息的副本数，acks有3个值可以取，分别是0、1和all
        //acks=0 表示producer完全不理睬leader broker端的处理结果
        //acks=all或-1 表示当发送消息时，leader broker不仅会将消息写入本地日志，同时还会等待ISR中所有其他副本都成功写入他们各自的本地日志后，才发送响应结果给producer.
        //显然当设置acks=all，只要ISR中至少有一个副本处于”存活“状态的，那么这条消息就肯定不会丢失，因而可以达到最高的消息持久性，但通常这种设置下producer的吞吐量也是最低的
        //acks=1:是0和all折中的方案，也是默认的参数值。producer发送给消息后leader broker仅将消息吸入本地日志后，然后便发送给响应结果给producer.无须等待ISR中其他副本写入该消息
        props.put("acks", "-1");
        //表示进行重试的次数，可能会导致重复。
        props.put("retries", 3);
        //producer会将发往同一分区的多条消息封装进一个batch中，当batch满了的时候，producer会发送batch中的所有消息。不过，
        //producer并不总是等待batch满了才发送消息，很可能当batch还有很多空闲时producer就发送该batch
        //batch.size参数默认值时16384，即16KB
        props.put("batch.size", 323840);
        //前边说到batch.size参数时提到，batch不满时也会发送，其实这是一种权衡，即吞吐量与延时之间的权衡。
        //linger.ms参数就是控制消息发送延时行为的。该参数默认值为0，表示消息被尽可能快递发送，不关心batch是否被填满，大多数情况下这是合理的。
        //不过这样做会拉低producer吞吐量，
        props.put("linger.ms", 10);
        //producer端用于缓存消息的缓冲区大小，单位是字节，默认是32MB
        props.put("buffer.memory", 33554432);
        props.put("max.block.ms", 3000);

        Producer<String, User> producer = new KafkaProducer<>(props);
        //示例
        String topic = "test-topic2";
       //构建User实例
        User user = new User("XI","HU",33,"BeiJing,China");
        //构建ProducerRecord实例，注意泛型格式是<String,User>
        ProducerRecord<String,User> record = new ProducerRecord<>(topic,user);
        try {
            producer.send(record).get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
        producer.close();


    }

}
