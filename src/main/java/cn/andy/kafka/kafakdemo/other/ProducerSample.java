package cn.andy.kafka.kafakdemo.other;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.HashMap;
import java.util.Map;

/**
 * @Author: zhuwei
 * @Date:2019/8/15 11:18
 * @Description:
 */
public class ProducerSample {
    public static void main(String[] args) {
        Map<String,Object> props = new HashMap<String,Object>();
        //bootstrap.servers表示Kafka集群。如果集群中有多台物理服务器，则服务器地址之间用逗号分隔，9092是Kafka服务器默认监听的端口号
        props.put("bootstrap.servers","192.168.80.100:9092");
        //key.serializer和value.serializer表示消息的序列化类型。Kafka的消息是以键值对的形式发送到Kafka服务器的，在消息被发送到服务器之前，
        //消息生产者需要把不同类型的消息序列化为二进制类型
        props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        //key.deserializer和value.deserializer表示消息的反序列化类型。把来自Kafka集群的二进制消息反序列化为指定的类型
        props.put("key.deserializer","org.apache.kafka.common.serialization.StringDeSerializer");
        props.put("value.deserializer","org.apache.kafka.common.serialization.StringDeSerializer");
        //用于指定Kafka连接Zookeeper的URL,提供了基于Zookeeper的集群服务器自动感知功能，可以动态从Zookeeper中读取Kafka集群配置信息
        props.put("zk.connect","192.168.80.100:2181");

        String topic = "test-topic";

        //KafkaProducer类来创建一个消息生产者，该类的构造函数入参是一系列属性值
        Producer<String,String> producer = new KafkaProducer<String, String>(props);

        //有了消息生产者之后，就可以调用send方法发送消息了。该方法入参是ProducerRecord类型对象，ProducerRecord提供了多种构造函数，常见的三种如下：
        //ProducerRecord(String topic, K key, V value)
        //ProducerRecord(String topic, V value)
        //ProducerRecord(String topic, Integer partition, K key, V value)
        //其中topic和value是必填的，partition和key是可选的。
        //如果指定了partition，那么消息会被发送至指定的partition
        //如果没有指定partition，但指定了key,那么消息会按照hash(key)发送至对应的partition
        //如果partition和key都没有指定，那么消息会按照round-robin模式发送（即以轮询的方式依次发送）到每一个partition
        producer.send(new ProducerRecord<String,String>(topic,"idea-key2","java-message 1"));
        producer.send(new ProducerRecord<String,String>(topic,"idea-key2","java-message 2"));
        producer.send(new ProducerRecord<String,String>(topic,"idea-key2","java-message 3"));
        producer.close();
    }
}
