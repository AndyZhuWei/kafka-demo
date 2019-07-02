package cn.andy.kafka.kafakdemo.chapter02;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * @Author: zhuwei
 * @Date:2019/6/24 20:57
 * @Description:
 */
public class KafkaProducerAnalysis {
    public static final String brokerList = "192.168.80.100:9092";
    public static final String topic = "topic-demo";

    public static Properties initConfig() {
        Properties props = new Properties();
       /* props.put("bootstrap.servers",brokerList);
        props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        props.put("client.id","producer.client.id.demo");*/

      /* props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,brokerList);
       props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
       props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
       props.put(ProducerConfig.CLIENT_ID_CONFIG,"producer.client.id.demo");*/

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,brokerList);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,CompanySerializer.class.getName());
        props.put(ProducerConfig.CLIENT_ID_CONFIG,"producer.client.id.demo");
        return props;
    }


    public static void main(String[] args) {
        Properties props = initConfig();
       /*KafkaProducer<String,String> producer = new KafkaProducer<String, String>(props);
        ProducerRecord<String,String> record = new ProducerRecord<>(topic,"hello kafka");*/

        KafkaProducer<String,Company> producer = new KafkaProducer<String, Company>(props);
        Company company = Company.builder().name("hiddenkafka").address("China").build();
        ProducerRecord<String,Company> record = new ProducerRecord<>(topic,company);
        //发送消息
        try {
            producer.send(record).get();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            //关闭生产者客户端示例
            producer.close();
        }

    }
}
