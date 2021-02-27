package cn.andy.kafka.kafakdemo.book.chapter02;

import cn.andy.kafka.kafakdemo.chapter02.ProducerInterceptorPrefix;
import cn.andy.kafka.kafakdemo.chapter02.ProducerInterceptorPrefixPlus;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * @Description 生产者客户端代码示例
 * @Author zhuwei
 * @Date 2021/1/26 11:51
 */
public class KafkaProducerAnalysis {
    public static final String brokerList = "node01:9092,node02:9092,node03:9092";
    public static final String topic = "topic-demo";

    /**
     * 构建生产者需要的配置信息
     *
     */
    public static Properties initConfig() {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers",brokerList);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        //设置KafkaProducer对应的客户端id，默认值为“”，如果不设置，会自动生成一个非空字符串，内容形式入“producer-1”
        properties.setProperty(ProducerConfig.CLIENT_ID_CONFIG,"producer.client.id.demo");

        //配置拦截器
//        properties.setProperty(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, ProducerInterceptorPrefix.class.getName());

        //可以配置多个拦截器，以逗号分隔
        properties.setProperty(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, ProducerInterceptorPrefix.class.getName()+","+ProducerInterceptorPrefixPlus.class.getName());

        return properties;
    }

    public static void main(String[] args) {
        Properties properties = initConfig();

        //KafkaProducer是线程安全的
        KafkaProducer<String,String> producer = new KafkaProducer<>(properties);


            try {
                while(true) {
                    ProducerRecord producerRecord = new ProducerRecord(topic,"hello kafka!");
                //发送消息主要有三种模式：发后即忘(fire-and-forget)、同步(sync)及异步(async)
                //producer.send(producerRecord);//发后即忘

                //要实现同步的发送方式，可以利用返回的Future对象实现
                //producer.send(producerRecord).get();//阻塞等待Kafka的响应

                //KafkaProducer中一般会发生两种类型的异常：可重试异常和不可重试异常
                //对于可重试异常，可以配置retries参数进行重试来解决，默认为0
                //ProducerConfig.RETRIES_CONFIG

                //异步发送，一般是在send()方法里指定一个Callback的回调函数，Kafka在返回响应时调用该函数了实现异步的发送确认
                //对于同一个分区而言，如果消息record1于record2之前发送，那么KafkaProducer就可以保证对用的callback1在callback2之前调用
                //也就是说回调函数也可以保证分区有序
                producer.send(producerRecord, new Callback() {
                    //onCompletion两个参数时互斥的，要么recordMetadata有值，要么exception有值
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception exception) {
                        if(exception != null) {
                            exception.printStackTrace();
                        } else {
                            System.out.println(recordMetadata.partition()+ ":" + recordMetadata.offset());
                        }
                    }
                });

        }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                //close()方法会阻塞等待之前所有的发送请求完成后再关闭KafkaProducer
                //与此同时还提供了close(long timeout, TimeUnit timeUnit)方法
                //实际应用中使用无参的
                producer.close();
            }

    }

}
