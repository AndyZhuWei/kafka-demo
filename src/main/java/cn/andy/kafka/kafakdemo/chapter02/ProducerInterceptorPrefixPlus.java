package cn.andy.kafka.kafakdemo.chapter02;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * @Author: zhuwei
 * @Date:2019/6/25 10:03
 * @Description:
 *
 * KafkaProducer中不仅可以指定一个拦截器，还可以指定多个拦截器以形成拦截链。拦截链会按照interceptor.classes参数配置的拦截器的
 * 顺序来一一执行（配置的时候，各个拦截器之间使用逗号隔开）
 *
 *
 * 如果拦截链中的某个拦截器的执行需要依赖于前一个拦截器的输出，那么就有可能产生“副作用”。
 * 在拦截链中，如果某个拦截器执行失败，那么下一个拦截器会接着从上一个执行成功的拦截器继续执行
 *
 *
 *
 */
public class ProducerInterceptorPrefixPlus implements ProducerInterceptor<String,String> {

    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> producerRecord) {
        String modifiedValue = "prefix2-"+producerRecord.value();
        return new ProducerRecord<>(producerRecord.topic(),
                producerRecord.partition(),producerRecord.timestamp(),
                producerRecord.key(),modifiedValue,producerRecord.headers());
    }


    @Override
    public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {

    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
