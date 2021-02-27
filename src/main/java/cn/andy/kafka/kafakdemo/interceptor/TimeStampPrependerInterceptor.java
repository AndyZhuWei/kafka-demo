package cn.andy.kafka.kafakdemo.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Map;

/**
 * @Author zhuwei
 * @Date 2020/7/1 14:04
 * @Description: 会在消息发送前将时间戳信息加到消息value的最前部
 */
public class TimeStampPrependerInterceptor implements ProducerInterceptor<String,String> {


    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        return new ProducerRecord<>(record.topic(),record.partition(),record.timestamp(),
                record.key(),System.currentTimeMillis()+","+record.value().toString());
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
