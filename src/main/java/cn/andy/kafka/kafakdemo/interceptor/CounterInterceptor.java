package cn.andy.kafka.kafakdemo.interceptor;


import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * @Author zhuwei
 * @Date 2020/7/1 14:09
 * @Description: 该interceptor会在消息发送后更新”发送成功消息数“和”发送失败消息数“
 */
public class CounterInterceptor implements ProducerInterceptor<String,String> {

    private int errorCounter = 0;
    private int successCounter = 0;

    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        return record;
    }

    @Override
    public void onAcknowledgement(RecordMetadata recordMetadata, Exception exception) {
        if(exception == null) {
            successCounter++;
        } else {
            errorCounter++;
        }

    }

    @Override
    public void close() {
        //保存结果
        System.out.println("Successful sent: "+successCounter);
        System.out.println("Failed sent: "+errorCounter);
    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
