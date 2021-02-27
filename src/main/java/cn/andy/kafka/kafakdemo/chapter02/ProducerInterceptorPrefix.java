package cn.andy.kafka.kafakdemo.chapter02;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.record.Record;

import java.util.Map;

/**
 * @Author: zhuwei
 * @Date:2019/6/25 10:03
 * @Description:
 * Kafka一共有两种拦截器：生产者拦截器和消费者拦截器
 * 此处讲解生产者拦截器
 * 生产者拦截器既可以用来再消息发送前做一些准备工作，比如按照某个规则过滤不符合要求的消息、修改消息的内容等
 * 也可以用来在发送回调逻辑前做一些定制化的需求，比如统计类工作
 *
 * 自定义拦截器主要是实现ProducerInterceptor接口
 */
public class ProducerInterceptorPrefix implements ProducerInterceptor<String,String> {
    private volatile long sendSuccess = 0;
    private volatile long sendFailure = 0;







    /**
     *
     * @param producerRecord  
     * @return
     * KafkaProducer在将消息序列化和计算分区之前会调用生产者拦截器的onSend()方法来对消息进行相应的定制化操作
     */
    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> producerRecord) {
        String modifiedValue = "prefix1-"+producerRecord.value();
        return new ProducerRecord<>(producerRecord.topic(),
                producerRecord.partition(),producerRecord.timestamp(),
                producerRecord.key(),modifiedValue,producerRecord.headers());
    }

    /**
     *
     * @param recordMetadata
     * @param e
     * KafkaProducer会在消息被应答之前或消息发送失败时调用生产者拦截器的onAcknowledgement方法，优先于Callback之前执行
     * 这个方法运行在Producer的I/O线程中，所以这个方法中实现的代码逻辑越简单越好，否则会影响消息的发送速度
     */
    @Override
    public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {
        if (e == null) {
            sendSuccess++;
        } else {
            sendFailure++;
        }
    }

    /**
     * 关闭拦截器时执行一些资源的清理工作
     */
    @Override
    public void close() {
        double successRatio = (double)sendSuccess/(sendSuccess+sendFailure);
        System.out.println("[INFO]发送成功率="+String.format("%f",successRatio*100)+"%");
    }

    /**以上3个方法抛出异常会被捕获并记录到日志中，但并不会再向上传递**


    /**
     *
     *
     *
     * 继承自Configurable接口的方法，于Partitioner接口一致
     *
     * @param map
     */
    @Override
    public void configure(Map<String, ?> map) {

    }
}
