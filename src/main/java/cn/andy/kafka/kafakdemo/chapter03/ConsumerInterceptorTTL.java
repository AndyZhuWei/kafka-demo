package cn.andy.kafka.kafakdemo.chapter03;

import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @Author: zhuwei
 * @Date:2019/6/27 16:28
 * @Description:
 * 消费者拦截器主要再消费到消息或者提交消费位移时进行一些定制化的操作
 *
 *
 * 对消息设置一个有效期的属性，如果某条消息再既定的时间窗口内无法到达，那么就会被视为无效，它也就不需要再被继续处理了。
 * TTL(time to live 过期时间)
 *
 *
 * 自定义消费者拦截器后，需要再消费者客户端参数中进行配置，与生产者配置类似。
 * 使用消费者拦截器的时候需要注意的是消费位移的提交，再使用带有参数提交位移的时候，有可能会出现错我，比如过滤了偏移量最大的消息，那么此时提交就会出错
 * 同样消费者拦截器也有拦截器链配置方式和生产者拦截器链一样
 * 需要注意提防“副作用”的发生，如果再拦截器链中某个拦截器执行失败，那么下一个拦截器会接着从上一个执行成功的拦截器继续执行。
 *
 */
public class ConsumerInterceptorTTL implements ConsumerInterceptor<String,String> {

    //过期的时间间隔
    private static final long EXPIRE_INTERVAL = 10 * 1000;


    /**
     * KafkaConsumer会在poll方法返回之前调用拦截器的onConsume()方法来对消息进行相应的定制化操作，比如修改返回的消息内容、按照某种规则过滤消息
     *  * （可能会减少poll方法返回的消息的个数）。
     *  如果此方法抛出异常，那么会被捕获并记录到日志中，但是异常不会再向上传递
     * @param consumerRecords
     * @return
     */
    @Override
    public ConsumerRecords<String, String> onConsume(ConsumerRecords<String, String> consumerRecords) {
        long now = System.currentTimeMillis();
        Map<TopicPartition, List<ConsumerRecord<String,String>>> newRecords = new HashMap<>();
        for(TopicPartition tp :consumerRecords.partitions() ) {
            List<ConsumerRecord<String,String>> tpRecords = consumerRecords.records(tp);
            List<ConsumerRecord<String,String>> newTpRecords = new ArrayList<>();
            for(ConsumerRecord<String,String> record : tpRecords) {
                if(now - record.timestamp() < EXPIRE_INTERVAL) {
                    newTpRecords.add(record);
                }
            }
            if(!newTpRecords.isEmpty()) {
                newRecords.put(tp,newTpRecords);
            }
        }

        return new ConsumerRecords<>(newRecords);
    }

    /**
     * KafkaConsumer会再提交消费位移之后调用拦截器的onCommit()方法，可以使用这个方法来记录跟踪所提交的位移信息，比如消费者使用
     * commitSync()的无参方法时，我们不知道提交的消费位移的具体细节。而使用拦截器的onCommit()方法却可以做到这一点
     * @param offsets
     */
    @Override
    public void onCommit(Map<TopicPartition, OffsetAndMetadata> offsets) {
        offsets.forEach((tp,offset) -> System.out.println(tp+":"+ offset.offset()));
    }


    /**
     * 与生成产者一致
     */
    @Override
    public void close() {

    }

    /**
     * 与生成产者一致
     */
    @Override
    public void configure(Map<String, ?> map) {

    }
}
