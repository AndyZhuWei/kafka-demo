package cn.andy.kafka.kafakdemo.consumer;

/**
 * @Author zhuwei
 * @Date 2020/7/7 9:54
 * @Description: 测试主方法类
 * 每个线程维护一个KafkaConsumer
 * 优点：
 * 1实现简单
 * 2速度较快，因为无线程间交互开销；
 * 3方便位移管理
 * 4易于维护分区间的消息消费顺序
 *
 * 缺点：
 * 1.Socket连接开销大
 * 2.consumer数受限于topic分区数，扩展性差
 * 3.broker端处理负载高（因为发往broker的请求数多）
 * 4.rebalance可能性增大
 */
public class ConsumerMain {

    public static void main(String[] args) {
        String brokerList = "192.168.80.100:9092";
        String groupId = "testGroup1";
        String topic = "test-topic";
        int consumerNum = 3;

        ConsumerGroup consumerGroup = new ConsumerGroup(consumerNum,groupId,topic,brokerList);
        consumerGroup.execute();
    }
}
