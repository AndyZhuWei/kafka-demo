package cn.andy.kafka.kafakdemo.consumer;

import java.util.ArrayList;
import java.util.List;

/**
 * @Author zhuwei
 * @Date 2020/7/7 9:53
 * @Description: 消费线程管理类，创建多个线程类执行消费任务
 */
public class ConsumerGroup {

    private List<ConsumerRunnable> consumers;

    public ConsumerGroup(int consumerNum,String groupId,String topic,String brokerList) {
        consumers = new ArrayList<>(consumerNum);
        for(int i=0;i<consumerNum;i++) {
            ConsumerRunnable consumerThread = new ConsumerRunnable(brokerList,groupId,topic);
            consumers.add(consumerThread);
        }
    }

    public void execute() {
        for(ConsumerRunnable task : consumers) {
            new Thread(task).start();
        }
    }
}
