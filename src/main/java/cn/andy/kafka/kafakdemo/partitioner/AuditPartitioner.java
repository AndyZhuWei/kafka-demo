package cn.andy.kafka.kafakdemo.partitioner;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;

import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * @Author zhuwei
 * @Date 2020/6/30 22:08
 * @Description: 自定义partitioner
 * 假设我们的消息中有一些消息时用于审计功能的，这类消息的key会被固定地分配一个字符串”audit“.我们想要让这类消息发送到topic的最后
 * 一个分区上，便于后续统一处理，而对于相同topic下的其他消息则采用随机发送的策略发送到其他分区上。
 */
public class AuditPartitioner implements Partitioner {

    private Random random;

    /**
     * @Description
     * @Author zhuwei
     * @Date 2020/6/30 22:16
     * @Param
     * @Return
     * @Exception
     *
     */
    @Override
    public int partition(String topic, Object keyObj, byte[] keyBytes,
                         Object value, byte[] valueBytes, Cluster cluster) {
        String key = (String)keyObj;
        List<PartitionInfo> partitionInfoList = cluster.availablePartitionsForTopic(topic);
        int partitionCount = partitionInfoList.size();
        int auditPartition = partitionCount - 1;
        return key == null || key.isEmpty() || !key.contains("audit") ? random.nextInt(partitionCount - 1) : auditPartition;
    }

    @Override
    public void configure(Map<String, ?> map) {
        //该方法实现必要资源的初始化工作
        random = new Random();
    }

    @Override
    public void close() {
        //该方法实现必要资源的清理工作

    }
}
