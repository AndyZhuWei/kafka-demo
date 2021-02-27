package cn.andy.kafka.kafakdemo.chapter02;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.Utils;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @Author: zhuwei
 * @Date:2019/6/25 9:55
 * @Description: 自定义分区器
 * 消息再通过send方法发往broker的过程中，有可能需要经过拦截器(interceptor)、序列化器(Serializer)和分区器(Partitioner)的一系列
 * 作用后才能真正地发往broker
 *
 * 拦截器一般不需要，序列化器是必须的，经过序列化后就需要确定它发往的分区
 *
 * 如果消息ProducerRecord中指定了partition字段，那么就不需要分区器的作用。因为partition代表的就是所要发往的分区号
 *否则就依赖分区器，根据key来计算partition的值。分区器的作用就是为消息分配分区
 *
 * Kafka默认的分区器是DefaultPartitioner,实现了Partitioner接口
 * 再默认分区器DefaultPartitioner的实现中，close()是空方法，再partition中定义了分区逻辑、。
 * 如果key不为null，那么默认的分区器会对key进行哈希(采用MurmurHash2算法，具备高运算性能及低碰撞率)，最终根据哈希值计算分区号，
 * 如果key为null，那么消息将会以轮询的方式发往主题内的各个可用分区
 *
 * 注意：如果key不为null，那么计算得到的分区号会是所有分区中的任意一个
 *      如果key为null，那么计算得到的分区号仅为可用分区中的任意一个，注意两者之间的差别
 *
 *
 *
 *
 *默认的分区器再key为null的时不会选择非可用的分区，我们可以自定义分区器DemoPartiton来打破这一限制
 *
 * 使用自定义分区器 需要配置ProducerConfig.PARTITIONER_CLASS_CONFIG
 */
public class DemoPartitioner implements Partitioner {

    private final AtomicInteger counter = new AtomicInteger(0);

    /**
     *
     * @param topic 主题
     * @param key 键
     * @param keyBytes 序列化后的键
     * @param value 值
     * @param valueBytes 序列化后的值
     * @param cluster 集群的元数据信息
     * @return 返回分区号
     * 计算分区号
     */
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
        int numPartitions = partitions.size();
        if(null == keyBytes) {
            return counter.getAndIncrement() % numPartitions;
        } else {
            return Utils.toPositive(Utils.murmur2(keyBytes)) % numPartitions;
        }
    }

    /**
     * 关闭分区器的时候来回收一些资源
     */
    @Override
    public void close() {

    }

    /**
     * 获取配置信息及初始化数据
     * @param map
     */
    @Override
    public void configure(Map<String, ?> map) {

    }
}
