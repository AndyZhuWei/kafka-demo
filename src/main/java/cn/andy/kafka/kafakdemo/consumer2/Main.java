package cn.andy.kafka.kafakdemo.consumer2;

/**
 * @Author zhuwei
 * @Date 2020/7/7 10:41
 * @Description: 测试主方法
 * 全局consumer+多worker线程
 *
 * 优点：
 * 1消息获取和处理解耦
 * 2.可独立扩展consumer数和worker数，伸缩性好
 *
 * 缺点
 * 1实现负载
 * 2难于维护分区内的消息顺序
 * 3处理链路变长，导致位移管理困难
 * 4worker线程异常可能导致消费数据丢失
 */
public class Main {

    public static void main(String[] args) {
        String brokerList = "192.168.80.100:9092";
        String topic = "test-topic";
        String groupID = "test-group";
        final ConsumerThreadHandler<byte[],byte[]> handler = new ConsumerThreadHandler<>(brokerList,groupID,topic);
        final int cpuCount = Runtime.getRuntime().availableProcessors();

        Runnable runnable = new Runnable() {
            @Override
            public void run() {
                handler.consume(cpuCount);
            }
        };
        new Thread(runnable).start();

        try {
            //20秒后自动停止该测试程序
            Thread.sleep(20000L);
        } catch (InterruptedException e) {
            //此处忽略此异常的处理
        }
        System.out.println("Starting to close the consumer...");
        handler.close();
    }


}
