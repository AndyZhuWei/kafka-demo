package cn.andy.kafka.kafakdemo.chapter04;

/**
 * @Author: zhuwei
 * @Date:2019/6/27 20:43
 * @Description:
 */
public class TopicCommand {

    public static void createTopic() {
        String[] options = new String[]{"--zookeeper","192.168.80.100:2181/kafka",
                "--create",
                "--replication-factor","1",
                "--partitions","1",
                "--topic","topic-create-api"
        };
        kafka.admin.TopicCommand.main(options);
    }
}
