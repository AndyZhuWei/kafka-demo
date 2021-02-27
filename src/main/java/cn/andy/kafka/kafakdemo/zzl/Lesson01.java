package cn.andy.kafka.kafakdemo.zzl;

import kafka.tools.ConsoleProducer;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * @Date 2021-01-24 09:47:12
 */
public class Lesson01 {

    @Test
    public void producer() throws ExecutionException, InterruptedException {
        String topic = "msb-items";
        Properties p = new Properties();
        p.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"node02:9092,node03:9092,node04:9092");
        //kafka 持久化数据的MQ,  数据->byte[] ，不会对数据进行干预，双方要约定编解码
        //kafka是一个app:使用零拷贝 sendfile 系统调用实现快速数据消费
        p.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(p);

        //现在的producer就是一个提供者，面向的其实是broker，虽然再使用的时候我们期望把数据打入topic
        /*
         msb-items
         2个partition
         三种商品，每种商品有线性的3个ID
         相同的商品最好去到一个分区里,这样kafka就可以保证同一个分区消息的有序性
         */
        while(true) {
            for(int i=0;i<3;i++) {
                for(int j=0;j<3;j++) {
                    ProducerRecord<String, String> record = new ProducerRecord<>(topic, "item" + j, "val" + i);
                    Future<RecordMetadata> send = producer.send(record);
                    RecordMetadata recordMetadata = send.get();

                    int partition = recordMetadata.partition();
                    long offset = recordMetadata.offset();
                    System.out.println("key:"+record.key()+" val:"+record.value()+" partitions:"+partition+" offset:"+offset);
                }
            }
        }


    }

    public void consumer() {
        String topic = "msb-items";

        //基础配置
        Properties p = new Properties();
        p.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"node02:9092,node03:9092,node04:9092");
        p.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //消费细节
        //1消费者必须从属于消费组的
        //2一个组可以有很多consumer
        //3一个分区只能被一个组中的一个consumer消费，但是可以被多个组中的不同consumer消费
        p.setProperty(ConsumerConfig.GROUP_ID_CONFIG,"ooxx01");
        //kafak is MQ and is storage
        /**
         * What to do when there is no initial offset in Kafka or if the current offset
         * does not exist any more on the server (e.g. because that data has been deleted):
         * <ul>
         *     <li>earliest: automatically reset the offset to the earliest offset
         *     <li>latest: automatically reset the offset to the latest offset</li>
         *     <li>none: throw exception to the consumer if no previous offset is found for the consumer's group</li>
         *     <li>anything else: throw exception to the consumer.</li>
         * </ul>";
         */
        p.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"latest");//第一次启动，没有offset
        //一个运行运行的consumer，那么自己会维护自己消费进度
        //一旦你自动提交，但是是异步的
        //1.还没到时间，挂了，没提交，重启一个consumer，参照offset的时候，会重复消费
        //2.一个批次的数据还没有写数据库成功，但是这个批次的offset异步提交了，挂了，重启一个consumer，参照offset的时候，会丢失数据
        p.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"false");//自动提交时异步提交,丢数据&重复消费
//        p.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,"");//默认5秒
//        p.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,"");//POLL 拉取数据，弹性，按需 拉取多少？


        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(p);

        //ConsumerRebalanceListener解释
        //刚开始有一个consumer，一会又多了一个，如何一会又多了一个
        //如果topic有3个分区，从第一个consumer消费3个分区，到均衡到1个消费1个，1个消费2个分区
        //到最后每个consumer分配一个分区去消费，会是一个动态负载均衡的过程

        //kafka的consumer会动态负载均衡
        consumer.subscribe(Arrays.asList(topic), new ConsumerRebalanceListener() {

            //我丢了哪些分区
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                System.out.println("---onPartitionsRevoked");
                Iterator<TopicPartition> iterator = partitions.iterator();
                while(iterator.hasNext()) {
                    System.out.println(iterator.next().partition());
                }
            }

            //我得到了哪些分区
            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                System.out.println("---onPartitionsAssigned");
                Iterator<TopicPartition> iterator = partitions.iterator();
                while(iterator.hasNext()) {
                    System.out.println(iterator.next().partition());
                }
            }
        });


        while(true) {
            /**
             * 常识：如果想多线程处理多分区
             * 每poll一次，用一个语义：一个job启动
             * 一次job用多线程并行处理分区，
             * 且，job应该被控制是串行的
             * 以上的知识点，其实如果你学习过大数据
             */



            //微批的感觉
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(0));//没有数据就阻塞，有数据就返回，拉回来的数据时0~n多条

            if(!records.isEmpty()) {
                //以下代码的优化很重要
                System.out.println("-------------"+records.count()+"----------------");


                Set<TopicPartition> partitions = records.partitions();//每次poll的时候是取多个分区的数据
                //且每个分区的数据是有序的

                /**
                 * 如果手动提交offset
                 * 1.按消息进度同步提交
                 * 2.按分区粒度同步提交
                 * 3.按当前poll的批次同步提交
                 *
                 * 思考：如果再多个线程下
                 * 1.以上1，3的方式不用多线程
                 * 2.以上2的方式最容易想到多线程方式处理
                 */

                for(TopicPartition partition : partitions) {
                    List<ConsumerRecord<String, String>> pRecords = records.records(partition);
                    //在一个微批里，按分区获取poll回来的数据
                    //线性按分区处理，还可以并行按分区处理用多线程的方式
                    Iterator<ConsumerRecord<String, String>> pIter = pRecords.iterator();
                    while(pIter.hasNext()) {
                        ConsumerRecord<String, String> next = pIter.next();
                        int par = next.partition();
                        long offset = next.offset();
                        String key = next.key();
                        String value = next.value();

                        System.out.println("key: "+key+" val: "+value+" partitions:"+par+" offset: "+offset);


                        TopicPartition sp = new TopicPartition(topic, par);
                        OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(offset);
                        HashMap<TopicPartition, OffsetAndMetadata> map = new HashMap<>();
                        map.put(sp,offsetAndMetadata);
                        consumer.commitSync(map);//这个是最安全的，每条记录级的更新，第一点
                        //单线程，多线程，都可以
                    }

                    long poff = pRecords.get(pRecords.size() - 1).offset();//获取分区内最后一个消息的offset
                    OffsetAndMetadata pom = new OffsetAndMetadata(poff);
                    HashMap<TopicPartition, OffsetAndMetadata> map = new HashMap<>();
                    map.put(partition,pom);
                    consumer.commitSync(map);//这个是第二种，分区粒度提交offset
                    /**
                     * 因为你都分区了
                     * 拿到了分区的数据集
                     * 期望的是先对数据整体加工
                     * 小问题会出现？你怎么知道最后一个的offset？！！！！！
                     * 感觉一定要有，kafka，很傻，你拿走了多少，我不关心，你告诉我你正确的最后一个的offset
                     *
                     */
                }

                consumer.commitSync();//这个就是按poll的批次提交offset，第3点的方式

/*
               //按记录数进行处理
                Iterator<ConsumerRecord<String, String>> iterator = records.iterator();
                while(iterator.hasNext()) {
                    //因为一个consumer可以消费多个分区，但是一个分区只能给一个组里的一个consumer消费
                    ConsumerRecord<String, String> record = iterator.next();
                    int partition = record.partition();
                    long offset = record.offset();
                    String key = record.key();
                    String value = record.value();


                    System.out.println("key: "+record.key()+" val: "+record.value()+" partitions:"+partition+" offset: "+offset);


                }*/
            }

        }




    }


    /**
     * 优化后的
     */
    public void consumer01() {
        String topic = "msb-items";

        //基础配置
        Properties p = new Properties();
        p.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"node02:9092,node03:9092,node04:9092");
        p.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //消费细节
        //1消费者必须从属于消费组的
        //2一个组可以有很多consumer
        //3一个分区只能被一个组中的一个consumer消费，但是可以被多个组中的不同consumer消费
        p.setProperty(ConsumerConfig.GROUP_ID_CONFIG,"ooxx01");
        //kafak is MQ and is storage
        /**
         * What to do when there is no initial offset in Kafka or if the current offset
         * does not exist any more on the server (e.g. because that data has been deleted):
         * <ul>
         *     <li>earliest: automatically reset the offset to the earliest offset
         *     <li>latest: automatically reset the offset to the latest offset</li>
         *     <li>none: throw exception to the consumer if no previous offset is found for the consumer's group</li>
         *     <li>anything else: throw exception to the consumer.</li>
         * </ul>";
         */
        p.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"latest");//第一次启动，没有offset
        //一个运行运行的consumer，那么自己会维护自己消费进度
        //一旦你自动提交，但是是异步的
        //1.还没到时间，挂了，没提交，重启一个consumer，参照offset的时候，会重复消费
        //2.一个批次的数据还没有写数据库成功，但是这个批次的offset异步提交了，挂了，重启一个consumer，参照offset的时候，会丢失数据
        p.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"false");//自动提交时异步提交,丢数据&重复消费
//        p.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,"");//默认5秒
//        p.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,"");//POLL 拉取数据，弹性，按需 拉取多少？


        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(p);

        //ConsumerRebalanceListener解释
        //刚开始有一个consumer，一会又多了一个，如何一会又多了一个
        //如果topic有3个分区，从第一个consumer消费3个分区，到均衡到1个消费1个，1个消费2个分区
        //到最后每个consumer分配一个分区去消费，会是一个动态负载均衡的过程

        //kafka的consumer会动态负载均衡
        consumer.subscribe(Arrays.asList(topic), new ConsumerRebalanceListener() {

            //我丢了哪些分区
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                System.out.println("---onPartitionsRevoked");
                Iterator<TopicPartition> iterator = partitions.iterator();
                while(iterator.hasNext()) {
                    System.out.println(iterator.next().partition());
                }
            }

            //我得到了哪些分区
            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                System.out.println("---onPartitionsAssigned");
                Iterator<TopicPartition> iterator = partitions.iterator();
                while(iterator.hasNext()) {
                    System.out.println(iterator.next().partition());
                }
            }
        });


        while(true) {




            //微批的感觉
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(0));//没有数据就阻塞，有数据就返回，拉回来的数据时0~n多条

            if(!records.isEmpty()) {
                //以下代码的优化很重要
                System.out.println("-------------"+records.count()+"----------------");


                consumerWay(records);

                consumer.commitSync();

            }

        }




    }


    /**
     * 第一种消费方式，按消息进度同步提交
     * @param records 拉取到的数据
     */
    public void consumerWay(ConsumerRecords<String, String> records) {
        Iterator<ConsumerRecord<String, String>> iterator = records.iterator();
        while(iterator.hasNext()) {
            ConsumerRecord<String, String> consumerRecord = iterator.next();
            long offset = consumerRecord.offset();
            int partition = consumerRecord.partition();
            String key = consumerRecord.key();
            String value = consumerRecord.value();
            System.out.println("key: "+key+" val: "+value+" partitions:"+partition+" offset: "+offset);
        }

    }


    /**
     * 根据时间戳查询
     * @param consumer
     *
     * 以下代码是你再未来开发的时候，向通过自己定义时间的方式，自定义消费数据位置
     *
     * 其实本质，核心知识就是seek方法
     *
     * 举一反三：
     * 1.通过时间换算成offset，再通过seek来自定义偏移
     * 2.如果你自己维护offset持久化
     */
    public void consumerByTime(KafkaConsumer consumer) {
        Map<TopicPartition,Long> tts = new HashMap();
        //通过consumer取回自己分配的分区
        Set<TopicPartition> assignment = consumer.assignment();
        while(assignment.size()==0) {
            consumer.poll(Duration.ofMillis(100));
            assignment = consumer.assignment();
        }

        //自己填充一个hashmap,为每个分区设置独赢的时间戳
        for(TopicPartition partition : assignment) {
            tts.put(partition,System.currentTimeMillis()-1*1000);
        }
        //通过consumer的api，取回timeindex的数据
        Map<TopicPartition,OffsetAndTimestamp> offtime = consumer.offsetsForTimes(tts);

        for (TopicPartition partition : assignment) {
            //通过取回的offset数据，通过consumer的seek方法，修正自己的消费偏移
            OffsetAndTimestamp offsetAndTimestamp = offtime.get(partition);//如果不是通过time换offset，如果是从MySQL读取，其本质是一样的

            long offset = offsetAndTimestamp.offset();

            consumer.seek(partition,offset);

        }

    }
}
