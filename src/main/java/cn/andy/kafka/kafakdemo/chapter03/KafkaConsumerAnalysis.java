package cn.andy.kafka.kafakdemo.chapter03;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 *
 * @Author: zhuwei
 * @Date:2019/6/25 11:17
 * @Description:
 * kafka的消费基于拉模式
 * kafka默认提交的方式是自动提交可以通过参数enable.auto.commit来控制
 * 默认的自动提交并不是每消费一条消息就提交一次，而是定期提交，这个定期的提交周期就是客户端参数来auto.commit.interval.ms配置，默认值为5秒
 * 每隔5秒会把拉取到的每个分区的最大的消息位移进行提交，自动提交的逻辑是再poll方法的逻辑中进行完成的，再每次真正向服务拉取数据之前，
 * 都会检查是否可以进行位移提交，如果可以，那么就会提交上一次轮询的位移。
 *
 * 手动提交可以细分为同步提交和异步提交，对应于KafkaConsumer中的commitSync()和commitAsync()两种类型的方法
 * commitSync()方法会根据poll()方法拉取的最新的位移来进行提交
 * 如果想寻求更细粒度的、更精确的提交，那么就需要利用commiySync()的另一个含参的方法，
 *  public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) 该方法用来提交指定分区的位移。
 *
 *
 * 在Kafka中每当消费者查找不到所记录的消费位移时，就会根据消费者客户端参数auto.offset.reset的配置
 * 来决定从何处开始进行消费，这个参数的默认值为“latest”,表示从分区末尾开始消费消息
 * 除了查找不到消费位移，位移越界也会触发auto.offset.reset参数的执行
 *
 * KafkaConsumer中的seek()方法提供了让我们得以追前消费或回溯消费
 * public void seed(TopicPartition partition,long offset)
 * seek()方法只能重置消费者分配到的分区的消费数据，而分区的分配时在poll()
 * 方法的调用过程中实现的，也就是说，在执行seek()方法之前需要先执行一次poll()方法，等到
 * 分配到分区之后才可以重置消费位置
 *
 * 如果消费组内的消费者在启动的时候能够找到消费位移，除非发生位移越界，否则
 * auto.offset.reset参数并不会凑效，此时如果想指定从开头或末尾开始消费，就需要
 * seek()方法的帮助了
 *
 * 配合sesk方法，消费位移可以保存在任意的存储介质中，例如数据库、文件系统等。
 *
 * 再均衡是指分区的所属权从一个消费者转移到另一个消费者的行为，它为消费组具备高可用性和伸缩性提供保障，使我们可以既方便又安全地删除消费组内消费者或往消费组内
 * 添加消费者。不过再均衡发生期间，消费组的消费者是无法读取消息的。另外，当一个分区被重新分配给另一个消费者时，消费者当前的状态也会丢失。一般情况下，应尽量避免不必要
 * 的再均衡的发生。
 * 在subscribe()方法中提及再均衡器监听器ConsumerRebalanceListener用来设定发生再均衡动作前后的一些准备工作。例子参考test11()
 *
 *
 *
 * KafkaProducer是线程安全的，然而KafkaConsumer却是非线程安全的，通过KafkaConsumer中定义一个acquire()方法，用来检测当前是否只有一个线程再操作，
 * 若有其他线程正在操作则会抛出ConcurrentModifcationException异常
 * KafkaConsumer中的每个公用的方法再执行所要执行的动作之前都会调用这个acquire()方法，只有wakeup()方法是个例外
 * acquire与release方法成对出现
 *
 *
 *
 *
 *
 *
 *
 */
public class KafkaConsumerAnalysis {
//    public static final String brokerList = "192.168.80.100:9092";
    public static final String brokerList = "node01:9092,node02:9092,node03:9092";
    public static final String topic = "topic-demo";
    public static final String groupId = "andy3";
    public static final AtomicBoolean isRunning = new AtomicBoolean(true);

    public static Properties initConfig() {
        Properties props = new Properties();
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,brokerList);
        props.put(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG,"consumer.client.id.demo");

        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        return props;
    }


    public static void main(String[] args) {
        //test01();
        test02();
    }


    public static void test01() {
        Properties props = initConfig();
        //非线性安全
        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(props);
        consumer.subscribe(Arrays.asList(topic));

        //直接订阅某些主题的特定分区,例如订阅topic-demo主题中分区编号为0的分区
//        consumer.assign(Collection<TopicPartition> partitions);
        //  consumer.assign(Arrays.asList(new TopicPartition("topic-demo",0)));


        //查询指定主题的元数据信息
//        List<PartitionInfo> partitionInfos = consumer.partitionsFor(String topic);


        //取消订阅
//        consumer.unsubscribe();

        //如果将subscribe(Collection)或assign(Collection)中的集合参数设置为空集合，那么作用等同于unsubscribe()方法
        //集合订阅的方式subscribe(Collection)、正则表达式订阅的方式subscribe(Pattern)和指定分区的订阅方式assign(Collection)分
        //表代表了三种不同的订阅状态:AUTO_TOPICS、AUTO_PATTERN和USER_ASSIGNED（如果没有订阅，那么订阅状态为NONE）。然而这三种状态
        //是互斥的，在一个消费者种只能使用其中的一种，否则会报IllegalStateException异常


        try {
            while(isRunning.get()) {
                //一次拉取操作所获得的消息集
                ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(1000));
              /*  for(ConsumerRecord<String,String> record : records) {
                    System.out.println("topic="+record.topic()+",partition="+record.partition()+",offset="+record.offset());
                    System.out.println("key="+record.key()+",value="+record.value());
                }*/

                //按照分区维度来进行消费
                //获取消息集中所有分区
              /*  for(TopicPartition tp : records.partitions()) {
                    for(ConsumerRecord<String,String> record : records.records(tp)) {
                        System.out.println("topic="+record.topic()+",partition="+record.partition()+",offset="+record.offset());
                        System.out.println("key="+record.key()+",value="+record.value());
                    }
                }*/

                //按照主题维度来进行消费
               /* for(String t : Arrays.asList(topic)) {
                    for(ConsumerRecord<String,String> record : records.records(t)) {
                        System.out.println("topic="+record.topic()+",partition="+record.partition()+",offset="+record.offset());
                        System.out.println("key="+record.key()+",value="+record.value());
                    }
                }*/
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            consumer.close();
        }
    }



    //演示position=committed offset=lastConsumedOffset+1
    //当然position和commited offset并不会一直相同
    public static void test02()  {
        Properties props = initConfig();
        //非线性安全
        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(props);
        TopicPartition tp = new TopicPartition(topic,0);
        consumer.assign(Arrays.asList(tp));
        long lastConsumedOffset = -1;//当前消费到的位移



        try {
            while(isRunning.get()) {
                //一次拉取操作所获得的消息集
                ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(1000));

                if(records.isEmpty()) {
                    break;
                }

                List<ConsumerRecord<String, String>> partitionRecords = records.records(tp);
                lastConsumedOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
                consumer.commitSync();//同步提交消费位移
            }
            System.out.println("consumed offset is "+lastConsumedOffset);
            OffsetAndMetadata offsetAndMetadata = consumer.committed(tp);
            System.out.println("commited offset is "+offsetAndMetadata.offset());
            long position = consumer.position(tp);
            System.out.println("the offset of the next record is "+position);

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            consumer.close();
        }
    }


    //带参数的同步位移提交
    public static void test03()  {
        Properties props = initConfig();
        //非线性安全
        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(props);
        TopicPartition tp = new TopicPartition(topic,0);
        consumer.assign(Arrays.asList(tp));
        long lastConsumedOffset = -1;//当前消费到的位移



        try {
            while(isRunning.get()) {
                //一次拉取操作所获得的消息集
                ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> record : records) {
                    // so some logical producssing.
                    long offset = record.offset();

                    TopicPartition partition = new TopicPartition(record.topic(),record.partition());
                    //再实际应用中，很少会有这种每消费一条消息就提交一次消费位移的必要场景
                    //commitSync()本身是同步的，这样的写法会将性能拉到一个想当低的点
                    consumer.commitSync(Collections.singletonMap(partition,new OffsetAndMetadata(offset+1)));
                    //更多时候是按照分区的粒度划分提交位移的界限，

                }

            }


        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            consumer.close();
        }
    }


    //带分区的粒度的进行位移提交
    public static void test04()  {
        Properties props = initConfig();
        //非线性安全
        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(props);
        TopicPartition tp = new TopicPartition(topic,0);
        consumer.subscribe(Arrays.asList(topic));
        try {
            while(isRunning.get()) {
                //一次拉取操作所获得的消息集
                ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(1000));

                Set<TopicPartition> partitions = records.partitions();
                for (TopicPartition partition : partitions) {
                    List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);
                    for (ConsumerRecord<String, String> partitionRecord : partitionRecords) {
                        //do some logical processing
                    }
                    long lastConsumedOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
                    consumer.commitSync(Collections.singletonMap(partition,
                            new OffsetAndMetadata(lastConsumedOffset+1)));
                }

            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            consumer.close();
        }
    }


    //异步提交的方法演示
    public static void test05()  {
        Properties props = initConfig();
        //非线性安全
        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(props);
        TopicPartition tp = new TopicPartition(topic,0);
        consumer.subscribe(Arrays.asList(topic));
        try {
            while(isRunning.get()) {
                //一次拉取操作所获得的消息集
                ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> record : records) {
                    //do some logical processing
                }
                consumer.commitAsync(new OffsetCommitCallback() {
                    @Override
                    public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
                        if(exception == null) {
                            System.out.println(offsets);
                        } else {
                            System.out.println("fail to commit offsets "+offsets+exception);
                        }
                    }
                });

            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            consumer.close();
        }
    }


    //优雅的跳出消费的while循环
    public static void test06()  {
        Properties props = initConfig();
        //非线性安全
        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(props);
        TopicPartition tp = new TopicPartition(topic,0);
        consumer.subscribe(Arrays.asList(topic));
        try {
            while(isRunning.get()) {
                //一次拉取操作所获得的消息集
                ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(1000));
                //do some logical processing

                //暂定某些分区在拉取分区数据时返回给客户端
//                public void pause(Collection<TopicPartition> partitions)
                //恢复某些分区在拉取分区数据时返回给客户端
//                public void resume(Collection<TopicPartition> partitions)
                //返回被暂停的分区集合
//                Set<TopicPartition> paused = consumer.paused();

                //跳出消费逻辑的循环方式
                //1.通过设置isRunning.set(false);
                //2.通过consumer.wakeup() 这个方法是唯一可以安全的从其他线程调用的方法，记住consumer不是线程安全的，调用这个方法会抛出WakeupException异常
                //我们不需要管这个异常，它是一种跳出循环的方式
//                consumer.wakeup();
            }
        } catch (WakeupException e) {
            e.printStackTrace();
        } finally {
            //并不是无限制等待，内部设定了最长等待30秒
            consumer.close();
        }
    }


    //seek方法的使用
    public static void test07()  {
        Properties props = initConfig();
        //非线性安全
        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(props);
        TopicPartition tp = new TopicPartition(topic,0);
        consumer.subscribe(Arrays.asList(topic));
        try {
            //此处参数如果设置为0，则会发现seek方法并未起作用，因为
            //设置为0，seek方法会立即返回，poll内部还来不及分配分区，
            //也就是下面得不到任何分区信息，那应该怎么设置，参考test08的写法
            consumer.poll(Duration.ofMillis(10000));
            //do some logical processing

            //获取消费者所分配的分区信息
            Set<TopicPartition> assignment = consumer.assignment();
            for (TopicPartition partition : assignment) {
                //设置每个分区的消费位置为10
                consumer.seek(partition,10);
            }

            //拉取数据
            while(true) {
                ConsumerRecords<String, String> poll = consumer.poll(Duration.ofMillis(1000));
                //consume the record
            }
        } catch (WakeupException e) {
            e.printStackTrace();
        } finally {
            //并不是无限制等待，内部设定了最长等待30秒
            consumer.close();
        }
    }


    //seek方法的另一种示例
    public static void test08()  {
        Properties props = initConfig();
        //非线性安全
        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(props);
        TopicPartition tp = new TopicPartition(topic,0);
        consumer.subscribe(Arrays.asList(topic));
        Set<TopicPartition> assignment = new HashSet<>();
        while(assignment.size() == 0) {//如果不为0，则说明已经成功分配到分区了
            consumer.poll(Duration.ofMillis(100));
            assignment = consumer.assignment();
        }
        try {
            for (TopicPartition partition : assignment) {
                //设置每个分区的消费位置为10
                //如果对未分配到的分区执行seek方法，那么会抛出
                //IllegalStateException的异常，类似在调用subscribe方法之后直接调用
                //seek()方法
                consumer.seek(partition,10);
            }

            //拉取数据
            while(true) {
                ConsumerRecords<String, String> poll = consumer.poll(Duration.ofMillis(1000));
                //consume the record
            }

        } catch (WakeupException e) {
            e.printStackTrace();
        } finally {
            //并不是无限制等待，内部设定了最长等待30秒
            consumer.close();
        }
    }


    //使用seek()方法从分区末尾消费
    public static void test09()  {
        Properties props = initConfig();
        //非线性安全
        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(props);
        TopicPartition tp = new TopicPartition(topic,0);
        consumer.subscribe(Arrays.asList(topic));
        Set<TopicPartition> assignment = new HashSet<>();
        while(assignment.size() == 0) {//如果不为0，则说明已经成功分配到分区了
            consumer.poll(Duration.ofMillis(100));
            assignment = consumer.assignment();
        }
        try {

            //获取指定分区的末尾的消息位置
            Map<TopicPartition, Long> topicPartitionLongMap = consumer.endOffsets(assignment);

            //等待时间有客户端参数request.timeout.ms来设置，默认未30000.
            //于endOffsets对应的时beginningOffsets()方法，一个分区的起始位置时0，但并不代表每时每刻都为0
            //因为日志清理的动作会清理旧的数据，所以分区的起始位置会自然而然低增加
            //kafkah提供了seekToBeginning和seekToEnd的方法
            //kafka还提供了一个offsetsForTimes方法，通过timestamp来查询于此对应的分区位置 参考test10
//            consumer.endOffsets(Collection<TopicPartition> partitions, Duration timeout)
            for (TopicPartition partition : assignment) {
                consumer.seek(partition,topicPartitionLongMap.get(partition));
            }

            //拉取数据
            while(true) {
                ConsumerRecords<String, String> poll = consumer.poll(Duration.ofMillis(1000));
                //consume the record
            }

        } catch (WakeupException e) {
            e.printStackTrace();
        } finally {
            //并不是无限制等待，内部设定了最长等待30秒
            consumer.close();
        }
    }


    //根据时间查询消息
    public static void test010()  {
        Properties props = initConfig();
        //非线性安全
        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(props);
        TopicPartition tp = new TopicPartition(topic,0);
        consumer.subscribe(Arrays.asList(topic));
        try {

            Map<TopicPartition,Long> timestampToSearch = new HashMap<>();
            Set<TopicPartition> assignment = new HashSet<>();
            while(assignment.size() == 0) {//如果不为0，则说明已经成功分配到分区了
                consumer.poll(Duration.ofMillis(100));
                assignment = consumer.assignment();
            }

            for (TopicPartition partition : assignment) {
                timestampToSearch.put(partition,System.currentTimeMillis()-1*24*3600*1000);
            }

            Map<TopicPartition, OffsetAndTimestamp> topicPartitionOffsetAndTimestampMap = consumer.offsetsForTimes(timestampToSearch);
            for (TopicPartition partition : assignment) {
                OffsetAndTimestamp offsetAndTimestamp = topicPartitionOffsetAndTimestampMap.get(partition);
                if(offsetAndTimestamp != null) {
                    consumer.seek(tp,offsetAndTimestamp.offset());
                }
            }

            //拉取数据
            while(true) {
                ConsumerRecords<String, String> poll = consumer.poll(Duration.ofMillis(1000));
                //consume the record
            }

        } catch (WakeupException e) {
            e.printStackTrace();
        } finally {
            //并不是无限制等待，内部设定了最长等待30秒
            consumer.close();
        }
    }


    //再均衡监听器应用
    public static void test011()  {
        Properties props = initConfig();
        Map<TopicPartition,OffsetAndMetadata> currentOffsets = new HashMap<>();
        //非线性安全
        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(props);
        TopicPartition tp = new TopicPartition(topic,0);
        consumer.subscribe(Arrays.asList(topic), new ConsumerRebalanceListener() {
            /**
             * 这个方法会再均衡开始之前和消费者停止读取消息之后被调用。可以通过这个方法回调方法来处理消费位移的提交，以此
             * 来避免一些不必要的重复消费现象的发生。
             * @param partitions 再均衡前所分配到的分区
             */
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                consumer.commitSync(currentOffsets);
                currentOffsets.clear();
            }

            /**
             * 这个方法会在重新分配分区之后和消费者开始读取消息之前被调用
             * @param partitions 再均衡后所分配到的分区
             */
            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                //do nothing.
            }
        });
        try {

           while(isRunning.get()) {
               ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
               for (ConsumerRecord<String, String> record : records) {
                   //process the record
                   currentOffsets.put(new TopicPartition(record.topic(),record.partition()),
                           new OffsetAndMetadata(record.offset()+1));
               }
               consumer.commitAsync(currentOffsets,null);

           }

        } catch (WakeupException e) {
            e.printStackTrace();
        } finally {
            //并不是无限制等待，内部设定了最长等待30秒
            consumer.close();
        }
    }

}
