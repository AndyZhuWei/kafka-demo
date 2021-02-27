##常用命令
0. 启动kafka
./bin/kafka-server-start.sh -daemon config/server1.properties 
启动 zookeeper
./bin/zkServer.sh start conf/zoo1.cfg

1 创建topic
./bin/kafka-topics.sh --zookeeper zk1:2181,zk2:2182,zk3:2183 --create --topic test-topic --partitions 3 --replication-factor 3
2 查询topic
./bin/kafka-topics.sh --zookeeper zk1:2181,zk2:2182,zk3:2183 -list
3 topic详情
./bin/kafka-topics.sh --zookeeper zk1:2181,zk2:2182,zk3:2183 -describe --topic test-topic
4 删除topic
./bin/kafka-topics.sh --zookeeper zk1:2181,zk2:2182,zk3:2183 -delete --topic test-topic
5 脚本producer
./bin/kafka-console-producer.sh --broker-list kafka1:9092,kafka2:9093,kafka3:9094 --topic test-topic
6 脚本consumer
./bin/kafka-console-consumer.sh --bootstrap-server kafka1:9092,kafka2:9093,kafka3:9094 --topic test-topic --from-beginning
./bin/kafka-console-consumer.sh --zookeeper zk1:2181,zk2:2182,zk3:2183 --topic test-topic --from-beginning
7 使用kafka提供的辅助工具查询每个分区的消息数目
./bin/kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list localhost:9092 --topic test-topic

##无消息丢失配置
*producer端参数*
**1. block.on.buffer.full=true**
这个参数在Kafka 0.9.0.0版本已经被标记为”deprecated“，并使用max.block.ms参数替代
设置为true,使得内存缓冲区被填满时producer处于阻塞状态并停止接收新的消息而不是抛出异常。否则到导致producer生产速度过快会耗尽缓冲区。
新版本可以不用理会这个参数，转而设置max.block.ms即可
**acks=all**
设置all表明所有follower都响应了发送消息才能认为提交成功，这是producer端最强程度的持久化保证。
**retries=Integer.MAX_VALUE**
开启无限重试，producer只会重试那些可恢复的异常情况，所以放心设置要给比较大的值通常能很好地保证消息不丢失
**max.in.flight.requests.per.connection=1**
设置为1 防止topic同分区下的消息乱序问题。这个参数的实际效果其实限制了producer在单个broker连接上能够发送的未响应请求的数量。因此，如果设置成1，
则producer在某个broker发送响应之前将无法再给该broker发送PRODUE请求
**使用带回调机制的send发送消息，即KafkaProducer.send(record,callback)**
不要使用KafkaProducer中单参数的send方法，因为该send调用仅仅是把消息发出而不会理会消息发送的结果。如果消息发送失败，该方法不会得到任何通知，
故可能造成数据的丢失。实际环境中一定要使用带回调机制的send版本，即KafkaProducer.send(record,callback)
**Callback逻辑中显式地立即关闭producer，使用close(0)**
在Callback的失败处理逻辑中显式调用close(0)，这样做的目的是为了处理消息的乱序问题。若不使用close(0),默认情况下producer会被允许将未完成的消息发送出去，
这样就有可能造成消息乱序

*broker端配置*
**unclean.leader.election.enable=false**
关闭unclean leader选举，即不允许非ISR中的副本被选择为leader，从而避免broker端因日志水位截断而造成的消息丢失
**replication.factory>=3**
设置成3主要是参考了hadoop及业界通用的三备份原则，其实这里想强调的是一定要使用多个副本来保存分区的消息
**min.insync.replicas>1**
用于控制某条消息至少被写入到ISR中的多少个副本才算成功，设置成大于1是为了提升producer端发送语义的持久性。记住只有在producer端acks被设置成all或-1时，这个参数才有意义。
在实际使用时，不要使用默认值
**replication.factor>min.insync.replicas**
若两者相等，那么只要有一个副本挂掉，分区就无法正常工作，虽然有很高的持久性但可用性被极大地i降低了。推荐配置成replication.factor=min.insyn.replicas+1


enable.auto.commit=false



##消息
Kafka本质上是使用Java NIO的ByteBuffer来保存消息，同时依赖文件系统提供的页缓存机制，而非依赖Java的堆缓存

消息层次：分为消息集合（message set）和消息
消息集合：包含若干个日志项，每个日志项封装了实际的消息和一组元数据信息

kafka日志文件就是由一系列消息集合日志项构成的,kafka总是在消息集合上进行写入操作

V2之前更多使用的是日志项(log entry)
每个消息集合中的日志项由一条”浅层“消息和日志项头部组成

浅层消息（shallow message）：如果没有启用消息压缩，那么这条浅层消息就是消息本身
       否则，kafka会将多条消息压缩到一起统一封装进这条浅层消息的value字段。此时该浅层消息
       被称为包装消息（或外部消息，即wrapper消息），而value字段中包含的消息则被称为内部消息，即inner消息。
       V0、V1版本中的日志项只能包含一条浅层消息
日志项头部（log entry header）:头部由8字节的位移(offset)字段加上4字节的长度（size）字段构成。
    如果是未压缩消息，该offset就是消息的offset。否则该字段表示wrapper消息中最后一条inner消息的offset
   
VO、V1版本消息格式的缺点
1.空间利用率不高
2.只保存最新消息位移
3.冗余的消息级CRC校验
4.未保存消息长度

V2版本则使用消息批次(record batch)
V2版本依然分为消息和消息集合两个维度，只不过消息集合的提法被消息批次所取代。
V2版本中，它有一个专门的术语：RecordBatch

可变长度相比V0 V1版本中无差别地使用4字节来保存要节省3个字节
V2版本的消息格式变化之大不仅仅引入了可变长度，同时它还新增、删除及重构了一些消息字段

增加消息总长度字段：在消息格式的头部增加该字段
保存时间戳增量（timestamp delta）:不再需要使用8字节保存时间戳信息，而是使用一个可变长度保存于batch起始时间戳的差值。
保存位移增量(offset delta):保存消息位移于外层batch起始位移的差值，而不再固定保存8字节的位移值
增加消息头部(message headers)
去除消息级CRC校验：V2版本不再为每条消息计算CRC32值，而是对整个消息batch进行CRC校验
弃用attribute字段：以前保存在这个里面的信息现在统一保存在外层的batch格式字段中，保留了这个字段


##集群管理











