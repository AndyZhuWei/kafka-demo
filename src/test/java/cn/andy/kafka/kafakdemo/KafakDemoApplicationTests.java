package cn.andy.kafka.kafakdemo;

import cn.andy.kafka.kafakdemo.other.KafkaSender;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest
public class KafakDemoApplicationTests {

    @Autowired
    private KafkaSender kafkaSender;


    @Test
    public void send() {
        for(int i=0;i<3;i++) {
            kafkaSender.send();
        }
        System.out.println("已发送");

    }

    @Test
    public void contextLoads() {
    }




}
