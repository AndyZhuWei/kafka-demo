package cn.andy.kafka.kafakdemo.deserializer;

import cn.andy.kafka.kafakdemo.serializer.User;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

/**
 * @Author zhuwei
 * @Date 2020/7/5 22:20
 * @Description: 解序列化器
 */
public class UserDeserializer implements Deserializer {

    private ObjectMapper objectMapper;

    @Override
    public void configure(Map map, boolean b) {
        objectMapper = new ObjectMapper();
    }

    @Override
    public Object deserialize(String topic, byte[] data) {
        User user = null;
        try {
            user = objectMapper.readValue(data,User.class);
        } finally {
            return user;
        }
    }

    @Override
    public void close() {

    }
}
