package cn.andy.kafka.kafakdemo.serializer;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * @Author zhuwei
 * @Date 2020/7/1 11:19
 * @Description: User序列化器
 */
public class UserSerializer implements Serializer {

    private Logger logger = LoggerFactory.getLogger(UserSerializer.class);

    private ObjectMapper objectMapper;

    @Override
    public void configure(Map map, boolean b) {
        objectMapper = new ObjectMapper();
    }

    @Override
    public byte[] serialize(String topic, Object data) {
        byte[] ret = null;
        try {
            ret = objectMapper.writeValueAsString(data).getBytes("utf-8");
        } catch (Exception e) {
            logger.warn("failed to serialize the object:{}",data,e);
        }
        return ret;
    }

    @Override
    public void close() {

    }
}
