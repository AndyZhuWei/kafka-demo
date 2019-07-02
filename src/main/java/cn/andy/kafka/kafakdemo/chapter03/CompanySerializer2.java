package cn.andy.kafka.kafakdemo.chapter03;

import cn.andy.kafka.kafakdemo.chapter02.Company;
import io.protostuff.LinkedBuffer;
import io.protostuff.Pipe;
import io.protostuff.ProtostuffIOUtil;
import io.protostuff.Schema;
import io.protostuff.runtime.RuntimeSchema;
import org.apache.kafka.common.serialization.Serializer;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Map;

/**
 * @Author: zhuwei
 * @Date:2019/6/25 9:41
 * @Description: 自定义序列化器 使用protostuff序列化工具
 */
public class CompanySerializer2 implements Serializer<Company> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public byte[] serialize(String topic, Company data) {
        if (data == null) {
            return null;
        }
        Schema schema = (Schema) RuntimeSchema.getSchema(data.getClass());
        LinkedBuffer buffer = LinkedBuffer.allocate(LinkedBuffer.DEFAULT_BUFFER_SIZE);
        byte[] protostuff = null;
        try {
            protostuff = ProtostuffIOUtil.toByteArray(data,schema,buffer);
        } catch (Exception e) {
            throw new IllegalStateException(e.getMessage(),e);
        } finally {
            buffer.clear();
        }

        return protostuff;
    }

    @Override
    public void close() {

    }
}
