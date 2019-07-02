package cn.andy.kafka.kafakdemo.chapter03;

import cn.andy.kafka.kafakdemo.chapter02.Company;
import io.protostuff.ProtostuffIOUtil;
import io.protostuff.Schema;
import io.protostuff.runtime.RuntimeSchema;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Map;

/**
 * @Author: zhuwei
 * @Date:2019/6/25 19:28
 * @Description: 使用Protostuff进行返序列
 */
public class CompanyDeserializer2 implements Deserializer<Company> {


    @Override
    public void configure(Map<String, ?> map, boolean b) {

    }

    @Override
    public Company deserialize(String topic, byte[] data) {
        if(data == null) {
            return null;
        }

        Schema schema = RuntimeSchema.getSchema(Company.class);
        Company ans = new Company();
        ProtostuffIOUtil.mergeFrom(data,ans,schema);
        return ans;
    }

    @Override
    public void close() {

    }
}
