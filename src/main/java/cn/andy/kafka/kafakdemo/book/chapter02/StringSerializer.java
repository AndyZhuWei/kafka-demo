package cn.andy.kafka.kafakdemo.book.chapter02;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import java.io.UnsupportedEncodingException;
import java.util.Map;

/**
 * @Description StringSerializer的代码实现
 * @Author zhuwei
 * @Date 2021/1/26 15:20
 */
public class StringSerializer implements Serializer<String> {

    private String encoding = "UTF-8";

    /**
     * 配置当前类
     * @param configs
     * @param isKey
     *
     * 这个方法是再创建KafkaProducer实例的时候调用的。主要用来确定编码类型。
     * 其中key.serializer.encoding、value.serializer.encoding和serializer.encoding看作用户自定义的参数
     */
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        String propertyName = isKey ? "key.serializer.encoding" : "value.serializer.encoding";
        Object encodingValue = configs.get(propertyName);
        if(encodingValue == null) {
            encodingValue = configs.get("serializer.encoding");
        }
        if(encodingValue != null && encodingValue instanceof String) {
            encoding = (String)encodingValue;
        }

    }

    /**
     * 执行序列化操作
     * @param topic
     * @param data
     * @return
     */
    @Override
    public byte[] serialize(String topic, String data) {
        try {
            if (data == null) {
                return null;
            } else {
                return data.getBytes(encoding);
            }
        } catch (UnsupportedEncodingException e) {
            throw new SerializationException("Error when serializing "+
                    "String to byte[] due to unsupported encoding "+encoding);
        }
    }

    /**
     * 用来关闭序列化器，一般情况下close()是一个空方法，如果实现了此方法，
     * 则必须确保此方法的幂等性，因为这个方法很可能会被KafkaProduer调用多次
     */
    @Override
    public void close() {
        //nothing to do
    }
}
