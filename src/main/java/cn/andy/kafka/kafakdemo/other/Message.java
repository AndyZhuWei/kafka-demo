package cn.andy.kafka.kafakdemo.other;

import lombok.Data;

import java.util.Date;

/**
 * @Author: zhuwei
 * @Date:2019/8/16 15:27
 * @Description:
 */
@Data
public class Message {
    private Long id;

    private String msg;

    private Date sendTime;
}
