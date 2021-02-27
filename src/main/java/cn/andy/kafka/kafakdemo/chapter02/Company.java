package cn.andy.kafka.kafakdemo.chapter02;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Author: zhuwei
 * @Date:2019/6/25 9:38
 * @Description: 使用了lombok
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class Company {
    private String name;
    private String address;
}
