package cn.andy.kafka.kafakdemo.serializer;

/**
 * @Author zhuwei
 * @Date 2020/7/1 11:16
 * @Description: 定义数据对象格式
 */
public class User {

    private String firstName;
    private String lastName;
    private int age;
    private String address;

    public User(String firstName, String lastName, int age, String address) {
        this.firstName = firstName;
        this.lastName = lastName;
        this.age = age;
        this.address = address;
    }

    @Override
    public String toString() {
        return "User{" +
                "firstName='" + firstName + '\'' +
                ", lastName='" + lastName + '\'' +
                ", age=" + age +
                ", address='" + address + '\'' +
                '}';
    }
}
