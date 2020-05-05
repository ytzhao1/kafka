package myKafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

/**
 * @author ：zyt
 * @date ：Created in 2020-04-04 10:48
 * @description： 消费者demo
 * @modified By：
 * @version: 1.0.0
 */
public class MyConsumer {
    public static void main(String[] args) {
        Properties props = new Properties();
        //broker的地址
        props.put("bootstrap.servers","localhost:9092");
        //所属Consumer Group的Id
        props.put("group.id","test");
        //自动提交offset
        props.put("enable.auto.commit","true");
        //自动提交offset的时间间隔
        props.put("auto.commit.interval.ms","10000");
        props.put("session.timeout.ms","30000");
        //key使用的Deserializer，参考第2章的相关小节
        props.put("key.deserializer","org.apache.kafka.common.serialization.IntegerDeserializer");
        //value 使用的Deserializer，参考第2章相关小节
        props.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<Integer,String> consumer = new KafkaConsumer<Integer, String>(props);

        //订阅topic1 两个topic
        consumer.subscribe(Arrays.asList("topic1"));

        try{
            while(true){
                //从服务端拉去信息，每次poll（）可以拉取多个信息
                ConsumerRecords<Integer,String> records = consumer.poll(100);
                //消费消息，这里仅仅是将消息的offset、key、value输出
                for(ConsumerRecord<Integer,String> record:records){
                    System.out.printf("offset = %d,key = $s,value = %s\n",
                            record.offset(),record.key(),record.value());

                }
            }
        }finally {
            consumer.close();
        }


    }
}