/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.examples;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class Producer extends Thread {
    private final KafkaProducer<Integer, String> producer;
    private final String topic;
    //定义消息的发送方式：异步发送还是同步发送
    private final Boolean isAsync;

    public Producer(String topic, Boolean isAsync) {
        Properties props = new Properties();
        //Kafka服务端的主机名和端口号
        props.put("bootstrap.servers", "localhost:9092");
        //客户端的id
        props.put("client.id", "DemoProducer");
        //key的序列化方法
        props.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
        //value的序列化方法
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<>(props);
        this.topic = topic;
        this.isAsync = isAsync;
    }

    public void run() {
        //消息的key
        int messageNo = 1;
        while (true) {
            //消息的value
            String messageStr = "Message_" + messageNo;
            long startTime = System.currentTimeMillis();
            if (isAsync) { // Send asynchronously 异步发送
                //第一个参数是ProducerRecord对象，封装了目标Topic、消息的key，消息的value
                //第二个参数是一个Callback对象，当生产者接收到Kafka发来的ACK确认消息的时候，
                //会调用此CallBack对象的onCompletion()方法，实现回调
                producer.send(new ProducerRecord<>(topic,
                    messageNo,
                    messageStr), new DemoCallBack(startTime, messageNo, messageStr));
            } else { // Send synchronously 同步发送
                try {
                    //send()返回的是一个Future<RecordMetadata>这里通过Future.get()方法，阻塞当前线程
                    //等待Kafka服务端的ACK响应
                    producer.send(new ProducerRecord<>(topic,
                        messageNo,
                        messageStr)).get();
                    System.out.println("Sent message: (" + messageNo + ", " + messageStr + ")");
                } catch (InterruptedException | ExecutionException e) {
                    e.printStackTrace();
                }
            }
            //递增消息的Key
            ++messageNo;
        }
    }
}

//回调对象
class DemoCallBack implements Callback {
    //开始发送消息的时间戳
    private final long startTime;
    //消息的Key
    private final int key;
    //消息的value
    private final String message;

    public DemoCallBack(long startTime, int key, String message) {
        this.startTime = startTime;
        this.key = key;
        this.message = message;
    }

    /**
     * 生产者成功发送消息，收到Kafka服务端发来的ACK确认消息后，会调用此回调函数
     *
     * @param metadata  生产者发送的消息的元数据，如果发送过程中出现异常，此参数为null
     * @param exception 发送过程中出现的异常，如果发送成功，则此参数为null
     */
    public void onCompletion(RecordMetadata metadata, Exception exception) {
        long elapsedTime = System.currentTimeMillis() - startTime;
        if (metadata != null) {
            //RecordMetadata中包含了分区信息、offset信息等
            System.out.println(
                "message(" + key + ", " + message + ") sent to partition(" + metadata.partition() +
                    "), " +
                    "offset(" + metadata.offset() + ") in " + elapsedTime + " ms");
        } else {
            exception.printStackTrace();
        }
    }
}
