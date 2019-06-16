package com.itheima.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Arrays;
import java.util.Properties;

public class DataFromConsumer2Producer {
    public static void main(String[] args) {
        //kafkaConsumer
        //1、配置信息
        Properties props = new Properties();
        props.put("bootstrap.servers", "node1:9092,node2:9092,node3:9092");
        props.put("group.id", "test");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        //2、创建kafka消费者对象
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);

        //3、消费具体topic中的数据
        consumer.subscribe(Arrays.asList("itcast"));

        String msg = "";
        while (true) {

            //4、遍历获取来自于具体的topic数据
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                msg = record.value();
                sendMsg(msg);

                System.out.println(msg);
            }
        }

    }

    static Properties props = null;

    //生产者对象的配置-只需要加载一次
    static {
        //1、准备配置项-KafkaProducer创建的时候，必须的一些配置项
        props = new Properties();
        props.put("bootstrap.servers", "node1:9092,node2:9092,node3:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    }


    /**
     * 生产数据
     * @param msg
     */
    public static void sendMsg(String msg) {
        //2、创建生产者对象 -KafkaProducer
        Producer<String, String> producer = new KafkaProducer<String, String>(props);
        //3、发送数据到具体的topic
        producer.send(new ProducerRecord<String, String>("TOPIC_Heima", "", msg));
        //4、释放资源
        producer.close();
    }
}
