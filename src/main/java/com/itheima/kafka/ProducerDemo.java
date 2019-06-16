package com.itheima.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class ProducerDemo {
    public static void main(String[] args) {
        //1、准备配置项-KafkaProducer创建的时候，必须的一些配置项
        Properties props = new Properties();
        props.put("bootstrap.servers", "node1:9092,node2:9092,node3:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        //2、创建生产者对象 -KafkaProducer
        Producer<String, String> producer = new KafkaProducer<String, String>(props);
        for(int i = 0; i < 100; i++){

            //3、发送数据到具体的topic
            producer.send(new ProducerRecord<String, String>("itcast", "", Integer.toString(i)+"hadoop"));


            //1、数据分发策略：没有指定具体的分区，也没有key---->走  round-robin 轮询策略进行分发数据
            producer.send(new ProducerRecord<String, String>("itcast","hello"));

            //2、有key的这种形式，数据分发策略为：基于key的hash
            producer.send(new ProducerRecord<String, String>("itcast","key","value"));

            //3、数据分发策略-指定了具体的分区，就将数据分发到指定的分区里面去了---不建议使用
            producer.send(new ProducerRecord<String, String>("itcast",2,"key","value"));
        }

        //4、释放资源
        producer.close();
    }
}
