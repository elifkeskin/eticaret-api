package com.bigdatacompany.eticaret.api;

import jakarta.annotation.PostConstruct;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.stereotype.Component;

import java.util.Properties;

@Component
//Spring bunu bir servis olarak görür, başka yazdığımız yerde servis olarak kullanabiliriz.

public class MessageProducer {
    Producer producer;
    @PostConstruct
    //Bu class çalıştırıldığında bir kereliğine bu class'ı çalıştırır.
    // Çünkü, Kafka'ya bir kere bağlanacağız.
    public void init()
    {
        Properties config=new Properties();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "206.189.117.218:9092");
        //Kafka'da veriyi gönderirken eğer "Key" değeri verirsek, her "Key"i bir partition'da saklar.(Partition-->Topic'in kaça bölüneceği)
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, new StringSerializer().getClass().getName());
        // Value, bizim datamızı barındırır.
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, new StringSerializer().getClass().getName());

        // Key ve Value değerleri String olduğu için, String olarak belrttik.
        producer=new KafkaProducer<String,String>(config);
    }

    public void send(String term){
        // ProducerRecord--> Send() deyip göndereceğimiz class'tır.
        // Term--> Kullanıcının aradığı kelime, İlk parametre-->topic,ikinci parametre-->data
        ProducerRecord<String,String> rec=new ProducerRecord<String, String>("search-analysis-stream", term);
        producer.send(rec);
    }

    public void close()
    {
        // Bu metot, producer'ı kapatmamızı sağlar.
        producer.close();
    }
}
