package com.bigdatacompany.eticaret.api;

import com.fasterxml.jackson.databind.util.JSONPObject;
import org.json.simple.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.sql.SQLOutput;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

@RestController
public class SearchController {

    @Autowired // Mevcut bir class'ı başka bir class içinde kullanmamızı sağlayarak, yeni bir class oluşturur.
    MessageProducer messageProducer;

    @GetMapping("/search")
    public void searchIndex(@RequestParam String term) {
        List<String> cities = Arrays.asList("Ankara", "İstanbul", "Adana", "Mersin", "Zonguldak", "Malatya", "Elazığ", "Hakkari",
                "İzmir", "Tekirdağ", "Trabzon");
        List<String> products = Arrays.asList("Bebek Bezi", "Cep Telefonu", "Klavye", "Mouse", "Bardak", "Cüzdan", "Laptop", "Paspas",
                "Ampul", "Harddisk", "Koltuk");
        Timestamp timestamp = new Timestamp(System.currentTimeMillis());

        while (true) {
            Random rnd = new Random();
            int i = rnd.nextInt(cities.size());
            int k = rnd.nextInt(products.size());

            //Timestamp'i de rastgele almak için:
            long offset = Timestamp.valueOf("2023-10-09 02:00:00").getTime();
            long end = Timestamp.valueOf("2023-10-09 23:59:00").getTime();
            long diff = end - offset + i;
            Timestamp rand = new Timestamp(offset + (long) (Math.random() * diff));

            // Verileri Kafka'ya JSON formatında göndermek için:
            JSONObject jsonObject = new JSONObject();
            jsonObject.put("search", products.get(k));
            jsonObject.put("current_ts", rand.toString());
            jsonObject.put("region", cities.get(i));
            jsonObject.put("userid", rnd.nextInt(3000 - 2000) + 2000);

            System.out.println(jsonObject.toJSONString());
            //Kafka Producer'a gönderilecek veri, jsonObject tipinde kullanıcının search bar'da arama yapmasıyla (Get isteği) oluşur.
            messageProducer.send(jsonObject.toJSONString());
        }
    }
        @GetMapping("/search/stream")
        public void searchIndexStream(@RequestParam String term)
        {
            List<String> cities = Arrays.asList("Ankara", "İstanbul", "Adana", "Mersin", "Zonguldak", "Malatya", "Elazığ", "Hakkari",
                    "İzmir", "Tekirdağ", "Trabzon");
            Timestamp timestamp=new Timestamp(System.currentTimeMillis());

                Random rnd = new Random();
                int i = rnd.nextInt(cities.size());

                // Verileri Kafka'ya JSON formatında göndermek için:
                JSONObject jsonObject = new JSONObject();
                jsonObject.put("search",term);
                jsonObject.put("current_ts", timestamp.toString());
                jsonObject.put("region", cities.get(i));
                jsonObject.put("userid", rnd.nextInt(3000-2000)+2000);

                //Kafka Producer'a gönderilecek veri, jsonObject tipinde kullanıcının search bar'da arama yapmasıyla (Get isteği) oluşur.
                messageProducer.send(jsonObject.toJSONString());
            }
    }
