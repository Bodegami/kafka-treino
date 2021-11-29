package br.com.alura.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class EmailService {

    public static void main(String[] args) {

        /*
        Instancia do nosso consumidor recebendo as properties
        Nosso consumidor passa a escutar o topico indicado na singletonList
        O consumidor fica escutando por 100 ms e salva o resultado na variavel records
        Alem disso, o comum e nosso ficar escutando o topico por um tempo, por isso
        colocamos o poll dentro de um laco (while)
        */
        var consumer = new KafkaConsumer<String, String>(properties());
        consumer.subscribe(Collections.singletonList("ECOMMERCE_SEND_EMAIL"));

        while (true) {

            var records = consumer.poll(Duration.ofMillis(100));
            if (!records.isEmpty()) {
                System.out.println("Encontrei " + records.count() + " registros");

                for (var record : records) {
                    System.out.println("------------------------------------------------");
                    System.out.println("Send email");
                    System.out.println(record.key());
                    System.out.println(record.value());
                    System.out.println(record.partition());
                    System.out.println(record.offset());
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        //ignoring
                        e.printStackTrace();
                    }
                    System.out.println("Email sent");
                }
            }
        }
    }

    private static Properties properties() {

        //Properties do consumer, seguindo a mesma ideia das properties do producer
        var properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, EmailService.class.getSimpleName());
        return properties;
    }

}
