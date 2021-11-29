package br.com.alura.ecommerce;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class NewOrderMain {

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        /*
         Instancia do producer kafka passando os properties
         Value é a mensagem que desejamos enviar

         O producerRecord cria a mensagem a ser enviada ao producer,
         mas para isso precisamos passar o topico, a chave e o valor.O
         Aqui no caso a chave e o valor sao os mesmos

         O send executa a acao de enviar do procuder e ele é um método assincrono.
         Para esperar ele terminar, chamamos o metodo .get()
         Alem disso, o send aceita um "callback". Basta implementar a interface de callback
         que recebemos os metadados de sucesso e exception de falha.

         Usamos o slf4j como listener, dessa forma conseguimos saber quando a mensagem foi enviada
        */

        var producer = new KafkaProducer<String, String>(properties());
        var value = "152658,142523658,2548587";
        var record = new ProducerRecord<String, String>("ECOMMERCE_NEW_ORDER", value, value);
        Callback callback = (data, ex) -> {
            if (ex != null) {
                ex.printStackTrace();
                return;
            }

            System.out.println("sucesso enviando " + data.topic() + ":::partition " + data.partition() + "/ offset " + data.offset() + "/" + data.timestamp());
        };
        var email = "Thank you for your order! We are processing your order";
        var emailRecord = new ProducerRecord<String, String>("ECOMMERCE_SEND_EMAIL", email, email);
        producer.send(record, callback).get();
        producer.send(emailRecord, callback).get();



    }

    private static Properties properties() {

        // Properties necessários para criação do do producer kafka
        var properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return properties;
    }

}
