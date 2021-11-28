package br.com.alura.ecommerce;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class NewOrderMain {

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        //Instancia do producer kafka passando os properties
        var producer = new KafkaProducer<String, String>(properties());

        //mensagem que desejamos enviar
        var value = "Deu certo, a primeira aula, de Kafka";

       /* O producerRecord cria a mensagem a ser enviada ao producer,
       mas para isso precisamos passar o topico, a chave e o valor.O
       Aqui no caso a chave e o valor sao os mesmos*/
        var record = new ProducerRecord<String, String>("ECOMMERCE_NEW_ORDER", value, value);

        /*
         O send executa a acao de enviar do procuder e ele é um método assincrono.
         Para esperar ele terminar, chamamos o metodo .get()
         Alem disso, o send aceita um "callback". Basta implementar a interface de callback
         que recebemos os metadados de sucesso e exception de falha.
        */
        producer.send(record,(data, ex) -> {
            if (ex != null) {
                ex.printStackTrace();
                return;
            }
            //Conceito de listener, dessa forma conseguimos saber quando a mensagem foi enviada
            System.out.println("sucesso enviando " + data.topic() + ":::partition " + data.partition() + "/ offset " + data.offset() + "/" + data.timestamp());
        }).get();



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
