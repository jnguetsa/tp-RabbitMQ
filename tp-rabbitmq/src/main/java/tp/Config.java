package tp;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public final class Config {

    /**
     * Établit et retourne une nouvelle connexion AMQP vers le broker RabbitMQ.
     * Utilise les paramètres d'authentification et l'hôte configurés pour le TP.
     *
     * @return
     * @throws Exception
     */
    public static Connection connect() throws Exception {
        ConnectionFactory factory = new ConnectionFactory();

        factory.setHost("localhost");
        factory.setPort(5672);
        factory.setUsername("tp");
        factory.setPassword("tp2026");
        factory.setVirtualHost("/");

        return factory.newConnection();
    }
}