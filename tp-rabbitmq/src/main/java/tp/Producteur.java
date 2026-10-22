package tp;

import com.rabbitmq.client.*;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicBoolean;

public class Producteur {
    public static void main(String[] a) throws Exception {
        if (a.length < 3) throw new IllegalArgumentException(
                "Arguments requis : exchange cle_routage eventId [type] [montant]");

        String exchange = a[0];
        String routingKey = a[1];
        String eventId = a[2];
        String type = a.length > 3 ? a[3] : "CommandeValidee";
        long montant = a.length > 4 ? Long.parseLong(a[4]) : 15000;

        // Construction du corps JSON respectant le contrat du TP
        String jsonPayload = String.format(
                "{\"eventId\": \"%s\", \"type\": \"%s\", \"occurredAt\": \"2026-10-02T08:00:00Z\", \"schemaVersion\": 1, \"data\": {\"commandeId\": 42, \"montant\": %d, \"devise\": \"XAF\"}}",
                eventId, type, montant
        );

        try (Connection c = Config.connect(); Channel ch = c.createChannel()) {
            AtomicBoolean retourne = new AtomicBoolean(false);

            ch.addReturnListener((code, texte, ex, cle, props, corps) -> {
                retourne.set(true);
                System.err.println("RETOUR " + code + " " + texte);
            });

            ch.confirmSelect();

            // La propriété AMQP messageId prend la même valeur que eventId
            AMQP.BasicProperties props = new AMQP.BasicProperties.Builder()
                    .contentType("application/json")
                    .deliveryMode(2) // Persistant
                    .messageId(eventId)
                    .build();

            ch.basicPublish(exchange, routingKey, true, props,
                    jsonPayload.getBytes(StandardCharsets.UTF_8));

            ch.waitForConfirmsOrDie(5000);

            if (retourne.get()) throw new IllegalStateException("Non routable");
            System.out.println("Publication confirmee du message JSON : " + eventId);
            System.out.println("Payload : " + jsonPayload);
        }
    }
}