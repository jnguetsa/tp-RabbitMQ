package tp;

import com.rabbitmq.client.*;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;

public class WorkerRobuste {
    public static void main(String[] a) throws Exception {
        String file = "tp.preparation.robuste";
        String nom = "WorkerRobuste";

        Connection c = Config.connect();
        Channel ch = c.createChannel();
        ch.basicQos(1);

        DeliverCallback traiter = (tag, d) -> {
            long deliveryTag = d.getEnvelope().getDeliveryTag();
            String id = d.getProperties().getMessageId();
            String corps = new String(d.getBody(), StandardCharsets.UTF_8);

            System.out.printf("%s DEBUT id=%s corps=%s%n", nom, id, corps);

            try {
                if (!corps.contains("\"eventId\"") || corps.contains("invalide")) {
                    throw new IllegalArgumentException("Format JSON invalide ou corrompu !");
                }

                Thread.sleep(1000);
                System.out.println(nom + " FIN id=" + id);
                ch.basicAck(deliveryTag, false);

            } catch (Exception e) {
                System.err.println(nom + " ERREUR : " + e.getMessage() + " -> Rejet définitif vers la DLQ.");
                ch.basicNack(deliveryTag, false, false);
            }
        };

        ch.basicConsume(file, false, traiter, tag -> System.out.println("Consommateur annulé : " + tag));
        System.out.println(nom + " attend les commandes robustes sur " + file);
        new CountDownLatch(1).await();
    }
}