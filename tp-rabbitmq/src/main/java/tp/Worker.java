package tp;

import com.rabbitmq.client.*;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;

public class Worker {
    public static void main(String[] a) throws Exception {
        if (a.length != 3) {
            throw new IllegalArgumentException("Veuillez fournir 3 arguments : file, nom, dureeMs");
        }

        String file = a[0];
        String nom = a[1];
        long duree = Long.parseLong(a[2]);

        Connection c = Config.connect();
        Channel ch = c.createChannel();

        ch.basicQos(1);

        DeliverCallback traiter = (tag, d) -> {
            long deliveryTag = d.getEnvelope().getDeliveryTag();
            String id = d.getProperties().getMessageId();
            String corps = new String(d.getBody(), StandardCharsets.UTF_8);

            System.out.printf("%s DEBUT id=%s redelivered=%s corps=%s%n",
                    nom, id, d.getEnvelope().isRedeliver(), corps);

            try {
                Thread.sleep(duree);
                System.out.println(nom + " FIN id=" + id);

                ch.basicAck(deliveryTag, false);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                ch.basicNack(deliveryTag, false, true);
            }
        };

        ch.basicConsume(file, false, traiter, tag -> System.out.println("Consommateur annule: " + tag));
        System.out.println(nom + " attend sur " + file);

        new CountDownLatch(1).await();
    }
}