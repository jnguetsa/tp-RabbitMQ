package tp;

import com.rabbitmq.client.*;
import java.util.Map;

public class Topologie {
    public static void main(String[] args) throws Exception {
        try (Connection c = Config.connect(); Channel ch = c.createChannel()) {
            // Exchanges de base
            ch.exchangeDeclare("tp.commandes", "direct", true);
            ch.exchangeDeclare("tp.evenements", "fanout", true);
            ch.exchangeDeclare("tp.journaux", "topic", true);

            ch.exchangeDeclare("tp.echecs", "direct", true);

            for (String q : new String[]{"tp.preparation", "tp.notifications",
                    "tp.statistiques", "tp.logs.commandes", "tp.logs.erreurs",
                    "tp.preparation.dlq"}) {
                ch.queueDeclare(q, true, false, false,
                        Map.of("x-queue-type", "classic"));
            }

            ch.queueDeclare("tp.preparation.robuste", true, false, false,
                    Map.of(
                            "x-queue-type", "classic",
                            "x-dead-letter-exchange", "tp.echecs",
                            "x-dead-letter-routing-key", "preparation.echec"
                    ));

            ch.queueBind("tp.preparation", "tp.commandes", "preparation");
            ch.queueBind("tp.notifications", "tp.evenements", "");
            ch.queueBind("tp.statistiques", "tp.evenements", "");
            ch.queueBind("tp.logs.commandes", "tp.journaux", "commande.*");
            ch.queueBind("tp.logs.erreurs", "tp.journaux", "#.erreur");

            ch.queueBind("tp.preparation.dlq", "tp.echecs", "preparation.echec");
            ch.queueBind("tp.preparation.robuste", "tp.commandes", "robuste");

            System.out.println("Topologie complète (avec DLQ et file robuste) déployée avec succès.");
        }
    }
}