package inbound;

import io.smallrye.reactive.messaging.kafka.IncomingKafkaRecord;
import org.eclipse.microprofile.reactive.messaging.Incoming;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

@ApplicationScoped
public class KafkaRebalancedConsumer {

    @Inject
    KafkaRebalancedConsumerRebalanceListener rebalanceListener;

    @Incoming("rebalanced-example")
    public CompletionStage<Void> consume(IncomingKafkaRecord<Integer, String> message) {
        if (rebalanceListener.isConsumerReady(message)) {
            // process the message
        }
        // we don't need to commit offsets so no need to ack the message
        return CompletableFuture.completedFuture(null);
    }

}
