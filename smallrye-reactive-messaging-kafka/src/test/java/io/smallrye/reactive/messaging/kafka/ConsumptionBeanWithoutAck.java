package io.smallrye.reactive.messaging.kafka;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Acknowledgment;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;

@ApplicationScoped
public class ConsumptionBeanWithoutAck {

    private final List<Integer> list = Collections.synchronizedList(new ArrayList<>());
    private final List<KafkaRecord<String, Integer>> kafka = Collections.synchronizedList(new ArrayList<>());

    @Incoming("data")
    @Outgoing("sink")
    @Acknowledgment(Acknowledgment.Strategy.MANUAL)
    public Message<Integer> process(KafkaRecord<String, Integer> input) {
        kafka.add(input);
        return Message.of(input.getPayload() + 1);
    }

    @Incoming("sink")
    public void sink(int val) {
        list.add(val);
    }

    public List<Integer> getResults() {
        return list;
    }

    public List<KafkaRecord<String, Integer>> getKafkaMessages() {
        return kafka;
    }
}
