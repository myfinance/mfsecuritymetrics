package de.hf.myfinance.securitymetrics.events.out;

import de.hf.myfinance.event.Event;
import de.hf.myfinance.restmodel.SecurityMetrics;

import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;

import static de.hf.myfinance.event.Event.Type.CREATE;

@Component
public class SecurityMetricsApprovedEventHandler {

    private final StreamBridge streamBridge;

    public SecurityMetricsApprovedEventHandler(StreamBridge streamBridge){
        this.streamBridge = streamBridge;
    }

    public void sendInstrumentApprovedEvent(SecurityMetrics securityMetrics){
        sendMessage("securityMetricsApproved-out-0",
                new Event<>(CREATE, securityMetrics.getBusinesskey(), securityMetrics));
    }

    private void sendMessage(String bindingName, Event<String, SecurityMetrics> event) {
        Message<Event<String, SecurityMetrics>> message = MessageBuilder.withPayload(event)
                .setHeader("partitionKey", event.getKey())
                .build();
        streamBridge.send(bindingName, message);
    }
}