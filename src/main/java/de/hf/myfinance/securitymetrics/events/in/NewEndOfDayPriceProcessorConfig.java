package de.hf.myfinance.securitymetrics.events.in;

import de.hf.framework.audit.AuditService;
import de.hf.framework.audit.Severity;
import de.hf.framework.exceptions.MFException;
import de.hf.myfinance.event.Event;
import de.hf.myfinance.restmodel.EndOfDayPrice;
import de.hf.myfinance.restmodel.EndOfDayPrices;
import de.hf.myfinance.securitymetrics.persistence.entities.PriceEntity;
import de.hf.myfinance.securitymetrics.persistence.repositories.PriceRepository;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import reactor.core.publisher.Mono;

import java.time.LocalDate;
import java.util.TreeMap;
import java.util.function.Consumer;

@Configuration
public class NewEndOfDayPriceProcessorConfig {

    private final AuditService auditService;
    protected static final String AUDIT_MSG_TYPE = "valueProcessor_Event";
    private final PriceRepository priceRepository;

    public NewEndOfDayPriceProcessorConfig(AuditService auditService, PriceRepository priceRepository) {
        this.auditService = auditService;
        this.priceRepository = priceRepository;
    }

    @Bean
    public Consumer<Event<String, EndOfDayPrices>> newEndOfDayPriceProcessor() {
        return event -> {

            try {
                auditService.saveMessage("Process Value in InstrumentService. message created at " + event.getEventCreatedAt(), Severity.DEBUG,
                        AUDIT_MSG_TYPE);

                if (event.getEventType() == Event.Type.CREATE) {
                    auditService.saveMessage("process valueCurve for inactivatablecheck of instrument with businesskey=" + event.getKey(),
                            Severity.DEBUG, AUDIT_MSG_TYPE);

                        priceRepository.findByBusinesskey(event.getData().getInstrumentBusinesskey())
                            .switchIfEmpty(handleNotExistingInstrument(event.getData().getInstrumentBusinesskey()))
                            .flatMap(e -> priceRepository.save(processValueInformation(e, event.getData())))
                            .block();
                } else {
                    String errorMessage = "Incorrect event type: " + event.getEventType() + ", expected a Create event";
                    auditService.saveMessage(errorMessage, Severity.WARN, AUDIT_MSG_TYPE);
                }
                auditService.saveMessage("Process Value in InstrumentService processing done!", Severity.DEBUG, AUDIT_MSG_TYPE);
            } catch (MFException e) {
                // no need to throw mfExceptions. These are Validation-Errors and retry the
                // message makes no sense.
            } catch (Exception e) {
                auditService.saveMessage("unexpected error", Severity.FATAL, AUDIT_MSG_TYPE);
                throw e;
            }
        };
    }

    private Mono<PriceEntity> handleNotExistingInstrument(String businesskey) {
        var priceEntity = new PriceEntity();
        priceEntity.setBusinesskey(businesskey);
        return Mono.just(priceEntity);
    }

    private PriceEntity processValueInformation(PriceEntity priceEntity,
        EndOfDayPrices endOfDayPrices) {
    
        TreeMap<LocalDate, EndOfDayPrice> prices = new TreeMap<>(endOfDayPrices.getPrices());
        priceEntity.setValue(prices.lastEntry().getValue().getValue());
        priceEntity.setCurrency(prices.lastEntry().getValue().getCurrencyKey());
        return priceEntity;
    }
}
