package de.hf.myfinance.securitymetrics.events.in;

import de.hf.framework.audit.AuditService;
import de.hf.framework.audit.Severity;
import de.hf.myfinance.event.Event;
import de.hf.myfinance.restmodel.Instrument;
import de.hf.myfinance.restmodel.InstrumentType;
import de.hf.myfinance.restmodel.SecurityMetrics;
import de.hf.myfinance.securitymetrics.persistence.InstrumentMapper;
import de.hf.myfinance.securitymetrics.persistence.SecurityMetricsMapper;
import de.hf.myfinance.securitymetrics.persistence.repositories.InstrumentRepository;
import de.hf.myfinance.securitymetrics.persistence.repositories.SecurityMetricsRepository;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.function.Consumer;

@Configuration
public class SaveSecurityMetricsProcessorConfig {
    private final SecurityMetricsMapper securityMetricsMapper;
    private final SecurityMetricsRepository securityMetricsRepository;
    private final AuditService auditService;
    protected static final String AUDIT_MSG_TYPE="SaveSecurityMetricsProcessorConfig_Event";

    public SaveSecurityMetricsProcessorConfig(SecurityMetricsMapper securityMetricsMapper, SecurityMetricsRepository securityMetricsRepository, AuditService auditService) {
        this.securityMetricsMapper = securityMetricsMapper;
        this.securityMetricsRepository = securityMetricsRepository;
        this.auditService = auditService;
    }

    @Bean
    public Consumer<Event<String, SecurityMetrics>> saveSecurityMetricsProcessor() {
        return event -> {
            auditService.saveMessage("Process message created at "+ event.getEventCreatedAt(), Severity.INFO, AUDIT_MSG_TYPE);

            switch (event.getEventType()) {

                case CREATE:
                    var securityMetrics = event.getData();
                    auditService.saveMessage("Create instrument with ID: "+ securityMetrics.getBusinesskey(), Severity.INFO, AUDIT_MSG_TYPE);
                    var entity = securityMetricsMapper.apiToEntity(securityMetrics);
                    securityMetricsRepository.deleteByBusinesskey(entity.getBusinesskey()).then(securityMetricsRepository.save(entity)).block();

                    break;

                default:
                    String errorMessage = "Incorrect event type: " + event.getEventType() + ", expected a CREATE event";
                    auditService.saveMessage(errorMessage, Severity.WARN, AUDIT_MSG_TYPE);
            }

            auditService.saveMessage("Message processing done!", Severity.INFO, AUDIT_MSG_TYPE);

        };
    }
}
