package de.hf.myfinance.securitymetrics.events.in;

import de.hf.framework.audit.AuditService;
import de.hf.framework.audit.Severity;
import de.hf.myfinance.event.Event;
import de.hf.myfinance.restmodel.SecurityMetrics;
import de.hf.myfinance.securitymetrics.service.SecurityMetricsService;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.function.Consumer;

@Configuration
public class ValidateSecurityMetricsProcessorConfig {

    private final SecurityMetricsService securityMetricsService;
    private final AuditService auditService;
    protected static final String AUDIT_MSG_TYPE = "ValidateSecurityMetricsProcessor_Event";

    public ValidateSecurityMetricsProcessorConfig(SecurityMetricsService securityMetricsService, AuditService auditService) {
        this.securityMetricsService = securityMetricsService;
        this.auditService = auditService;
    }

    @Bean
    public Consumer<Event<String, SecurityMetrics>> validateSecurityMetricsRequest() {
        return event -> {
            auditService.saveMessage("Process validateSecurityMetricsRequest in SecurityMetricsservice. message created at " + event.getEventCreatedAt(), Severity.DEBUG, AUDIT_MSG_TYPE);
            if (event.getEventType() == Event.Type.CREATE) {
                securityMetricsService.validateSecurityMetrics(event.getData());
            } else {
                String errorMessage = "Incorrect event type: " + event.getEventType() + ", expected a Create event";
                auditService.saveMessage(errorMessage, Severity.WARN, AUDIT_MSG_TYPE);
            }
            auditService.saveMessage("Process validateSecurityMetricsRequest in SecurityMetricsservice processing done!", Severity.DEBUG, AUDIT_MSG_TYPE);
        };
    }
}