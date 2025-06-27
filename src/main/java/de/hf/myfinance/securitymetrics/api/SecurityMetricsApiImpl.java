package de.hf.myfinance.securitymetrics.api;


import de.hf.myfinance.restapi.SecurityMetricsApi;
import de.hf.myfinance.restmodel.SecurityMetrics;
import de.hf.myfinance.securitymetrics.service.SecurityMetricsService;
import reactor.core.publisher.Flux;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.RestController;

import de.hf.framework.utils.ServiceUtil;

@RestController
public class SecurityMetricsApiImpl implements SecurityMetricsApi {
    ServiceUtil serviceUtil;
    SecurityMetricsService service;
    protected static final String AUDIT_MSG_TYPE="SecurityMetricsApiImpl_User_Event";

    @Value("${api.common.version}")
    String apiVersion;

    public SecurityMetricsApiImpl(ServiceUtil serviceUtil, SecurityMetricsService service) { 
        this.serviceUtil = serviceUtil;
        this.service = service;
    }

    @Override
    public String index() {
        return "Hello my SecurityMetricsService version:"+apiVersion;
    }

    @Override
    public Flux<SecurityMetrics> getSecurityMetrics() {
        return this.service.getSecurityMetrics();
    }

} 