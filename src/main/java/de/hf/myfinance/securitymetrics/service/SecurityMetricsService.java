package de.hf.myfinance.securitymetrics.service;

import java.util.ArrayList;

import org.springframework.stereotype.Component;

import de.hf.myfinance.restmodel.SecurityMetrics;
import de.hf.myfinance.securitymetrics.persistence.DataReader;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Component
public class SecurityMetricsService {
    DataReader reader;
    public SecurityMetricsService(DataReader reader){
        this.reader = reader;
    }

    
    public Flux<SecurityMetrics> getSecurityMetrics(){
        return reader.listActiveInstruments().collectList().flatMap(instrumentList->{
                var keyList = new ArrayList<String>();
                instrumentList.forEach(i->keyList.add(i.getBusinesskey()));
                return Mono.just(keyList);
            })
            .flatMapMany(l->{
            Flux<SecurityMetrics> bla =  reader.findSecurityMetricByBusinesskeyIn(l);
            return bla;
        });
    }
}
