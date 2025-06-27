package de.hf.myfinance.securitymetrics.persistence;


import java.util.List;

import de.hf.myfinance.restmodel.Instrument;
import de.hf.myfinance.restmodel.SecurityMetrics;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface DataReader {
    Mono<Instrument> findInstrumentByBusinesskey(String businesskey);
    Flux<Instrument> listActiveInstruments();
    Mono<Double> findPriceByBusinesskey(String businesskey);
    Mono<SecurityMetrics> findSecurityMetricsByBusinesskey(String businesskey);
    Flux<SecurityMetrics> findSecurityMetricByBusinesskeyIn(List<String> businesskeyList);
}
