package de.hf.myfinance.securitymetrics.persistence;

import de.hf.myfinance.restmodel.EndOfDayPrice;
import de.hf.myfinance.restmodel.Instrument;
import de.hf.myfinance.restmodel.SecurityMetrics;
import de.hf.myfinance.securitymetrics.persistence.repositories.InstrumentRepository;
import de.hf.myfinance.securitymetrics.persistence.repositories.PriceRepository;
import de.hf.myfinance.securitymetrics.persistence.repositories.SecurityMetricsRepository;

import java.util.List;

import org.springframework.stereotype.Component;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Component
public class DataReaderImpl implements DataReader{
    private final InstrumentRepository instrumentRepository;
    private final InstrumentMapper instrumentMapper;
    private final PriceRepository priceRepository;
    private final SecurityMetricsMapper securityMetricsMapper;
    private final SecurityMetricsRepository securityMetricsRepository;


    public DataReaderImpl(InstrumentRepository instrumentRepository, InstrumentMapper instrumentMapper,
                            PriceRepository priceRepository,
                            SecurityMetricsMapper securityMetricsMapper, SecurityMetricsRepository securityMetricsRepository
                          ) {
        this.instrumentRepository = instrumentRepository;
        this.instrumentMapper = instrumentMapper;
        this.priceRepository = priceRepository;
        this.securityMetricsMapper = securityMetricsMapper;
        this.securityMetricsRepository = securityMetricsRepository;
    }


    @Override
    public Mono<Instrument> findInstrumentByBusinesskey(String businesskey) {
        return instrumentRepository.findByBusinesskey(businesskey)
                .map(instrumentMapper::entityToApi);
    }

    @Override
    public Mono<EndOfDayPrice> findPriceByBusinesskey(String businesskey) {
        return priceRepository.findByBusinesskey(businesskey)
                .map(p->{
                    return new EndOfDayPrice(p.getValue(), p.getCurrency());
                });
    }

    @Override
    public Mono<SecurityMetrics> findSecurityMetricsByBusinesskey(String businesskey) {
        return securityMetricsRepository.findByBusinesskey(businesskey)
                .map(securityMetricsMapper::entityToApi);
    }


    @Override
    public Flux<Instrument> listActiveInstruments() {
        return instrumentRepository.findByActive(true)
            .map(instrumentMapper::entityToApi);
    }


    @Override
    public Flux<SecurityMetrics> findSecurityMetricByBusinesskeyIn(List<String> businesskeyList) {
        return securityMetricsRepository.findByBusinesskeyIn(businesskeyList)
                .map(securityMetricsMapper::entityToApi);
    }

}
