package de.hf.myfinance.securitymetrics.service;

import java.util.ArrayList;
import org.springframework.stereotype.Component;

import de.hf.framework.audit.AuditService;
import de.hf.myfinance.exception.MFMsgKey;
import de.hf.myfinance.restmodel.EndOfDayPrice;
import de.hf.myfinance.restmodel.Instrument;
import de.hf.myfinance.restmodel.InstrumentType;
import de.hf.myfinance.restmodel.SecurityMetrics;
import de.hf.myfinance.securitymetrics.persistence.DataReader;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Component
public class SecurityMetricsService {

    private final DataReader reader;
    protected final AuditService auditService;
    protected static final String AUDIT_MSG_TYPE = "SecurityMetricsService_Event";

    public SecurityMetricsService(DataReader reader, AuditService auditService){
        this.reader = reader;
        this.auditService = auditService;
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

    public Mono<SecurityMetrics> validateSecurityMetrics(SecurityMetrics securityMetrics) {
        return loadInstrument(securityMetrics.getBusinesskey())
            .flatMap(this::loadSecurityMetrics)
            .flatMap(existingMetrics -> {
                var updatedSecurityMetrics = existingMetrics;
                // new securityMetrics have no valid 
                if(securityMetrics.getFiscalEndDate() == null){
                    return auditService.handleMonoError("No valid fiscalEndDate for instrument:"+existingMetrics.getDescription(), AUDIT_MSG_TYPE, MFMsgKey.ILLEGAL_ARGUMENTS).cast(SecurityMetrics.class);
                }
                if(existingMetrics.getFiscalEndDate() != null && existingMetrics.getFiscalEndDate().isAfter(securityMetrics.getFiscalEndDate())) {
                    return auditService.handleMonoError("fiscalEndDate is to old for instrument:"+existingMetrics.getDescription(), AUDIT_MSG_TYPE, MFMsgKey.ILLEGAL_ARGUMENTS).cast(SecurityMetrics.class);
                }
                //the existing metrics are out of date
                if(existingMetrics.getFiscalEndDate() != null && existingMetrics.getFiscalEndDate().isBefore(securityMetrics.getFiscalEndDate())) {
                    updatedSecurityMetrics = initSecurityMetrics(existingMetrics.getBusinesskey(), existingMetrics.getDescription());
                    updatedSecurityMetrics.setFiscalEndDate(securityMetrics.getFiscalEndDate());
                    updatedSecurityMetrics.setCurrencyKey(securityMetrics.getCurrencyKey());
                }
                if(!securityMetrics.getCurrencyKey().equals(updatedSecurityMetrics.getCurrencyKey())) {
                    return auditService.handleMonoError("Currency does not match for instrument:"+existingMetrics.getDescription(), AUDIT_MSG_TYPE, MFMsgKey.ILLEGAL_ARGUMENTS).cast(SecurityMetrics.class);
                }
                return Mono.just(updateBaseValues(updatedSecurityMetrics, securityMetrics));
            })
            .flatMap(this::loadPriceAndCurrency)
            .flatMap(this::calcSecurityMetrics)
            .flatMap(this::saveSecurityMetrics);
    }

    private Mono<SecurityMetrics> calcSecurityMetrics(SecurityMetrics securityMetrics) {
        if(securityMetrics.getCapitalExpenditures() != null && securityMetrics.getOperatingCashflow() != null) {
            securityMetrics.setFreeCashflow(securityMetrics.getOperatingCashflow() - securityMetrics.getCapitalExpenditures());
        }
        return Mono.just(securityMetrics);
    }

    private Mono<SecurityMetrics> loadSecurityMetrics(Instrument instrument) {
        return reader.findSecurityMetricsByBusinesskey(instrument.getBusinesskey())
                .switchIfEmpty(Mono.just(initSecurityMetrics(instrument.getBusinesskey(), instrument.getDescription())));
    }

    private SecurityMetrics initSecurityMetrics(String businesskey, String description) {
        SecurityMetrics securityMetrics = new SecurityMetrics();
        securityMetrics.setBusinesskey(businesskey);
        securityMetrics.setDescription(description);
        return securityMetrics;
    }

    private Mono<Instrument> loadInstrument(String businesskey) {
        return this.reader.findInstrumentByBusinesskey(businesskey)
                .switchIfEmpty(handleNotExistingInstrument(businesskey))
                .flatMap(instrument -> {
                    if(instrument.getInstrumentType().equals(InstrumentType.EQUITY)) {
                        return Mono.just(instrument);
                    }
                    return handleWrongInstrumentType(instrument);
                });
    }

    private Mono<Instrument> handleNotExistingInstrument(String businesskey){
        return auditService.handleMonoError("Instrument for businesskey:"+businesskey + " does not exists.", AUDIT_MSG_TYPE, MFMsgKey.UNKNOWN_INSTRUMENT_EXCEPTION).cast(Instrument.class);
    }

    private Mono<Instrument> handleWrongInstrumentType(Instrument instrument){
        return auditService.handleMonoError("Instrumenttyp of :"+instrument.getDescription() + " is not Equity.", AUDIT_MSG_TYPE, MFMsgKey.WRONG_INSTRUMENTTYPE_EXCEPTION).cast(Instrument.class);
    }

    private SecurityMetrics updateBaseValues(SecurityMetrics updatedSecurityMetrics, SecurityMetrics newSecurityMetrics) {

        if(newSecurityMetrics.getCapitalExpenditures() != null) {
            updatedSecurityMetrics.setCapitalExpenditures(newSecurityMetrics.getCapitalExpenditures());
        }
        if(newSecurityMetrics.getOperatingCashflow() != null) {
            updatedSecurityMetrics.setOperatingCashflow(newSecurityMetrics.getOperatingCashflow());
        }
        if(newSecurityMetrics.getSharesOutstanding() != null) {
            updatedSecurityMetrics.setSharesOutstanding(newSecurityMetrics.getSharesOutstanding());
        }
        if(newSecurityMetrics.getRevenue() != null) {
            updatedSecurityMetrics.setRevenue(newSecurityMetrics.getRevenue());
        }
        if(newSecurityMetrics.getEps() != null) {
            updatedSecurityMetrics.setEps(newSecurityMetrics.getEps());
        }
        if(newSecurityMetrics.getTotalAssets() != null) {
            updatedSecurityMetrics.setTotalAssets(newSecurityMetrics.getTotalAssets());
        }
        if(newSecurityMetrics.getTotalLiabilities() != null) {
            updatedSecurityMetrics.setTotalLiabilities(newSecurityMetrics.getTotalLiabilities());
        }
        if(newSecurityMetrics.getDilutedEPS5Y() != null) {
            updatedSecurityMetrics.setDilutedEPS5Y(newSecurityMetrics.getDilutedEPS5Y());
        }
        if(newSecurityMetrics.getDividendPerShare() != null) {
            updatedSecurityMetrics.setDividendPerShare(newSecurityMetrics.getDividendPerShare());
        }
        return updatedSecurityMetrics;
    }

    private Mono<SecurityMetrics> saveSecurityMetrics(SecurityMetrics securityMetrics) {
        if(securityMetrics.getBusinesskey() != null && !securityMetrics.getBusinesskey().isEmpty()
            && securityMetrics.getFiscalEndDate() != null) {
            
        }
        return Mono.just(securityMetrics);
    }

    private Mono<SecurityMetrics> loadPriceAndCurrency(SecurityMetrics securityMetrics) {
        return reader.findPriceByBusinesskey(securityMetrics.getBusinesskey())
            .flatMap(price -> setPrice(securityMetrics, price));
    }



    private Mono<Double> convertCurrency(Double value, String fromCurrency, String toCurrency) {
        if (fromCurrency.equals(toCurrency)) {
            return Mono.just(value);
        }
        
        Mono<Double> fromCurrencyInEur = reader.findPriceByBusinesskey(fromCurrency)
            .map(price -> price.getValue())
            .switchIfEmpty(auditService.handleMonoError("No price found for currency:"+fromCurrency, AUDIT_MSG_TYPE, MFMsgKey.UNKNOWN_INSTRUMENT_EXCEPTION).cast(Double.class));

        Mono<Double> toCurrencyInEur = Mono.just(1.0);
        if(!toCurrency.equals("EUR")) {
            toCurrencyInEur = reader.findPriceByBusinesskey(toCurrency)
                .map(price -> price.getValue())
                .switchIfEmpty(auditService.handleMonoError("No price found for currency:"+toCurrency, AUDIT_MSG_TYPE, MFMsgKey.UNKNOWN_INSTRUMENT_EXCEPTION).cast(Double.class));
        }


        return fromCurrencyInEur.zipWith(toCurrencyInEur, (from, to) -> (value * from) / to);
    }

    private Mono<SecurityMetrics> setPrice(SecurityMetrics securityMetrics, EndOfDayPrice price) {
        if(securityMetrics.getCurrencyKey().equals(price.getCurrencyKey())){
            securityMetrics.setPrice(price.getValue());
        } else {
            convertCurrency(price.getValue(), price.getCurrencyKey(), securityMetrics.getCurrencyKey())
                .map(convertedValue -> {
                    securityMetrics.setPrice(convertedValue);
                    return securityMetrics;
                });
        }

        return reader.findInstrumentByBusinesskey(price.getCurrencyKey())
            .flatMap(currency -> {
                if(currency.getDescription().equals("Euro")) {
                    securityMetrics.setPriceInEuro(price.getValue());
                } else {
                    convertCurrency(price.getValue(), price.getCurrencyKey(), "EUR")
                        .map(convertedValue -> {
                            securityMetrics.setPriceInEuro(convertedValue);
                            return securityMetrics;
                        });
                }
                return Mono.just(securityMetrics);
            });
    }
}
