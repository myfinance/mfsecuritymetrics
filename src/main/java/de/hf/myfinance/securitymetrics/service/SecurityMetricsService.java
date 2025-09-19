package de.hf.myfinance.securitymetrics.service;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import org.springframework.stereotype.Component;

import de.hf.framework.audit.AuditService;
import de.hf.framework.audit.Severity;
import de.hf.myfinance.exception.MFMsgKey;
import de.hf.myfinance.restmodel.AdditionalProperties;
import de.hf.myfinance.restmodel.EndOfDayPrice;
import de.hf.myfinance.restmodel.Instrument;
import de.hf.myfinance.restmodel.InstrumentType;
import de.hf.myfinance.restmodel.SecurityMetrics;
import de.hf.myfinance.securitymetrics.events.out.SecurityMetricsApprovedEventHandler;
import de.hf.myfinance.securitymetrics.persistence.DataReader;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Component
public class SecurityMetricsService {

    private final DataReader reader;
    protected final AuditService auditService;
    protected static final String AUDIT_MSG_TYPE = "SecurityMetricsService_Event";
    private final SecurityMetricsApprovedEventHandler eventHandler;

    private static final Double DISCOUNTFACTOR = 1.1;
    
    public SecurityMetricsService(DataReader reader, AuditService auditService, SecurityMetricsApprovedEventHandler eventHandler){
        this.reader = reader;
        this.auditService = auditService;
        this.eventHandler = eventHandler;
    }

    public Flux<SecurityMetrics> getSecurityMetrics(){
        return reader.listActiveInstruments().collectList().flatMap(instrumentList->{
                var keyList = new ArrayList<String>();
                instrumentList.forEach(i->keyList.add(i.getBusinesskey()));
                return Mono.just(keyList);
            })
            .flatMapMany(l->{
            return reader.findSecurityMetricByBusinesskeyIn(l);
        });
    }

    public Mono<SecurityMetrics> recalcSecurityMetrics(String businesskey) {
        return loadSecurityMetrics(businesskey)
            .switchIfEmpty(Mono.just(initEmptySecurityMetrics(businesskey)))
            .flatMap(this::loadPriceAndCurrency)
            .flatMap(this::setCurrencyCode)
            .flatMap(this::calcSecurityMetrics)
            .flatMap(this::saveSecurityMetrics);
    }

    public Mono<SecurityMetrics> validateSecurityMetrics(SecurityMetrics securityMetrics) {
        return loadInstrument(securityMetrics)
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
                    updatedSecurityMetrics = initSecurityMetrics(existingMetrics);
                }
                if(securityMetrics.getCurrencyKey()==null || securityMetrics.getCurrencyKey().isEmpty()) {
                    return auditService.handleMonoError("CurrencyKey is not allowed to be empty:"+existingMetrics.getDescription(), AUDIT_MSG_TYPE, MFMsgKey.ILLEGAL_ARGUMENTS).cast(SecurityMetrics.class);
                }
                if(!securityMetrics.getCurrencyKey().equals(existingMetrics.getCurrencyKey())){
                    updatedSecurityMetrics = initSecurityMetrics(existingMetrics);
                    setStaticSecurityMetrics(updatedSecurityMetrics, existingMetrics);
                }
                return Mono.just(updateBaseValues(updatedSecurityMetrics, securityMetrics));
            })
            .flatMap(this::loadPriceAndCurrency)
            .flatMap(this::setCurrencyCode)
            .flatMap(this::calcSecurityMetrics)
            .flatMap(this::saveSecurityMetrics);
    }

    private Mono<SecurityMetrics> loadSecurityMetrics(SecurityMetrics securityMetrics) {
        return loadSecurityMetrics(securityMetrics.getBusinesskey())
                .switchIfEmpty(Mono.just(initSecurityMetrics(securityMetrics)));
    }

    private Mono<SecurityMetrics> loadSecurityMetrics(String businesskey) {
        return reader.findSecurityMetricsByBusinesskey(businesskey);
    }

    private SecurityMetrics initEmptySecurityMetrics(String businesskey) {
        SecurityMetrics securityMetrics = new SecurityMetrics();
        securityMetrics.setBusinesskey(businesskey);
        securityMetrics.setFiscalEndDate(LocalDate.MIN);
        securityMetrics.setCurrencyCode("EUR");
        securityMetrics.setLastUpdateTs(LocalDateTime.now());
        return securityMetrics;
    }

    private SecurityMetrics initSecurityMetrics(SecurityMetrics newSecurityMetrics) {
        SecurityMetrics securityMetrics = new SecurityMetrics();
        securityMetrics.setBusinesskey(newSecurityMetrics.getBusinesskey());
        securityMetrics.setDescription(newSecurityMetrics.getDescription());
        securityMetrics.setFiscalEndDate(newSecurityMetrics.getFiscalEndDate());
        securityMetrics.setCurrencyKey(newSecurityMetrics.getCurrencyKey());
        securityMetrics.setCurrencyCode(newSecurityMetrics.getCurrencyCode());
        return securityMetrics;
    }

    private SecurityMetrics setStaticSecurityMetrics(SecurityMetrics target, SecurityMetrics src) {
        if(src.getAvgMarktcapFreeCashflowRatio() != null) {
            target.setAvgMarktcapFreeCashflowRatio(src.getAvgMarktcapFreeCashflowRatio());
        }
        if(src.getExpectedCashflowGrowth() != null) {
            target.setExpectedCashflowGrowth(src.getExpectedCashflowGrowth());
        }
        return target;
    }

    private Mono<SecurityMetrics> loadInstrument(SecurityMetrics securityMetrics) {
        return this.reader.findInstrumentByBusinesskey(securityMetrics.getBusinesskey())
                .switchIfEmpty(handleNotExistingInstrument(securityMetrics.getBusinesskey()))
                .flatMap(instrument -> {
                    if(instrument.getInstrumentType().equals(InstrumentType.EQUITY)) {
                        securityMetrics.setDescription(instrument.getDescription());
                        return Mono.just(securityMetrics);
                    }
                    return handleWrongInstrumentType(securityMetrics.getDescription());
                });
    }

    private Mono<Instrument> handleNotExistingInstrument(String businesskey){
        return auditService.handleMonoError("Instrument for businesskey:"+businesskey + " does not exists.", AUDIT_MSG_TYPE, MFMsgKey.UNKNOWN_INSTRUMENT_EXCEPTION).cast(Instrument.class);
    }

    private Mono<SecurityMetrics> handleWrongInstrumentType(String instrumentdesc){
        return auditService.handleMonoError("Instrumenttyp of :"+instrumentdesc + " is not Equity.", AUDIT_MSG_TYPE, MFMsgKey.WRONG_INSTRUMENTTYPE_EXCEPTION).cast(SecurityMetrics.class);
    }

    private SecurityMetrics updateBaseValues(SecurityMetrics updatedSecurityMetrics, SecurityMetrics newSecurityMetrics) {
        updatedSecurityMetrics.setLastUpdateTs(LocalDateTime.now());
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
        if(newSecurityMetrics.getSector() != null) {
            updatedSecurityMetrics.setSector(newSecurityMetrics.getSector());
        }
        if(newSecurityMetrics.getBeta() != null) {
            updatedSecurityMetrics.setBeta(newSecurityMetrics.getBeta());
        }
        setStaticSecurityMetrics(updatedSecurityMetrics, newSecurityMetrics);
        return updatedSecurityMetrics;
    }

    private Mono<SecurityMetrics> calcSecurityMetrics(SecurityMetrics securityMetrics) {
        if(securityMetrics.getCapitalExpenditures() != null && securityMetrics.getOperatingCashflow() != null) {
            securityMetrics.setFreeCashflow(securityMetrics.getOperatingCashflow() - securityMetrics.getCapitalExpenditures());
        }
        securityMetrics = calcIntrinsicValuePerShare(securityMetrics);
        if(securityMetrics.getPrice() != null 
            && securityMetrics.getPrice() != 0.0
            && securityMetrics.getIntrinsicValue() != null) {
            securityMetrics.setIntrinsicValueMargin((securityMetrics.getIntrinsicValue() -securityMetrics.getPrice())/securityMetrics.getPrice());
        }
        securityMetrics = calcPE(securityMetrics);
        securityMetrics = calcDividendYield(securityMetrics);
        securityMetrics = calcLynch(securityMetrics);
        
        return Mono.just(securityMetrics);
    }

    private SecurityMetrics calcIntrinsicValuePerShare(SecurityMetrics securityMetrics) {
        if(securityMetrics.getFreeCashflow() == null || securityMetrics.getSharesOutstanding() == null || securityMetrics.getExpectedCashflowGrowth() == null) {
            return securityMetrics;
        }
        Double discountFaktor = securityMetrics.getExpectedCashflowGrowth()/DISCOUNTFACTOR;
        Double sumOfDiscountedFreecashflows = discount(discountFaktor, securityMetrics.getFreeCashflow(), 10);
        Double marketcapIn10Y = securityMetrics.getFreeCashflow() 
            * Math.pow(discountFaktor, 10)
            * securityMetrics.getAvgMarktcapFreeCashflowRatio();
        double intrinsicValue = (sumOfDiscountedFreecashflows + marketcapIn10Y) / securityMetrics.getSharesOutstanding();
        securityMetrics.setIntrinsicValue(intrinsicValue);
        return securityMetrics;
    }

    private SecurityMetrics calcPE(SecurityMetrics securityMetrics) {
        if(securityMetrics.getEps() == null || securityMetrics.getPrice() == null) {
            return securityMetrics;
        }
        double pe = securityMetrics.getPrice() / securityMetrics.getEps();
        securityMetrics.setPe(pe);
        return securityMetrics;
    }

    private SecurityMetrics calcDividendYield(SecurityMetrics securityMetrics) {
        if(securityMetrics.getDividendPerShare() == null || securityMetrics.getPrice() == null) {
            return securityMetrics;
        }
        double dividentYield = securityMetrics.getPrice() * 100 / securityMetrics.getDividendPerShare();
        securityMetrics.setDividendYield(dividentYield);
        return securityMetrics;
    }

    private SecurityMetrics calcLynch(SecurityMetrics securityMetrics) {
        if(securityMetrics.getDividendYield() == null || securityMetrics.getDilutedEPS5Y() == null || securityMetrics.getPe() == null) {
            return securityMetrics;
        }
        double lynch = (securityMetrics.getDividendYield() + securityMetrics.getDilutedEPS5Y()) / securityMetrics.getPe();
        securityMetrics.setLynchScore(lynch);
        return securityMetrics;
    }

    private double discount(double faktor, double value, int years) {
        double result = 0.0;
        for(int exponent = 1; exponent <= years; exponent++) {
            result += Math.pow(faktor, exponent);
            exponent++;
        }
        return value* result;
    }

    private Mono<SecurityMetrics> saveSecurityMetrics(SecurityMetrics securityMetrics) {
        auditService.saveMessage("SecurityMetrics validated:businesskey=" + securityMetrics.getBusinesskey() + " desc=" + securityMetrics.getDescription(), Severity.INFO, AUDIT_MSG_TYPE);
        eventHandler.sendInstrumentApprovedEvent(securityMetrics);
        return Mono.just(securityMetrics);
    }

    private Mono<SecurityMetrics> setCurrencyCode(SecurityMetrics securityMetrics) {

        return reader.findInstrumentByBusinesskey(securityMetrics.getCurrencyKey())
            .map(instrument -> {
                securityMetrics.setCurrencyCode(instrument.getAdditionalProperties().get(AdditionalProperties.CURRENCYCODE));
                return securityMetrics;
            });
            
    }

    private Mono<SecurityMetrics> loadPriceAndCurrency(SecurityMetrics securityMetrics) {
        return reader.findPriceByBusinesskey(securityMetrics.getBusinesskey())
            .switchIfEmpty(Mono.just(new EndOfDayPrice(0.0, securityMetrics.getCurrencyKey())))
            .flatMap(price -> 
                setPrice(securityMetrics, price)
                );
    }



    private Mono<Double> convertCurrency(Double value, String fromCurrency, String toCurrency) {
        if (fromCurrency.equals(toCurrency)) {
            return Mono.just(value);
        }
        Mono<Double> fromCurrencyInEur = Mono.just(1.0);
        if(!fromCurrency.startsWith("EUR")){
            fromCurrencyInEur = reader.findPriceByBusinesskey(fromCurrency)
                .map(price -> price.getValue())
                .switchIfEmpty(auditService.handleMonoError("No price found for currency:"+fromCurrency, AUDIT_MSG_TYPE, MFMsgKey.UNKNOWN_INSTRUMENT_EXCEPTION).cast(Double.class));
        }

        Mono<Double> toCurrencyInEur = Mono.just(1.0);
        if(!toCurrency.startsWith("EUR")) {
            toCurrencyInEur = reader.findPriceByBusinesskey(toCurrency)
                .map(price -> 
                    price.getValue()
                )
                .switchIfEmpty(auditService.handleMonoError("No price found for currency:"+toCurrency, AUDIT_MSG_TYPE, MFMsgKey.UNKNOWN_INSTRUMENT_EXCEPTION).cast(Double.class));
        }


        return fromCurrencyInEur.zipWith(toCurrencyInEur, (from, to) -> (value * from) / to);
    }

    private Mono<SecurityMetrics> setPrice(SecurityMetrics securityMetrics, EndOfDayPrice price) {
        if(securityMetrics.getCurrencyKey().equals(price.getCurrencyKey())){
            securityMetrics.setPrice(price.getValue());
            return Mono.just(securityMetrics);
        } else {
            return convertCurrency(price.getValue(), price.getCurrencyKey(), securityMetrics.getCurrencyKey())
                .map(convertedValue -> {
                    securityMetrics.setPrice(convertedValue);
                    return securityMetrics;
                });
        }
    }
}
