package de.hf.myfinance.securitymetrics.service;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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
        securityMetrics.setCurrencyKey("6e57417d-54c0-36ae-ae48-2056db072a65");
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
        target.setExpectedFreeCashflowGrowthPerYear(updateMap(target.getExpectedFreeCashflowGrowthPerYear(), src.getExpectedFreeCashflowGrowthPerYear()));

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
        if(newSecurityMetrics.getShortLongTermDebtTotal() != null) {
            updatedSecurityMetrics.setShortLongTermDebtTotal(newSecurityMetrics.getShortLongTermDebtTotal());
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
        if(newSecurityMetrics.getTotalCash() != null) {
            updatedSecurityMetrics.setTotalCash(newSecurityMetrics.getTotalCash());
        }
        updatedSecurityMetrics.setHistoricalNetIncome(updateMap(updatedSecurityMetrics.getHistoricalNetIncome(), newSecurityMetrics.getHistoricalNetIncome()));
        updatedSecurityMetrics.setHistoricalRevenue(updateMap(updatedSecurityMetrics.getHistoricalRevenue(), newSecurityMetrics.getHistoricalRevenue()));
        updatedSecurityMetrics.setHistoricalFreeCashflow(updateMap(updatedSecurityMetrics.getHistoricalFreeCashflow(), newSecurityMetrics.getHistoricalFreeCashflow()));
        
        setStaticSecurityMetrics(updatedSecurityMetrics, newSecurityMetrics);
        return updatedSecurityMetrics;
    }

    private Map<Integer, Double> updateMap(Map<Integer, Double> oldMap, Map<Integer, Double> newMap) {
        if(newMap != null && !newMap.isEmpty()) {
            if(oldMap==null) {
                return newMap;
            } else {
                oldMap.putAll(newMap);
            }
        } return oldMap;
    }

    private Mono<SecurityMetrics> calcSecurityMetrics(SecurityMetrics securityMetrics) {
        if(securityMetrics.getCapitalExpenditures() != null && securityMetrics.getOperatingCashflow() != null) {
            securityMetrics.setFreeCashflow(securityMetrics.getOperatingCashflow() - securityMetrics.getCapitalExpenditures());
        }
        securityMetrics.setAvgFreeCashflow5Y(calcAvgFCF(securityMetrics.getHistoricalFreeCashflow()));
        securityMetrics.setAvgFreeCashflowGrowth5Y(calcAvgFcfGrowth(securityMetrics.getHistoricalFreeCashflow()));
        securityMetrics.setExpectedFreeCashflow(calcExpectedFreeCashflow(securityMetrics.getFreeCashflow(), securityMetrics.getAvgFreeCashflow5Y()));

        securityMetrics.setIntrinsicValue(calcIntrinsicValuePerShare(securityMetrics)); 
        
        if(securityMetrics.getPrice() != null 
            && securityMetrics.getPrice() != 0.0
            && securityMetrics.getIntrinsicValue() != null) {

            securityMetrics.setIntrinsicValueMargin((securityMetrics.getIntrinsicValue() -securityMetrics.getPrice())/securityMetrics.getPrice());

            if(securityMetrics.getShortLongTermDebtTotal() != null && securityMetrics.getTotalCash() != null){
                double liabilitiesPerShare = securityMetrics.getShortLongTermDebtTotal() / securityMetrics.getSharesOutstanding();
                double cashPerShare = securityMetrics.getTotalCash() / securityMetrics.getSharesOutstanding();
                double evPerShare = securityMetrics.getPrice() + liabilitiesPerShare - cashPerShare;
                securityMetrics.setIntrinsicValueEVMargin((securityMetrics.getIntrinsicValue() -evPerShare)/evPerShare);
            }
        }
        securityMetrics = calcPE(securityMetrics);
        securityMetrics = calcDividendYield(securityMetrics);
        securityMetrics = calcLynch(securityMetrics);
        
        return Mono.just(securityMetrics);
    }

    private double calcExpectedFreeCashflow(Double freeCashflow, Double avgFreeCashflow5Y) {
        if(freeCashflow == null && avgFreeCashflow5Y == null) {
            return 0.0;
        }
        if(freeCashflow == null) {
            return avgFreeCashflow5Y;
        }
        if(avgFreeCashflow5Y == null) {
            return freeCashflow;
        }
        return Math.max(freeCashflow, avgFreeCashflow5Y);
    }   

    private Double calcAvgFCF(Map<Integer, Double> fcf){
        if (fcf == null || fcf.isEmpty()) {
            return 0.0;
        }
        return fcf.entrySet().stream()
                .sorted(Map.Entry.<Integer, Double>comparingByKey().reversed())
                .limit(5)
                .mapToDouble(Map.Entry::getValue)
                .average()
                .orElse(0.0);
    }

    private Double calcAvgFcfGrowth(Map<Integer, Double> fcf) {
        if (fcf == null || fcf.size() < 2) {
            return 0.0;
        }

        List<Map.Entry<Integer, Double>> sortedEntries = fcf.entrySet().stream()
                .sorted(Map.Entry.<Integer, Double>comparingByKey().reversed())
                .collect(Collectors.toList());

        List<Double> growthRates = new ArrayList<>();
        for (int i = 0; i < Math.min(5, sortedEntries.size() - 1); i++) {
            Double currentFcf = sortedEntries.get(i).getValue();
            Double previousFcf = sortedEntries.get(i + 1).getValue();

            if (previousFcf != 0) {
                double growth = (currentFcf - previousFcf) / Math.abs(previousFcf);
                growthRates.add(growth);
            }
        }

        if (growthRates.isEmpty()) {
            return 0.0;
        }

        return growthRates.stream().mapToDouble(d -> d).average().orElse(0.0);
    }

    private Double calcIntrinsicValuePerShare(SecurityMetrics securityMetrics) {
        if (securityMetrics.getSharesOutstanding() == null || securityMetrics.getSharesOutstanding() == 0) {
            return 0.0;
        }

        Double sumOfDiscountedFreecashflows;
        Double terminalValue;

        if (securityMetrics.getExpectedFreeCashflowGrowthPerYear() != null && securityMetrics.getExpectedFreeCashflowGrowthPerYear().size() == 10 && securityMetrics.getAvgMarktcapFreeCashflowRatio() != null) {
            
            List<Double> fcfList = securityMetrics.getExpectedFreeCashflowGrowthPerYear().entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .map(Map.Entry::getValue)
                .collect(Collectors.toList());

            sumOfDiscountedFreecashflows = 0.0;
            var currentFcf = securityMetrics.getExpectedFreeCashflow();
            for (int i = 0; i < 10; i++) {
                currentFcf = currentFcf * fcfList.get(i);
                sumOfDiscountedFreecashflows += currentFcf / Math.pow(DISCOUNTFACTOR, i +1);
            }
            terminalValue = (currentFcf * securityMetrics.getAvgMarktcapFreeCashflowRatio()) / Math.pow(DISCOUNTFACTOR, 10);

        } else {
            if (securityMetrics.getExpectedFreeCashflow() == null || securityMetrics.getExpectedCashflowGrowth() == null || securityMetrics.getAvgMarktcapFreeCashflowRatio() == null) {
                return 0.0;
            }
            double growth = securityMetrics.getExpectedCashflowGrowth();
            sumOfDiscountedFreecashflows = 0.0;
            for (int i = 1; i <= 10; i++) {
                sumOfDiscountedFreecashflows += (securityMetrics.getExpectedFreeCashflow() * Math.pow(growth, i)) / Math.pow(DISCOUNTFACTOR, i);
            }
            
            double fcfIn10Years = securityMetrics.getExpectedFreeCashflow() * Math.pow(growth, 10);
            terminalValue = (fcfIn10Years * securityMetrics.getAvgMarktcapFreeCashflowRatio()) / Math.pow(DISCOUNTFACTOR, 10);
        } 

        double intrinsicValue = (sumOfDiscountedFreecashflows + terminalValue) / securityMetrics.getSharesOutstanding();
        return intrinsicValue;
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
        double dividentYield = securityMetrics.getDividendPerShare() * 100 / securityMetrics.getPrice();
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
