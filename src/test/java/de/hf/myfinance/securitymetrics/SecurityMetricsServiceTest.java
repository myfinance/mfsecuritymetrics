package de.hf.myfinance.securitymetrics;

import de.hf.myfinance.restmodel.InstrumentType;
import de.hf.myfinance.restmodel.SecurityMetrics;
import de.hf.myfinance.securitymetrics.persistence.entities.InstrumentEntity;
import de.hf.myfinance.securitymetrics.persistence.entities.PriceEntity;
import de.hf.myfinance.securitymetrics.persistence.repositories.InstrumentRepository;
import de.hf.myfinance.securitymetrics.persistence.repositories.PriceRepository;
import de.hf.myfinance.securitymetrics.service.SecurityMetricsService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.stream.binder.test.TestChannelBinderConfiguration;
import org.springframework.context.annotation.Import;
import org.testcontainers.junit.jupiter.Testcontainers;
import static org.springframework.boot.test.context.SpringBootTest.WebEnvironment.RANDOM_PORT;

import java.time.LocalDate;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

import de.hf.myfinance.restmodel.EndOfDayPrice;
import de.hf.myfinance.restmodel.Instrument;
import de.hf.myfinance.securitymetrics.persistence.DataReader;
import org.mockito.Mockito;
import org.springframework.boot.test.mock.mockito.MockBean;
import reactor.core.publisher.Mono;

@SpringBootTest(webEnvironment = RANDOM_PORT)
@Testcontainers
@Import({TestChannelBinderConfiguration.class})
public class SecurityMetricsServiceTest extends EventProcessorTestBase{

    @Autowired
    SecurityMetricsService securityMetricsService;
    @Autowired
    InstrumentRepository instrumentRepository;
    @Autowired
    PriceRepository priceRepository;

    @MockBean
    private DataReader reader;

    @Test
    void convertCurrencyToEur() {
        var chf = new InstrumentEntity();
        chf.setBusinesskey("CHF");
        chf.setDescription("Schweizer Franken");
        instrumentRepository.save(chf).block();

        var chfPrice = new PriceEntity();
        chfPrice.setBusinesskey("CHF");
        chfPrice.setValue(0.9);
        priceRepository.save(chfPrice).block();

        var eur = new InstrumentEntity();
        eur.setBusinesskey("EUR");
        eur.setDescription("Euro");
        instrumentRepository.save(eur).block();

        var eurPrice = new PriceEntity();
        eurPrice.setBusinesskey("EUR");
        eurPrice.setValue(1.0);
        priceRepository.save(eurPrice).block();

        var usd = new InstrumentEntity();
        usd.setBusinesskey("USD");
        usd.setDescription("US Dollar");
        instrumentRepository.save(usd).block();

        var usdPrice = new PriceEntity();
        usdPrice.setBusinesskey("USD");
        usdPrice.setValue(1.1);
        priceRepository.save(usdPrice).block();

        var security = new InstrumentEntity();
        security.setBusinesskey("test");
        security.setDescription("test");
        security.setInstrumentType(InstrumentType.EQUITY);
        instrumentRepository.save(security).block();

        var securityPrice = new PriceEntity();
        securityPrice.setBusinesskey("test");
        securityPrice.setValue(100.0);
        securityPrice.setCurrency("EUR");
        priceRepository.save(securityPrice).block();

        var securityMetrics = new SecurityMetrics();
        securityMetrics.setBusinesskey("test");
        securityMetrics.setCurrencyKey("EUR");
        securityMetrics.setFiscalEndDate(LocalDate.now());

        Instrument instrument = new Instrument();
        instrument.setBusinesskey("test");
        instrument.setInstrumentType(InstrumentType.EQUITY);
        instrument.setDescription("test");
        Mockito.when(reader.findInstrumentByBusinesskey("test")).thenReturn(Mono.just(instrument));
        Mockito.when(reader.findSecurityMetricsByBusinesskey("test")).thenReturn(Mono.empty());
        Mockito.when(reader.findPriceByBusinesskey("test")).thenReturn(Mono.just(new EndOfDayPrice(100.0, "EUR")));


        var result = securityMetricsService.validateSecurityMetrics(securityMetrics).block();
        final List<String> messages = getMessages("securityMetricsApproved-out-0");
        assertEquals(1, messages.size());
    }

    @Test
    void testConvertCurrency_EURToUSD_returnsConvertedValue() {
        String fromCurrency = "EUR";
        String toCurrency = "USD";
        String businesskey = "test";
        double value = 100.0;

        Instrument instrument = new Instrument();
        instrument.setBusinesskey(businesskey);
        instrument.setInstrumentType(InstrumentType.EQUITY);
        instrument.setDescription("test instrument");

        SecurityMetrics securityMetrics = new SecurityMetrics();
        securityMetrics.setBusinesskey(businesskey);
        securityMetrics.setCurrencyKey(toCurrency);
        securityMetrics.setFiscalEndDate(LocalDate.now());

        EndOfDayPrice price = new EndOfDayPrice(value, fromCurrency);
        EndOfDayPrice toCurrencyPrice = new EndOfDayPrice(0.85, toCurrency);

        Mockito.when(reader.findInstrumentByBusinesskey(businesskey)).thenReturn(Mono.just(instrument));
        Mockito.when(reader.findSecurityMetricsByBusinesskey(businesskey)).thenReturn(Mono.empty());
        Mockito.when(reader.findPriceByBusinesskey(businesskey)).thenReturn(Mono.just(price));
        Mockito.when(reader.findPriceByBusinesskey(toCurrency)).thenReturn(Mono.just(toCurrencyPrice));

        SecurityMetrics result = securityMetricsService.validateSecurityMetrics(securityMetrics).block();

        assertEquals(value / 0.85, result.getPrice());
    }
}