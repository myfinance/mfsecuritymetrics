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


        var result = securityMetricsService.validateSecurityMetrics(securityMetrics).block();
        final List<String> messages = getMessages("securityMetricsApproved-out-0");
        assertEquals(1, messages.size());
    }
}