package de.hf.myfinance.securitymetrics;

import de.hf.myfinance.restmodel.InstrumentType;
import de.hf.myfinance.restmodel.SecurityMetrics;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

        var eur = new Instrument();
        eur.setBusinesskey("EUR");
        eur.setDescription("Euro");
        eur.setInstrumentType(InstrumentType.CURRENCY);
        


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
        Mockito.when(reader.findPriceByBusinesskey("test")).thenReturn(Mono.just(new EndOfDayPrice(100.0, "USDkey")));
        Mockito.when(reader.findPriceByBusinesskey("USDkey")).thenReturn(Mono.just(new EndOfDayPrice(0.5, "EUR")));
        Mockito.when(reader.findInstrumentByBusinesskey("EUR")).thenReturn(Mono.just(eur));


        var result = securityMetricsService.validateSecurityMetrics(securityMetrics).block();
        assertEquals(50, result.getPrice());
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

        var usd = new Instrument();
        usd.setBusinesskey("USD");
        usd.setDescription("US Dollar");
        usd.setInstrumentType(InstrumentType.CURRENCY);

        Mockito.when(reader.findInstrumentByBusinesskey(businesskey)).thenReturn(Mono.just(instrument));
        Mockito.when(reader.findSecurityMetricsByBusinesskey(businesskey)).thenReturn(Mono.empty());
        Mockito.when(reader.findPriceByBusinesskey(businesskey)).thenReturn(Mono.just(price));
        Mockito.when(reader.findPriceByBusinesskey(toCurrency)).thenReturn(Mono.just(toCurrencyPrice));
        Mockito.when(reader.findInstrumentByBusinesskey("USD")).thenReturn(Mono.just(usd));

        SecurityMetrics result = securityMetricsService.validateSecurityMetrics(securityMetrics).block();

        assertEquals(value / 0.85, result.getPrice());
    }

    @Test
    void testRecalcSecurityMetrics_avgFcf() {
        var securityMetrics = new SecurityMetrics();
        securityMetrics.setBusinesskey("test");
        securityMetrics.setCurrencyKey("EUR");
        securityMetrics.setFiscalEndDate(LocalDate.now());
        Map<Integer, Double> fcf = new HashMap<>();
        fcf.put(2020, 100.0);
        fcf.put(2021, 110.0);
        fcf.put(2022, 120.0);
        fcf.put(2023, 130.0);
        fcf.put(2024, 140.0);
        fcf.put(2019, 90.0);
        securityMetrics.setHistoricalFreeCashflow(fcf);

        Instrument instrument = new Instrument();
        instrument.setBusinesskey("test");
        instrument.setInstrumentType(InstrumentType.EQUITY);
        instrument.setDescription("test");
        Mockito.when(reader.findInstrumentByBusinesskey("test")).thenReturn(Mono.just(instrument));
        Mockito.when(reader.findSecurityMetricsByBusinesskey("test")).thenReturn(Mono.just(securityMetrics));
        Mockito.when(reader.findPriceByBusinesskey("test")).thenReturn(Mono.just(new EndOfDayPrice(100.0, "EUR")));
        Mockito.when(reader.findInstrumentByBusinesskey("EUR")).thenReturn(Mono.just(new Instrument()));


        var result = securityMetricsService.recalcSecurityMetrics("test").block();
        assertEquals(120.0, result.getAvgFreeCashflow5Y(), 0.001);
    }

    @Test
    void testRecalcSecurityMetrics_avgFcf_lessThan5() {
        var securityMetrics = new SecurityMetrics();
        securityMetrics.setBusinesskey("test");
        securityMetrics.setCurrencyKey("EUR");
        securityMetrics.setFiscalEndDate(LocalDate.now());
        Map<Integer, Double> fcf = new HashMap<>();
        fcf.put(2022, 120.0);
        fcf.put(2023, 130.0);
        fcf.put(2024, 140.0);
        securityMetrics.setHistoricalFreeCashflow(fcf);

        Instrument instrument = new Instrument();
        instrument.setBusinesskey("test");
        instrument.setInstrumentType(InstrumentType.EQUITY);
        instrument.setDescription("test");
        Mockito.when(reader.findInstrumentByBusinesskey("test")).thenReturn(Mono.just(instrument));
        Mockito.when(reader.findSecurityMetricsByBusinesskey("test")).thenReturn(Mono.just(securityMetrics));
        Mockito.when(reader.findPriceByBusinesskey("test")).thenReturn(Mono.just(new EndOfDayPrice(100.0, "EUR")));
        Mockito.when(reader.findInstrumentByBusinesskey("EUR")).thenReturn(Mono.just(new Instrument()));


        var result = securityMetricsService.recalcSecurityMetrics("test").block();
        assertEquals(130.0, result.getAvgFreeCashflow5Y(), 0.001);
    }

    @Test
    void testRecalcSecurityMetrics_avgFcf_empty() {
        var securityMetrics = new SecurityMetrics();
        securityMetrics.setBusinesskey("test");
        securityMetrics.setCurrencyKey("EUR");
        securityMetrics.setFiscalEndDate(LocalDate.now());
        Map<Integer, Double> fcf = new HashMap<>();
        securityMetrics.setHistoricalFreeCashflow(fcf);

        Instrument instrument = new Instrument();
        instrument.setBusinesskey("test");
        instrument.setInstrumentType(InstrumentType.EQUITY);
        instrument.setDescription("test");
        Mockito.when(reader.findInstrumentByBusinesskey("test")).thenReturn(Mono.just(instrument));
        Mockito.when(reader.findSecurityMetricsByBusinesskey("test")).thenReturn(Mono.just(securityMetrics));
        Mockito.when(reader.findPriceByBusinesskey("test")).thenReturn(Mono.just(new EndOfDayPrice(100.0, "EUR")));
        Mockito.when(reader.findInstrumentByBusinesskey("EUR")).thenReturn(Mono.just(new Instrument()));


        var result = securityMetricsService.recalcSecurityMetrics("test").block();
        assertEquals(0.0, result.getAvgFreeCashflow5Y(), 0.001);
    }

    @Test
    void testRecalcSecurityMetrics_avgFcfGrowth() {
        var securityMetrics = new SecurityMetrics();
        securityMetrics.setBusinesskey("test");
        securityMetrics.setCurrencyKey("EUR");
        securityMetrics.setFiscalEndDate(LocalDate.now());
        Map<Integer, Double> fcf = new HashMap<>();
        fcf.put(2020, 100.0);
        fcf.put(2021, 110.0);
        fcf.put(2022, 120.0);
        fcf.put(2023, 130.0);
        fcf.put(2024, 140.0);
        fcf.put(2019, 90.0);
        securityMetrics.setHistoricalFreeCashflow(fcf);

        Instrument instrument = new Instrument();
        instrument.setBusinesskey("test");
        instrument.setInstrumentType(InstrumentType.EQUITY);
        instrument.setDescription("test");
        Mockito.when(reader.findInstrumentByBusinesskey("test")).thenReturn(Mono.just(instrument));
        Mockito.when(reader.findSecurityMetricsByBusinesskey("test")).thenReturn(Mono.just(securityMetrics));
        Mockito.when(reader.findPriceByBusinesskey("test")).thenReturn(Mono.just(new EndOfDayPrice(100.0, "EUR")));
        Mockito.when(reader.findInstrumentByBusinesskey("EUR")).thenReturn(Mono.just(new Instrument()));


        var result = securityMetricsService.recalcSecurityMetrics("test").block();
        assertEquals(0.0924, result.getAvgFreeCashflowGrowth5Y(), 0.001);
    }

    @Test
    void testRecalcSecurityMetrics_avgFcfGrowth_lessThan5() {
        var securityMetrics = new SecurityMetrics();
        securityMetrics.setBusinesskey("test");
        securityMetrics.setCurrencyKey("EUR");
        securityMetrics.setFiscalEndDate(LocalDate.now());
        Map<Integer, Double> fcf = new HashMap<>();
        fcf.put(2022, 120.0);
        fcf.put(2023, 130.0);
        fcf.put(2024, 140.0);
        securityMetrics.setHistoricalFreeCashflow(fcf);

        Instrument instrument = new Instrument();
        instrument.setBusinesskey("test");
        instrument.setInstrumentType(InstrumentType.EQUITY);
        instrument.setDescription("test");
        Mockito.when(reader.findInstrumentByBusinesskey("test")).thenReturn(Mono.just(instrument));
        Mockito.when(reader.findSecurityMetricsByBusinesskey("test")).thenReturn(Mono.just(securityMetrics));
        Mockito.when(reader.findPriceByBusinesskey("test")).thenReturn(Mono.just(new EndOfDayPrice(100.0, "EUR")));
        Mockito.when(reader.findInstrumentByBusinesskey("EUR")).thenReturn(Mono.just(new Instrument()));


        var result = securityMetricsService.recalcSecurityMetrics("test").block();
        assertEquals(0.08, result.getAvgFreeCashflowGrowth5Y(), 0.01);
    }

    @Test
    void testRecalcSecurityMetrics_avgFcfGrowth_empty() {
        var securityMetrics = new SecurityMetrics();
        securityMetrics.setBusinesskey("test");
        securityMetrics.setCurrencyKey("EUR");
        securityMetrics.setFiscalEndDate(LocalDate.now());
        Map<Integer, Double> fcf = new HashMap<>();
        securityMetrics.setHistoricalFreeCashflow(fcf);

        Instrument instrument = new Instrument();
        instrument.setBusinesskey("test");
        instrument.setInstrumentType(InstrumentType.EQUITY);
        instrument.setDescription("test");
        Mockito.when(reader.findInstrumentByBusinesskey("test")).thenReturn(Mono.just(instrument));
        Mockito.when(reader.findSecurityMetricsByBusinesskey("test")).thenReturn(Mono.just(securityMetrics));
        Mockito.when(reader.findPriceByBusinesskey("test")).thenReturn(Mono.just(new EndOfDayPrice(100.0, "EUR")));
        Mockito.when(reader.findInstrumentByBusinesskey("EUR")).thenReturn(Mono.just(new Instrument()));


        var result = securityMetricsService.recalcSecurityMetrics("test").block();
        assertEquals(0.0, result.getAvgFreeCashflowGrowth5Y(), 0.001);
    }

    @Test
    void testRecalcSecurityMetrics_avgFcfGrowth_withZero() {
        var securityMetrics = new SecurityMetrics();
        securityMetrics.setBusinesskey("test");
        securityMetrics.setCurrencyKey("EUR");
        securityMetrics.setFiscalEndDate(LocalDate.now());
        Map<Integer, Double> fcf = new HashMap<>();
        fcf.put(2022, 120.0);
        fcf.put(2023, 0.0);
        fcf.put(2024, 140.0);
        securityMetrics.setHistoricalFreeCashflow(fcf);

        Instrument instrument = new Instrument();
        instrument.setBusinesskey("test");
        instrument.setInstrumentType(InstrumentType.EQUITY);
        instrument.setDescription("test");
        Mockito.when(reader.findInstrumentByBusinesskey("test")).thenReturn(Mono.just(instrument));
        Mockito.when(reader.findSecurityMetricsByBusinesskey("test")).thenReturn(Mono.just(securityMetrics));
        Mockito.when(reader.findPriceByBusinesskey("test")).thenReturn(Mono.just(new EndOfDayPrice(100.0, "EUR")));
        Mockito.when(reader.findInstrumentByBusinesskey("EUR")).thenReturn(Mono.just(new Instrument()));


        var result = securityMetricsService.recalcSecurityMetrics("test").block();
        assertEquals(-1.0, result.getAvgFreeCashflowGrowth5Y(), 0.001);
    }

    @Test
    void testRecalcSecurityMetrics_intrinsicValue_withoutExpectedFcf() {
        var securityMetrics = new SecurityMetrics();
        securityMetrics.setBusinesskey("test");
        securityMetrics.setCurrencyKey("EUR");
        securityMetrics.setFiscalEndDate(LocalDate.now());
        securityMetrics.setSharesOutstanding(1000.0);
        securityMetrics.setAvgMarktcapFreeCashflowRatio(10.0);
        securityMetrics.setOperatingCashflow(1000.0 );
        securityMetrics.setCapitalExpenditures(0.0);
        
        Map<Integer, Double> expectedFcf = new HashMap<>();
        for(int i=0; i<10; i++) {
            expectedFcf.put(i+1, 1000.0 *Math.pow(1.1, i));
        }
        securityMetrics.setExpectedFreeCashflow(1000.0);
        securityMetrics.setExpectedCashflowGrowth(1.1);


        Instrument instrument = new Instrument();
        instrument.setBusinesskey("test");
        instrument.setInstrumentType(InstrumentType.EQUITY);
        instrument.setDescription("test");
        Mockito.when(reader.findInstrumentByBusinesskey("test")).thenReturn(Mono.just(instrument));
        Mockito.when(reader.findSecurityMetricsByBusinesskey("test")).thenReturn(Mono.just(securityMetrics));
        Mockito.when(reader.findPriceByBusinesskey("test")).thenReturn(Mono.just(new EndOfDayPrice(100.0, "EUR")));
        Mockito.when(reader.findInstrumentByBusinesskey("EUR")).thenReturn(Mono.just(new Instrument()));


        var result = securityMetricsService.recalcSecurityMetrics("test").block();
        assertEquals(20, result.getIntrinsicValue(), 0.001);
    }

    @Test
    void testRecalcSecurityMetrics_intrinsicValue_withExpectedFcf() {
                var securityMetrics = new SecurityMetrics();
        securityMetrics.setBusinesskey("test");
        securityMetrics.setCurrencyKey("EUR");
        securityMetrics.setFiscalEndDate(LocalDate.now());
        securityMetrics.setSharesOutstanding(1000.0);
        securityMetrics.setAvgMarktcapFreeCashflowRatio(10.0);
        securityMetrics.setOperatingCashflow(1000.0 );
        securityMetrics.setCapitalExpenditures(0.0);
        
        Map<Integer, Double> expectedFcf = new HashMap<>();
        for(int i=0; i<10; i++) {
            expectedFcf.put(i+1, 1000.0 *Math.pow(1.1, i+1));
        }
        securityMetrics.setExpectedFreeCashflowPerYear(expectedFcf);
        securityMetrics.setExpectedFreeCashflow(1000.0);
        securityMetrics.setExpectedCashflowGrowth(1.1);


        Instrument instrument = new Instrument();
        instrument.setBusinesskey("test");
        instrument.setInstrumentType(InstrumentType.EQUITY);
        instrument.setDescription("test");
        Mockito.when(reader.findInstrumentByBusinesskey("test")).thenReturn(Mono.just(instrument));
        Mockito.when(reader.findSecurityMetricsByBusinesskey("test")).thenReturn(Mono.just(securityMetrics));
        Mockito.when(reader.findPriceByBusinesskey("test")).thenReturn(Mono.just(new EndOfDayPrice(100.0, "EUR")));
        Mockito.when(reader.findInstrumentByBusinesskey("EUR")).thenReturn(Mono.just(new Instrument()));


        var result = securityMetricsService.recalcSecurityMetrics("test").block();
        assertEquals(20, result.getIntrinsicValue(), 0.001);
    }
}