package de.hf.myfinance.securitymetrics.persistence.entities;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Map;

import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Version;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;

import de.hf.myfinance.restmodel.RiskProfile;
import de.hf.myfinance.restmodel.SecurityLifecyclePhase;


@Document(collection = "SecurityMetrics")
public class SecurityMetricsEntity {

    @Id
    private String id;
    @Version
    private Integer version;
    @Indexed(unique = true)
    String businesskey;
    String description;
    // the currency code of the security, e.g. EUR, USD, GBP just for display purposes
    String currencyCode;
    //the businesskey of the currency to identify the value curve of the currency and calculate the price in Euro
    String currencyKey;
    LocalDate fiscalEndDate;
    SecurityLifecyclePhase securityLifecyclePhase;
    RiskProfile riskProfile;
    String sector;
    String country;
    LocalDateTime lastUpdateTs;
    LocalDateTime lastManualReviewTs;


    // all values are yearly TTM values
    //mandatory
    Double price;
    Double priceInEuro;
    Double sharesOutstanding;
    Double revenue;
    Double capitalExpenditures;
    Double operatingCashflow;
    Double netIncome;

    //optional
    Double totalAssets;
    Double totalLiabilities;
    Double shortLongTermDebtTotal;
    Double totalCash;
    Double dilutedEPS5Y;
    Double dividendPerShare;
    Double forwardFreeCashflow5YCAGR;
    Double forwardPriceToSales;
    Double beta;
    Double tam;
    Double forwardPE;
    Double goodwill; 
    Double ebitda;
    Double ebit;
    Double grossProfit;
    Double totalEquity;
    Double currentLiabilities;

    //calculated
    Double freeCashflow;
    //expected free cashflow, baseline for the intrinsic value calculation, it is the average of the last 5 years free cashflow or the last year free cashflow if not enough historical data is available or in case the FCF is bigger than the avg
    Double expectedFreeCashflow;
    //average of the last 5 years free cashflow
    Double avgFreeCashflow5Y;
    //average growth of the free cashflow in the last 5 years
    Double avgFreeCashflowGrowth5Y;
    Double pe;
    Double roa;
    //return on equity
    Double roe;
    //ReturnOnCapitalEmployed (ROCE) = EBIT/(Assets-currentLiabilities)
    Double roce;
    Double debtToAssets;
    Double dividendYield;
    Double dividendPayoutRatio;
    Double intrinsicValue;
    Double intrinsicValueMargin;
    //intrinsic value / enterprice value per share
    Double intrinsicValueEVMargin;
    Double lynchScore;
    Double revenueGrowthRate;
    Double eps;
    Double ruleOfFourty;
    Double grossMargin;
    Double pricePerSales;
    Double fcfMargin;

    //config
    Double avgMarktcapFreeCashflowRatio;
    //durchschnitt in den nächsten 10 Jahren erwartetes Wachstum des Free Cashflows
    //this is used to calculate the intrinsic value
    //it is the average of next 10 years of the FreeCashflow growth rate
    // notation e.g. 1.4 means 40% growth
    Double expectedCashflowGrowth;
    Double expectedFreeCashflowOverride;

    //historical map<year of fiscalaenddate, value>. fiscalaenddate is a Date, the values are TTM(trailing twelve month) values
    Map<Integer, Double> historicalRevenue;
    Map<Integer, Double> historicalNetIncome;
    Map<Integer, Double> historicalFreeCashflow;
    Map<Integer, Double> expectedFreeCashflowGrowthPerYear;// the keys are 1..10 for the next 10 years. Year 1 is next year, so the value is the expectedFreeCashflowfor this year (or the current FCF)* (1+expectedCashflowGrowth)


    //ranks
    Integer rankByPE;
    Integer rankByRoA;
    Integer rankByRoAAndPE;
    Integer rankByIntrinsicValueMargin;
    Integer rankByLynchScore;
    Integer rankByLynchAndIntrinsicValueMargin;

    public String getId() {
        return this.id;
    }
    public void setId(String id) {
        this.id = id;
    }

    public Integer getVersion() {
        return version;
    }
    public void setVersion(Integer version) {
        this.version = version;
    }
    
    public String getBusinesskey() {
        return this.businesskey;
    }

    public void setBusinesskey(String businesskey) {
        this.businesskey = businesskey;
    }

    public String getCurrencyCode() {
        return this.currencyCode;
    }

    public void setCurrencyCode(String currencyCode) {
        this.currencyCode = currencyCode;
    }

    public LocalDate getFiscalEndDate() {
        return this.fiscalEndDate;
    }

    public void setFiscalEndDate(LocalDate fiscalEndDate) {
        this.fiscalEndDate = fiscalEndDate;
    }

    public SecurityLifecyclePhase getSecurityLifecyclePhase() {
        return this.securityLifecyclePhase;
    }

    public void setSecurityLifecyclePhase(SecurityLifecyclePhase securityLifecyclePhase) {
        this.securityLifecyclePhase = securityLifecyclePhase;
    }

    public RiskProfile getRiskProfile() {
        return this.riskProfile;
    }

    public void setRiskProfile(RiskProfile riskProfile) {
        this.riskProfile = riskProfile;
    }

    public String getSector() {
        return this.sector;
    }

    public void setSector(String sector) {
        this.sector = sector;
    }

    public Double getPrice() {
        return this.price;
    }

    public void setPrice(Double price) {
        this.price = price;
    }

    public Double getSharesOutstanding() {
        return this.sharesOutstanding;
    }

    public void setSharesOutstanding(Double sharesOutstanding) {
        this.sharesOutstanding = sharesOutstanding;
    }

    public Double getRevenue() {
        return this.revenue;
    }

    public void setRevenue(Double revenue) {
        this.revenue = revenue;
    }

    public Double getEps() {
        return this.eps;
    }

    public void setEps(Double eps) {
        this.eps = eps;
    }

    public Double getFreeCashflow() {
        return this.freeCashflow;
    }

    public void setFreeCashflow(Double freeCashflow) {
        this.freeCashflow = freeCashflow;
    }

    public Double getTotalAssets() {
        return this.totalAssets;
    }

    public void setTotalAssets(Double totalAssets) {
        this.totalAssets = totalAssets;
    }

    public Double getTotalLiabilities() {
        return this.totalLiabilities;
    }

    public void setTotalLiabilities(Double totalLiabilities) {
        this.totalLiabilities = totalLiabilities;
    }

    public Double getDilutedEPS5Y() {
        return this.dilutedEPS5Y;
    }

    public void setDilutedEPS5Y(Double dilutedEPS5Y) {
        this.dilutedEPS5Y = dilutedEPS5Y;
    }

    public Double getDividendPerShare() {
        return this.dividendPerShare;
    }

    public void setDividendPerShare(Double dividendPerShare) {
        this.dividendPerShare = dividendPerShare;
    }

    public Double getForwardFreeCashflow5YCAGR() {
        return this.forwardFreeCashflow5YCAGR;
    }

    public void setForwardFreeCashflow5YCAGR(Double forwardFreeCashflow5YCAGR) {
        this.forwardFreeCashflow5YCAGR = forwardFreeCashflow5YCAGR;
    }

    public Double getForwardPriceToSales() {
        return this.forwardPriceToSales;
    }

    public void setForwardPriceToSales(Double forwardPriceToSales) {
        this.forwardPriceToSales = forwardPriceToSales;
    }

    public Double getBeta() {
        return this.beta;
    }

    public void setBeta(Double beta) {
        this.beta = beta;
    }

    public Double getTam() {
        return this.tam;
    }

    public void setTam(Double tam) {
        this.tam = tam;
    }

    public Double getForwardPE() {
        return this.forwardPE;
    }

    public void setForwardPE(Double forwardPE) {
        this.forwardPE = forwardPE;
    }

    public Double getNetIncome() {
        return this.netIncome;
    }

    public void setNetIncome(Double netIncome) {
        this.netIncome = netIncome;
    }

    public Double getPe() {
        return this.pe;
    }

    public void setPe(Double pe) {
        this.pe = pe;
    }

    public Double getRoa() {
        return this.roa;
    }

    public void setRoa(Double roa) {
        this.roa = roa;
    }

    public Double getDebtToAssets() {
        return this.debtToAssets;
    }

    public void setDebtToAssets(Double debtToAssets) {
        this.debtToAssets = debtToAssets;
    }

    public Double getDividendYield() {
        return this.dividendYield;
    }

    public void setDividendYield(Double dividendYield) {
        this.dividendYield = dividendYield;
    }

    public Double getDividendPayoutRatio() {
        return this.dividendPayoutRatio;
    }

    public void setDividendPayoutRatio(Double dividendPayoutRatio) {
        this.dividendPayoutRatio = dividendPayoutRatio;
    }

    public Double getIntrinsicValue() {
        return this.intrinsicValue;
    }

    public void setIntrinsicValue(Double intrinsicValue) {
        this.intrinsicValue = intrinsicValue;
    }

    public Double getIntrinsicValueMargin() {
        return this.intrinsicValueMargin;
    }

    public void setIntrinsicValueMargin(Double intrinsicValueMargin) {
        this.intrinsicValueMargin = intrinsicValueMargin;
    }

    public Double getLynchScore() {
        return this.lynchScore;
    }

    public void setLynchScore(Double lynchScore) {
        this.lynchScore = lynchScore;
    }

    public Double getRevenueGrowthRate() {
        return this.revenueGrowthRate;
    }

    public void setRevenueGrowthRate(Double revenueGrowthRate) {
        this.revenueGrowthRate = revenueGrowthRate;
    }

    public Integer getRankByPE() {
        return this.rankByPE;
    }

    public void setRankByPE(Integer rankByPE) {
        this.rankByPE = rankByPE;
    }

    public Integer getRankByRoA() {
        return this.rankByRoA;
    }

    public void setRankByRoA(Integer rankByRoA) {
        this.rankByRoA = rankByRoA;
    }

    public Integer getRankByRoAAndPE() {
        return this.rankByRoAAndPE;
    }

    public void setRankByRoAAndPE(Integer rankByRoAAndPE) {
        this.rankByRoAAndPE = rankByRoAAndPE;
    }

    public Integer getRankByIntrinsicValueMargin() {
        return this.rankByIntrinsicValueMargin;
    }

    public void setRankByIntrinsicValueMargin(Integer rankByIntrinsicValueMargin) {
        this.rankByIntrinsicValueMargin = rankByIntrinsicValueMargin;
    }

    public Integer getRankByLynchScore() {
        return this.rankByLynchScore;
    }

    public void setRankByLynchScore(Integer rankByLynchScore) {
        this.rankByLynchScore = rankByLynchScore;
    }

    public Integer getRankByLynchAndIntrinsicValueMargin() {
        return this.rankByLynchAndIntrinsicValueMargin;
    }

    public void setRankByLynchAndIntrinsicValueMargin(Integer rankByLynchAndIntrinsicValueMargin) {
        this.rankByLynchAndIntrinsicValueMargin = rankByLynchAndIntrinsicValueMargin;
    }
    

    public String getDescription() {
        return this.description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public LocalDateTime getLastUpdateTs() {
        return this.lastUpdateTs;
    }

    public void setLastUpdateTs(LocalDateTime lastUpdateTs) {
        this.lastUpdateTs = lastUpdateTs;
    }


    public String getCurrencyKey() {
        return this.currencyKey;
    }

    public void setCurrencyKey(String currencyKey) {
        this.currencyKey = currencyKey;
    }

    public Double getPriceInEuro() {
        return this.priceInEuro;
    }

    public void setPriceInEuro(Double priceInEuro) {
        this.priceInEuro = priceInEuro;
    }

    public Double getCapitalExpenditures() {
        return this.capitalExpenditures;
    }

    public void setCapitalExpenditures(Double capitalExpenditures) {
        this.capitalExpenditures = capitalExpenditures;
    }

    public Double getOperatingCashflow() {
        return this.operatingCashflow;
    }

    public void setOperatingCashflow(Double operatingCashflow) {
        this.operatingCashflow = operatingCashflow;
    }
     

    public Double getAvgMarktcapFreeCashflowRatio() {
        return this.avgMarktcapFreeCashflowRatio;
    }

    public void setAvgMarktcapFreeCashflowRatio(Double avgMarktcapFreeCashflowRatio) {
        this.avgMarktcapFreeCashflowRatio = avgMarktcapFreeCashflowRatio;
    }

    public Double getExpectedCashflowGrowth() {
        return this.expectedCashflowGrowth;
    }

    public void setExpectedCashflowGrowth(Double expectedCashflowGrowth) {
        this.expectedCashflowGrowth = expectedCashflowGrowth;
    }

    public Double getExpectedFreeCashflow() {
        return this.expectedFreeCashflow;
    }

    public void setExpectedFreeCashflow(Double expectedFreeCashflow) {
        this.expectedFreeCashflow = expectedFreeCashflow;
    }

    public Double getAvgFreeCashflow5Y() {
        return this.avgFreeCashflow5Y;
    }

    public void setAvgFreeCashflow5Y(Double avgFreeCashflow5Y) {
        this.avgFreeCashflow5Y = avgFreeCashflow5Y;
    }

    public Double getAvgFreeCashflowGrowth5Y() {
        return this.avgFreeCashflowGrowth5Y;
    }

    public void setAvgFreeCashflowGrowth5Y(Double avgFreeCashflowGrowth5Y) {
        this.avgFreeCashflowGrowth5Y = avgFreeCashflowGrowth5Y;
    }

    public Map<Integer,Double> getExpectedFreeCashflowGrowthPerYear() {
        return this.expectedFreeCashflowGrowthPerYear;
    }

    public void setExpectedFreeCashflowGrowthPerYear(Map<Integer,Double> expectedFreeCashflowGrowthPerYear) {
        this.expectedFreeCashflowGrowthPerYear = expectedFreeCashflowGrowthPerYear;
    }

    public Map<Integer,Double> getHistoricalRevenue() {
        return this.historicalRevenue;
    }

    public void setHistoricalRevenue(Map<Integer,Double> historicalRevenue) {
        this.historicalRevenue = historicalRevenue;
    }

    public Map<Integer,Double> getHistoricalNetIncome() {
        return this.historicalNetIncome;
    }

    public void setHistoricalNetIncome(Map<Integer,Double> historicalNetIncome) {
        this.historicalNetIncome = historicalNetIncome;
    }

    public Map<Integer,Double> getHistoricalFreeCashflow() {
        return this.historicalFreeCashflow;
    }

    public void setHistoricalFreeCashflow(Map<Integer,Double> historicalFreeCashflow) {
        this.historicalFreeCashflow = historicalFreeCashflow;
    }

    public Double getTotalCash() {
        return this.totalCash;
    }

    public void setTotalCash(Double totalCash) {
        this.totalCash = totalCash;
    }

    public Double getIntrinsicValueEVMargin() {
        return this.intrinsicValueEVMargin;
    }

    public void setIntrinsicValueEVMargin(Double intrinsicValueEVMargin) {
        this.intrinsicValueEVMargin = intrinsicValueEVMargin;
    }


    public Double getShortLongTermDebtTotal() {
        return this.shortLongTermDebtTotal;
    }

    public void setShortLongTermDebtTotal(Double shortLongTermDebtTotal) {
        this.shortLongTermDebtTotal = shortLongTermDebtTotal;
    }


    public String getCountry() {
        return this.country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public Double getGoodwill() {
        return this.goodwill;
    }

    public void setGoodwill(Double goodwill) {
        this.goodwill = goodwill;
    }

    public Double getEbitda() {
        return this.ebitda;
    }

    public void setEbitda(Double ebitda) {
        this.ebitda = ebitda;
    }

    public Double getEbit() {
        return this.ebit;
    }

    public void setEbit(Double ebit) {
        this.ebit = ebit;
    }

    public Double getGrossProfit() {
        return this.grossProfit;
    }

    public void setGrossProfit(Double grossProfit) {
        this.grossProfit = grossProfit;
    }

    public Double getTotalEquity() {
        return this.totalEquity;
    }

    public void setTotalEquity(Double totalEquity) {
        this.totalEquity = totalEquity;
    }

    public Double getCurrentLiabilities() {
        return this.currentLiabilities;
    }

    public void setCurrentLiabilities(Double currentLiabilities) {
        this.currentLiabilities = currentLiabilities;
    }

    public Double getRoe() {
        return this.roe;
    }

    public void setRoe(Double roe) {
        this.roe = roe;
    }

    public Double getRoce() {
        return this.roce;
    }

    public void setRoce(Double roce) {
        this.roce = roce;
    }

    public Double getRuleOfFourty() {
        return this.ruleOfFourty;
    }

    public void setRuleOfFourty(Double ruleOfFourty) {
        this.ruleOfFourty = ruleOfFourty;
    }

    public Double getGrossMargin() {
        return this.grossMargin;
    }

    public void setGrossMargin(Double grossMargin) {
        this.grossMargin = grossMargin;
    }

    public Double getPricePerSales() {
        return this.pricePerSales;
    }

    public void setPricePerSales(Double pricePerSales) {
        this.pricePerSales = pricePerSales;
    }

    public Double getFcfMargin() {
        return this.fcfMargin;
    }

    public void setFcfMargin(Double fcfMargin) {
        this.fcfMargin = fcfMargin;
    }


    public Double getExpectedFreeCashflowOverride() {
        return this.expectedFreeCashflowOverride;
    }

    public void setExpectedFreeCashflowOverride(Double expectedFreeCashflowOverride) {
        this.expectedFreeCashflowOverride = expectedFreeCashflowOverride;
    }

    public LocalDateTime getLastManualReviewTs() {
        return this.lastManualReviewTs;
    }

    public void setLastManualReviewTs(LocalDateTime lastManualReviewTs) {
        this.lastManualReviewTs = lastManualReviewTs;
    }

}
