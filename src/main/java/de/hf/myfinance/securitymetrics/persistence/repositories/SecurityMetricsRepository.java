package de.hf.myfinance.securitymetrics.persistence.repositories;

import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import de.hf.myfinance.securitymetrics.persistence.entities.SecurityMetricsEntity;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface SecurityMetricsRepository extends ReactiveCrudRepository<SecurityMetricsEntity, String> {
    Mono<SecurityMetricsEntity> findByBusinesskey(String businesskey);
    Flux<SecurityMetricsEntity> findByBusinesskeyIn(Iterable<String> businesskeyIterable);
    Mono<Long> deleteByBusinesskey(String businesskey);
}