package de.hf.myfinance.securitymetrics.persistence.repositories;

import org.springframework.data.repository.reactive.ReactiveCrudRepository;

import de.hf.myfinance.securitymetrics.persistence.entities.PriceEntity;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface PriceRepository extends ReactiveCrudRepository<PriceEntity, String> {
    Mono<PriceEntity> findByBusinesskey(String businesskey);
    Flux<PriceEntity> findByBusinesskeyIn(Iterable<String> businesskeyIterable);
    Mono<Long> deleteByBusinesskey(String businesskey);
}
