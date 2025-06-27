package de.hf.myfinance.securitymetrics.persistence.repositories;

import org.springframework.data.repository.reactive.ReactiveCrudRepository;

import de.hf.myfinance.securitymetrics.persistence.entities.InstrumentEntity;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;


public interface InstrumentRepository extends ReactiveCrudRepository<InstrumentEntity, String> {
    Mono<InstrumentEntity> findByBusinesskey(String businesskey);
    Flux<InstrumentEntity> findByBusinesskeyIn(Iterable<String> businesskeyIterable);
    Mono<Long> deleteByBusinesskey(String businesskey);

    Flux<InstrumentEntity> findByActive(boolean active);
}
