package de.hf.myfinance.securitymetrics;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mongodb.core.ReactiveMongoOperations;
import org.springframework.data.mongodb.core.index.IndexResolver;
import org.springframework.data.mongodb.core.index.MongoPersistentEntityIndexResolver;
import org.springframework.data.mongodb.core.index.ReactiveIndexOperations;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;
import org.springframework.web.client.RestTemplate;

import de.hf.myfinance.securitymetrics.persistence.entities.InstrumentEntity;
import de.hf.myfinance.securitymetrics.persistence.entities.PriceEntity;
import de.hf.myfinance.securitymetrics.persistence.entities.SecurityMetricsEntity;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.OpenAPI;

@SpringBootApplication
@ComponentScan("de.hf")
public class SecurityMetricsServiceApplication {
	@Value("${api.common.version}")         String apiVersion;
	@Value("${api.common.title}")           String apiTitle;
	@Value("${api.common.description}")     String apiDescription;

	@Value("${app.threadPoolSize:10}")      Integer threadPoolSize;
	@Value("${app.taskQueueSize:100}")      Integer taskQueueSize;

	private static final Logger LOG = LoggerFactory.getLogger(SecurityMetricsServiceApplication.class);

	@Bean
	RestTemplate restTemplate() {
		return new RestTemplate();
	}

	@Bean
	public OpenAPI getOpenApiDocumentation() {
		return new OpenAPI()
				.info(new Info().title(apiTitle)
						.description(apiDescription)
						.version(apiVersion));
	}

	@Bean
	public Scheduler publishEventScheduler() {
		return Schedulers.newBoundedElastic(threadPoolSize, taskQueueSize, "publish-pool");
	}

	public static void main(String[] args) {
		ConfigurableApplicationContext ctx = SpringApplication.run(SecurityMetricsServiceApplication.class, args);
		String mongodDbHost = ctx.getEnvironment().getProperty("spring.data.mongodb.host");
		String mongodDbPort = ctx.getEnvironment().getProperty("spring.data.mongodb.port");
		var msg = String.format("Connected to MongoDb: %s : %s", mongodDbHost, mongodDbPort);
		LOG.info(msg);
	}

	@Autowired
	ReactiveMongoOperations mongoTemplate;

	@EventListener(ContextRefreshedEvent.class)
	public void initIndicesAfterStartup() {

		MappingContext<? extends MongoPersistentEntity<?>, MongoPersistentProperty> mappingContext = mongoTemplate.getConverter().getMappingContext();
		IndexResolver resolver = new MongoPersistentEntityIndexResolver(mappingContext);

		ReactiveIndexOperations indexInstrument = mongoTemplate.indexOps(InstrumentEntity.class);
		resolver.resolveIndexFor(InstrumentEntity.class).forEach(indexInstrument::ensureIndex);

		ReactiveIndexOperations indexPrice = mongoTemplate.indexOps(PriceEntity.class);
		resolver.resolveIndexFor(PriceEntity.class).forEach(indexPrice::ensureIndex);

		ReactiveIndexOperations indexSecurityMetrics = mongoTemplate.indexOps(SecurityMetricsEntity.class);
		resolver.resolveIndexFor(SecurityMetricsEntity.class).forEach(indexSecurityMetrics::ensureIndex);
	}

}