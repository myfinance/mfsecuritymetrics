package de.hf.myfinance.securitymetrics.persistence;

import de.hf.myfinance.restmodel.SecurityMetrics;
import de.hf.myfinance.securitymetrics.persistence.entities.SecurityMetricsEntity;

import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.Mappings;

import java.util.List;

@Mapper(componentModel = "spring")
public interface SecurityMetricsMapper {

    SecurityMetrics entityToApi(SecurityMetricsEntity entity);

    @Mappings({
            @Mapping(target = "id", ignore = true)
    })
    SecurityMetricsEntity apiToEntity(SecurityMetrics api);

    List<SecurityMetrics> entityListToApiList(List<SecurityMetricsEntity> entity);

    List<SecurityMetricsEntity> apiListToEntityList(List<SecurityMetrics> api);

    default SecurityMetrics createInstrument() {
        return new SecurityMetrics();
    }
}