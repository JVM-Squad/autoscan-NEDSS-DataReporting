package gov.cdc.etldatapipeline.investigation.repository.model.reporting;

import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import lombok.Data;

@Data
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
public class InvestigationObservationKey {
    private Long publicHealthCaseUid;
    private Long observationId;
    private Long branchId;
}
