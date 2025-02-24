package gov.cdc.etldatapipeline.investigation.repository.model.reporting;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import lombok.Data;
import java.time.LocalDateTime;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
public class TreatmentReporting {
    private String treatmentUid;
    private String publicHealthCaseUid;
    private String organizationUid;
    private String providerUid;
    private String patientTreatmentUid;
    private String treatmentName;
    private String treatmentOid;
    private String treatmentComments;
    private String treatmentSharedInd;
    private String cd;
    private String treatmentDate;
    private String treatmentDrug;
    private String treatmentDrugName;
    private String treatmentDosageStrength;
    private String treatmentDosageStrengthUnit;
    private String treatmentFrequency;
    private String treatmentDuration;
    private String treatmentDurationUnit;
    private String treatmentRoute;
    private String localId;
    private String recordStatusCd;
    private String addTime;
    private String addUserId;
    private String lastChangeTime;
    private String lastChangeUserId;
    private String versionControlNumber;
}