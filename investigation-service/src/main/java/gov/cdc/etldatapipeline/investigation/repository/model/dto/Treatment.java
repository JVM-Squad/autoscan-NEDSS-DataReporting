package gov.cdc.etldatapipeline.investigation.repository.model.dto;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import lombok.Data;

@Entity
@Data
public class Treatment {
    @Id
    @Column(name = "treatment_uid")
    private String treatmentUid;

    @Column(name = "public_health_case_uid")
    private String publicHealthCaseUid;

    @Column(name = "organization_uid")
    private String organizationUid;

    @Column(name = "provider_uid")
    private String providerUid;

    @Column(name = "patient_treatment_uid")
    private String patientTreatmentUid;

    @Column(name = "Treatment_nm")
    private String treatmentName;

    @Column(name = "Treatment_oid")
    private String treatmentOid;

    @Column(name = "Treatment_comments")
    private String treatmentComments;

    @Column(name = "Treatment_shared_ind")
    private String treatmentSharedInd;

    @Column(name = "cd")
    private String cd;

    @Column(name = "Treatment_dt")
    private String treatmentDate;

    @Column(name = "Treatment_drug")
    private String treatmentDrug;

    @Column(name = "Treatment_drug_nm")
    private String treatmentDrugName;

    @Column(name = "Treatment_dosage_strength")
    private String treatmentDosageStrength;

    @Column(name = "Treatment_dosage_strength_unit")
    private String treatmentDosageStrengthUnit;

    @Column(name = "Treatment_frequency")
    private String treatmentFrequency;

    @Column(name = "Treatment_duration")
    private String treatmentDuration;

    @Column(name = "Treatment_duration_unit")
    private String treatmentDurationUnit;

    @Column(name = "Treatment_route")
    private String treatmentRoute;

    @Column(name = "LOCAL_ID")
    private String localId;

    @Column(name = "record_status_cd")
    private String recordStatusCd;

    @Column(name = "ADD_TIME")
    private String addTime;

    @Column(name = "ADD_USER_ID")
    private String addUserId;

    @Column(name = "LAST_CHG_TIME")
    private String lastChangeTime;

    @Column(name = "LAST_CHG_USER_ID")
    private String lastChangeUserId;

    @Column(name = "VERSION_CTRL_NBR")
    private String versionControlNumber;

    @Column(name = "refresh_datetime")
    private String refreshDatetime;

    @Column(name = "max_datetime")
    private String maxDatetime;
}
