create or alter view dbo.v_common_inv_keys as
select
	inv.public_health_case_uid,
	i.investigation_key as INVESTIGATION_KEY,
	con.condition_key as CONDITION_KEY,
	coalesce(dpat.patient_key,1) as patient_key,
	coalesce(dpro1.provider_key,	1) as Investigator_key,
	coalesce(dpro2.provider_key,	1) as Physician_key,
	coalesce(dpro3.provider_key,	1) as Reporter_key,
	coalesce(dorg1.organization_key,	1) as Rpt_Src_Org_key,
	coalesce(dorg2.Organization_key,	1) as ADT_HSPTL_KEY,
	coalesce(rd1.Date_key,	1) as Inv_Assigned_dt_key,
    coalesce(rd2.Date_key, 1) as INV_START_DT_KEY,
    coalesce(rd3.Date_key, 1) as DIAGNOSIS_DT_KEY,
    coalesce(rd4.Date_key, 1) as INV_RPT_DT_KEY,
    1 AS GEOCODING_LOCATION_KEY,
	COALESCE(lg.ldf_group_key, 1) as LDF_GROUP_KEY
from
	dbo.nrt_investigation inv
inner join dbo.INVESTIGATION i with(nolock) on
	inv.public_health_case_uid = i.case_uid
inner join dbo.CONDITION con with(nolock) on
	con.condition_cd = inv.CD
left outer join dbo.LDF_GROUP lg on
	lg.BUSINESS_OBJECT_UID = inv.public_health_case_uid
left outer join dbo.D_PATIENT dpat with(nolock) on
	inv.patient_id = dpat.patient_uid
left outer join dbo.D_PROVIDER dpro1 with(nolock) on
	inv.investigator_id = dpro1.provider_uid
left outer join dbo.D_PROVIDER dpro2 with(nolock) on
	inv.physician_id = dpro2.provider_uid
left outer join dbo.D_PROVIDER dpro3 with(nolock) on
	inv.person_as_reporter_uid = dpro3.provider_uid
left outer join dbo.D_ORGANIZATION dorg1 with(nolock) on
	inv.organization_id = dorg1.organization_uid
left outer join dbo.D_ORGANIZATION dorg2 with(nolock) on
	inv.hospital_uid = dorg2.organization_uid
left outer join dbo.RDB_DATE rd1 with(nolock) on
	inv.investigator_assigned_datetime = rd1.DATE_MM_DD_YYYY
left outer join dbo.RDB_DATE rd2 with(nolock) on
	inv.activity_to_time = rd2.DATE_MM_DD_YYYY
left outer join dbo.RDB_DATE rd3 with(nolock) on
	inv.diagnosis_time = rd3.DATE_MM_DD_YYYY
left outer join dbo.RDB_DATE rd4 with(nolock) on
	inv.rpt_form_cmplt_time = rd4.DATE_MM_DD_YYYY;