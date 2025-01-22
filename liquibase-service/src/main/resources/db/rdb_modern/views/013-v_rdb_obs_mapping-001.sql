CREATE OR ALTER VIEW dbo.v_rdb_obs_mapping
AS
SELECT  imrdb.RDB_table,
        imrdb.unique_cd,
        imrdb.RDB_attribute as col_nm,
        imrdb.db_field,
        COALESCE(ovc.public_health_case_uid, ovn.public_health_case_uid, ovt.public_health_case_uid, ovd.public_health_case_uid) as public_health_case_uid,
        COALESCE(ovc.observation_id, ovn.observation_id, ovt.observation_id, ovd.observation_id) as root_observation_uid,
        COALESCE(ovc.branch_id, ovn.branch_id, ovt.branch_id, ovd.branch_id) as branch_id,
        CASE
            WHEN imrdb.DB_field = 'code' then ovc.response
            ELSE NULL
        END AS coded_response,
        CASE
            WHEN imrdb.DB_field = 'numeric_value_1' then ovn.response
            ELSE NULL
        END AS numeric_response,
        CASE
            WHEN imrdb.DB_field = 'value_txt' then ovt.response
            ELSE NULL
        END AS txt_response,
        CASE
            WHEN imrdb.DB_field = 'from_time' then ovd.response
            ELSE NULL
        END AS date_response,
        ovc.label
FROM nbs_srte.dbo.imrdbmapping imrdb
         LEFT JOIN dbo.v_getobscode ovc ON imrdb.unique_cd = ovc.cd
         LEFT JOIN dbo.v_getobsnum ovn ON imrdb.unique_cd = ovn.cd
         LEFT JOIN dbo.v_getobstxt ovt ON imrdb.unique_cd = ovt.cd
         LEFT JOIN dbo.v_getobsdate ovd ON imrdb.unique_cd = ovd.cd;  