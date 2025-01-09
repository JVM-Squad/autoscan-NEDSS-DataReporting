CREATE OR ALTER PROCEDURE dbo.sp_crs_case_datamart_postprocessing @inv_uids nvarchar(max),
                                                                  @debug bit = 'false'
as

BEGIN

    DECLARE
        @RowCount_no INT;
    DECLARE
        @Proc_Step_no FLOAT = 0;
    DECLARE
        @Proc_Step_Name VARCHAR(200) = '';
    DECLARE
        @batch_id BIGINT;
    SET
        @batch_id = cast((format(getdate(), 'yyyyMMddHHmmss')) as bigint);
    DECLARE
        @dynamic_columns NVARCHAR(MAX) = '';
    DECLARE
        @dynamic_join NVARCHAR(MAX) = '';

    BEGIN TRY

        SET @Proc_Step_no = 1;
        SET
            @Proc_Step_Name = 'SP_Start';

        BEGIN
            TRANSACTION;

        INSERT INTO dbo.job_flow_log ( batch_id
                                     , [Dataflow_Name]
                                     , [package_Name]
                                     , [Status_Type]
                                     , [step_number]
                                     , [step_name]
                                     , [row_count]
                                     , [Msg_Description1])
        VALUES ( @batch_id
               , 'CRS_CASE_DATAMART'
               , 'CRS_CASE_DATAMART'
               , 'START'
               , @Proc_Step_no
               , @Proc_Step_Name
               , 0
               , LEFT('ID List-' + @inv_uids, 500));

        COMMIT TRANSACTION;

        BEGIN TRANSACTION
            SET
                @PROC_STEP_NO = @PROC_STEP_NO + 1;
            SET
                @PROC_STEP_NAME = ' GENERATING #CRS_CASE_INIT';

            select inv.public_health_case_uid,
                   i.INVESTIGATION_KEY,
                   i.CONDITION_KEY,
                   i.patient_key,
                   i.Investigator_key,
                   i.Physician_key,
                   i.Reporter_key,
                   i.Rpt_Src_Org_key,
                   i.ADT_HSPTL_KEY,
                   i.Inv_Assigned_dt_key,
                   i.LDF_GROUP_KEY,
                   i.GEOCODING_LOCATION_KEY
            INTO #CRS_CASE_INIT
            from dbo.nrt_investigation inv
                     inner join dbo.v_common_inv_keys i
                                on inv.public_health_case_uid = i.public_health_case_uid

            where inv.public_health_case_uid in (SELECT value FROM STRING_SPLIT(@inv_uids, ','))
              AND inv.investigation_form_cd LIKE 'INV_FORM_CRS%';

            if
                @debug = 'true'
                select @Proc_Step_Name as step, *
                from #CRS_CASE_INIT;

            SELECT @RowCount_no = @@ROWCOUNT;


            INSERT INTO [dbo].[job_flow_log]
            (batch_id, [Dataflow_Name], [package_Name], [Status_Type], [step_number], [step_name], [row_count])
            VALUES (@batch_id, 'CRS_CASE_DATAMART', 'CRS_CASE_DATAMART', 'START', @Proc_Step_no, @Proc_Step_Name,
                    @RowCount_no);

        COMMIT TRANSACTION;


        /*
        The coded attributes table exists for coded because the values need to be filtered out before the string split is applied.
        Otherwise, the stored procedure will consistently throw a deadlocking error.
        */
        BEGIN TRANSACTION
            SET
                @PROC_STEP_NO = @PROC_STEP_NO + 1;
            SET
                @PROC_STEP_NAME = ' GENERATING #CODED_ATTRIBUTES';

            SELECT unique_cd,
                   RDB_attribute
            INTO #CODED_ATTRIBUTES
            FROM dbo.v_nrt_srte_imrdbmapping
            WHERE unique_cd IN
                  (
                   'CRS007', /* AUTOPSY_PERFORMED_IND */
                   'CRS009', /* BIRTH_STATE */
                   'CRS011a', /* CHILD_AGE_AT_DIAGNOSIS_UNIT */
                   'CRS014', /* BIRTH_WEIGHT_UNIT */
                   'CRS015', /* CHILD_CATARACTS */
                   'CRS016', /* CHILD_HEARING_LOSS */
                   'CRS017', /* CHILD_HAS_CONGNITAL_HEART_DISEASE */
                   'CRS018', /* CHILD_PATENT_DUCTUS_ARTERIOSUS */
                   'CRS019', /* CHILD_PULMONIC_STENOSIS */
                   'CRS020', /* OTHER_CONGNITAL_HEART_DISS_IND */
                   'CRS022', /* MOTHER_HAS_MACULOPAPULAR_RASH */
                   'CRS024', /* MOTHER_HAD_FEVER */
                   'CRS027', /* MOTHER_ARTHRALGIA_ARTHRITIS */
                   'CRS028', /* MOTHER_HAD_LYMPHADENOPATHY */
                   'CRS030', /* CHILD_CONGNITAL_GLAUCOMA */
                   'CRS031', /* CHILD_PIGMENTARY_RETINOPATHY */
                   'CRS032', /* CHILD_MENTAL_RETARDATION */
                   'CRS033', /* CHILD_MENINGOENCEPHALITIS */
                   'CRS034', /* CHILD_MICROENCEPHALY */
                   'CRS035', /* CHILD_PURPURA */
                   'CRS036', /* CHILD_ENLARGED_SPLEEN */
                   'CRS037', /* CHILD_ENLARGED_LIVER */
                   'CRS038', /* CHILD_RADIOLUCENT_BONE */
                   'CRS039', /* CHILD_JAUNDICE */
                   'CRS040', /* CHILD_LOW_PLATELETS */
                   'CRS041', /* CHILD_DERMAL_ERYTHROPOISESIS */
                   'CRS042', /* CHILD_OTHER_ABNORMALITIES */
                   'CRS049', /* CHILD_RUBELLA_LAB_TEST_DONE */
                   'CRS050', /* RUBELLA_IGM_EIA_TESTED */
                   'CRS052', /* IGM_EIA_NONCAPTURE_RESULT */
                   'CRS053', /* RUBELLA_IGM_EIA_CAPTURE */
                   'CRS055', /* RUBELLA_IGM_EIA_CAPTURE_RESULT */
                   'CRS056', /* RUBELLA_IGM_OTHER_TEST */
                   'CRS059', /* RUBELLA_IGM_OTHER_TEST_RESULT */
                   'CRS060', /* RUBELLA_IGG_TEST_1 */
                   'CRS062', /* RUBELLA_IGG_TEST_2 */
                   'CRS064', /* DIFFERENCE_BETWEEN_TEST_1_2 */
                   'CRS065', /* VIRUS_ISOLATION_PERFORMED */
                   'CRS067', /* VIRUS_ISOLATION_SPECIMEN_SRC */
                   'CRS069', /* VIRUS_ISOLATION_RESULT */
                   'CRS070', /* RT_PCR_PERFORMED */
                   'CRS072', /* RT_PCR_SRC */
                   'CRS073', /* RT_PCR_RESULT */
                   'CRS074', /* OTHER_RUBELLA_LAB_TEST_DONE */
                   'CRS077', /* SPECIMEN_TO_CDC_FOR_GENOTYPING */
                   'CRS080', /* MOTHER_BIRTH_CNTRY */
                   'CRS085', /* CHILD_18YOUNGR_RUBELLA_VACCD */
                   'CRS087', /* FAMILYPLAND_PRIOR_CONCEPTION */
                   'CRS088', /* PRENATAL_CARE_THIS_PREGNANCY */
                   'CRS090', /* PRENATAL_CARE_OBTAINED_FRM_1, PRENATAL_CARE_OBTAINED_FRM_2, PRENATAL_CARE_OBTAINED_FRM_3 */
                   'CRS091', /* RUBELLA_LIKE_ILL_IN_PREGNANCY */
                   'CRS093', /* DIAGNOSED_BY_PHYSICIAN_IND */
                   'CRS095', /* SEROLOGICAL_CONFIRMED_AT_ILL */
                   'CRS096', /* MOTHER_KNOW_EXPOSED_AT_WHERE */
                   'CRS097', /* MOTHER_RUBELLA_ACQUIRED_PLACE */
                   'CRS098', /* MOTHER_RUBELLA_ACQUIRED_CNTRY */
                   'CRS100', /* MOTHER_UNK_EXPOSURE_TRAVEL_IND */
                   'CRS105', /* MOTHER_EXPOSD_TO_RUBELLA_CASE */
                   'CRS106', /* MOTHER_RELATIONTO_RUBELLA_CASE */
                   'CRS139', /* RUBELLA_IGG_TEST_1_RESULT */
                   'CRS140', /* RUBELLA_IGG_TEST_2_RESULT */
                   'CRS142', /* REASON_NOT_A_CRS_CASE */
                   'CRS147', /* MOTHER_IMMUNIZED_IND */
                   'CRS149', /* MOTHERRUBELLA_IMMUNIZE_INFOSRC */
                   'CRS151', /* VACCINE_SRC */
                   'CRS153', /* MOTHER_GIVEN_PRIOR_BIRTH_IN_US */
                   'CRS161', /* SEROLOGICAL_TST_BEFR_PREGNANCY */
                   'CRS162', /* MOTHER_RUBELLA_ACQUIRED_STATE */
                   'CRS163', /* MOTHER_RUBELLA_ACQUIRED_CNTY */
                   'CRS164', /* MOTHER_TRAVEL_1_TO_CNTRY */
                   'CRS165', /* MOTHER_TRAVEL_2_TO_CNTRY */
                   'CRS172', /* RUBELLA_SPECIMEN_TYPE */
                   'CRS175', /* SEROLOGICALLY_CONFIRMD_RESULT */
                   'CRS176', /* MOTHER_RUBELLA_LAB_TESTING_IND */
                   'CRS177', /* MOTHER_IS_A_RPTD_RUBELLA_CASE */
                   'CRS178', /* IGM_EIA_1_METHOD_USED */
                   'CRS179', /* IGM_EIA_2_METHOD_USED */
                   'CRS180', /* INFANT_DEATH_FRM_CRS */
                   'CRS182', /* GENOTYPE_SEQUENCED_CRS */
                   'CRS183' /* GENOTYPE_IDENTIFIED_CRS */
                      );

            if
                @debug = 'true'
                select @Proc_Step_Name as step, *
                from #CODED_ATTRIBUTES;

            SELECT @RowCount_no = @@ROWCOUNT;


            INSERT INTO [dbo].[job_flow_log]
            (batch_id, [Dataflow_Name], [package_Name], [Status_Type], [step_number], [step_name], [row_count])
            VALUES (@batch_id, 'CRS_CASE_DATAMART', 'CRS_CASE_DATAMART', 'START', @Proc_Step_no, @Proc_Step_Name,
                    @RowCount_no);

        COMMIT TRANSACTION;


        BEGIN TRANSACTION
            SET
                @PROC_STEP_NO = @PROC_STEP_NO + 1;
            SET
                @PROC_STEP_NAME = ' GENERATING #OBS_CODED';

            select obs.public_health_case_uid,
                   imrdb.unique_cd as cd,
                   obs.response,
                   imrdb.col_nm
            INTO #OBS_CODED
            from (SELECT unique_cd,
                         TRIM(Value) AS col_nm
                  FROM #CODED_ATTRIBUTES
                           CROSS APPLY STRING_SPLIT(RDB_attribute, ',')) imrdb
                     LEFT JOIN (SELECT ovc.public_health_case_uid,
                                       ovc.response,
                                       ovc.cd,
                                       CASE
                                           WHEN cd = 'CRS090' THEN 'PRENATAL_CARE_OBTAINED_FRM_' +
                                                                   CAST(ROW_NUMBER() OVER (PARTITION BY ovc.public_health_case_uid, ovc.cd ORDER BY ovc.cd, cvg.nbs_uid DESC) AS NVARCHAR(50))
                                           ELSE ''
                                           END AS col_nm
                                FROM dbo.v_getobscode ovc
                                         LEFT JOIN (SELECT code_desc_txt, nbs_uid
                                                    FROM dbo.v_nrt_srte_code_value_general
                                                    WHERE code_set_nm = 'RUB_PRE_CARE_T') cvg
                                                   ON ovc.response = cvg.code_desc_txt
                                WHERE public_health_case_uid in (SELECT value FROM STRING_SPLIT(@inv_uids, ','))) obs
                               ON imrdb.unique_cd = obs.cd
                                   AND
                                  CASE
                                      WHEN obs.col_nm != '' AND obs.col_nm = imrdb.col_nm THEN 1
                                      WHEN obs.col_nm = '' THEN 1
                                      ELSE 0
                                      END = 1
            where obs.public_health_case_uid is not null;

            if
                @debug = 'true'
                select @Proc_Step_Name as step, *
                from #OBS_CODED;

            SELECT @RowCount_no = @@ROWCOUNT;


            INSERT INTO [dbo].[job_flow_log]
            (batch_id, [Dataflow_Name], [package_Name], [Status_Type], [step_number], [step_name], [row_count])
            VALUES (@batch_id, 'CRS_CASE_DATAMART', 'CRS_CASE_DATAMART', 'START', @Proc_Step_no, @Proc_Step_Name,
                    @RowCount_no);

        COMMIT TRANSACTION;

        BEGIN TRANSACTION
            SET
                @PROC_STEP_NO = @PROC_STEP_NO + 1;
            SET
                @PROC_STEP_NAME = ' GENERATING #OBS_TXT';

            select obs.public_health_case_uid,
                   imrdb.unique_cd     as cd,
                   imrdb.RDB_attribute as col_nm,
                   obs.response
            INTO #OBS_TXT
            from dbo.v_nrt_srte_imrdbmapping imrdb
                     LEFT JOIN (SELECT *
                                FROM dbo.v_getobstxt
                                WHERE public_health_case_uid in (SELECT value FROM STRING_SPLIT(@inv_uids, ','))) obs
                               ON imrdb.unique_cd = obs.cd
            WHERE imrdb.unique_cd IN (
                                      'CRS005', /* DEATH_CERTIFICATE_PRIMARY_CAUS */
                                      'CRS006', /* DEATH_CERTIFICATE_2NDARY_CAUSE */
                                      'CRS008', /* FINAL_ANATOMICAL_DEATH_CAUSE */
                                      'CRS021', /* OTHER_CONGNITALHEART_DISS_DESC */
                                      'CRS043', /* CHILD_OTHER_ABNORMALITIES_1 */
                                      'CRS044', /* CHILD_OTHER_ABNORMALITIES_2 */
                                      'CRS045', /* CHILD_OTHER_ABNORMALITIES_3 */
                                      'CRS046', /* CHILD_OTHER_ABNORMALITIES_4 */
                                      'CRS057', /* RUBELLA_IGM_OTHER_TEST_DESC */
                                      'CRS068', /* VIRUS_ISOLATION_OTHER_SRC */
                                      'CRS075', /* OTHER_RUBELLA_LAB_TEST_DESC */
                                      'CRS076', /* OTHER_RUBELLA_LAB_TEST_RESULT */
                                      'CRS082', /* MOTHER_OCCUPATION_ATCONCEPTION */
                                      'CRS094', /* BY_WHOM_NOT_MD_DIAGNSD_RUBELLA */
                                      'CRS099', /* MOTHER_RUBELLA_ACQUIRED_CITY */
                                      'CRS144', /* RUBELLA_IGG_TEST1_RESULT_VAL, should be numeric in PS */
                                      'CRS145', /* RUBELLA_IGG_TEST2_RESULT_VAL, should be numeric in PS*/
                                      'CRS150', /* MOTHER_OTHER_VACC_INFO_SRC */
                                      'CRS152', /* MATERNAL_ILL_CLINICAL_FEATURE */
                                      'CRS154', /* YR_MOTHER_PRIOR_DELIVERY_IN_US */
                                      'CRS155', /* RT_PCR_OTHER_SRC */
                                      'CRS157', /* RT_PCR_OTHER_SPECIMEN_SRC */
                                      'CRS166', /* OTHER_RELATIONSHIP */
                                      'CRS167', /* IGM_EIA_TEST_1_RESULT_VAL */
                                      'CRS168', /* IGM_EIA_TEST_2_RESULT_VAL */
                                      'CRS169', /* IGM_EIA_OTHER_TST_RESULT_VAL */
                                      'CRS170', /* RT_PCR_TEST_RESULT_VAL */
                                      'CRS171', /* OTHER_RUBELLA_TEST_RESULT_VAL */
                                      'CRS173', /* OTHER_RUBELLA_SPECIMEN_TYPE */
                                      'CRS184' /* GENOTYPE_OTHER_IDENTIFIED_CRS */
                )
              and obs.public_health_case_uid is not null;

            if
                @debug = 'true'
                select @Proc_Step_Name as step, *
                from #OBS_TXT;

            SELECT @RowCount_no = @@ROWCOUNT;


            INSERT INTO [dbo].[job_flow_log]
            (batch_id, [Dataflow_Name], [package_Name], [Status_Type], [step_number], [step_name], [row_count])
            VALUES (@batch_id, 'CRS_CASE_DATAMART', 'CRS_CASE_DATAMART', 'START', @Proc_Step_no, @Proc_Step_Name,
                    @RowCount_no);

        COMMIT TRANSACTION;


        BEGIN TRANSACTION
            SET
                @PROC_STEP_NO = @PROC_STEP_NO + 1;
            SET
                @PROC_STEP_NAME = ' GENERATING #OBS_DATE';

            select obs.public_health_case_uid,
                   imrdb.unique_cd     as cd,
                   imrdb.RDB_attribute as col_nm,
                   obs.response
            INTO #OBS_DATE
            from dbo.v_nrt_srte_imrdbmapping imrdb
                     LEFT JOIN (SELECT *
                                FROM dbo.v_getobsdate
                                WHERE public_health_case_uid in (SELECT value FROM STRING_SPLIT(@inv_uids, ','))) obs
                               ON imrdb.unique_cd = obs.cd
            WHERE imrdb.unique_cd in (
                                      'CRS002', /* HEALTH_PROVIDER_LAST_EVAL_DT */
                                      'CRS022a', /* MOTHER_RASH_ONSET_DT */
                                      'CRS051', /* RUBELLA_IGM_EIA_NONCAPTURE_DT */
                                      'CRS054', /* RUBELLA_IGM_EIA_CAPTURE_DT */
                                      'CRS058', /* RUBELLA_IGM_OTHER_TEST_DT */
                                      'CRS061', /* RUBELLA_IGG_TEST_1_DT */
                                      'CRS063', /* RUBELLA_IGG_TEST_2_DT */
                                      'CRS066', /* VIRUS_ISOLATION_DT */
                                      'CRS071', /* RT_PCR_DT */
                                      'CRS089', /* PRENATAL_FIRST_VISIT_DT */
                                      'CRS101', /* MOTHER_TRAVEL_OUT_US_1_DT */
                                      'CRS102', /* MOTHER_TRAVEL_BACK_US_1_DT */
                                      'CRS103', /* MOTHER_TRAVEL_OUT_US_2_DT */
                                      'CRS104', /* MOTHER_TRAVEL_BACK_US_2_DT */
                                      'CRS107', /* MOTHER_RUBELLA_CASE_EXPOSE_DT */
                                      'CRS141', /* OTHER_RUBELLA_LAB_TEST_DT */
                                      'CRS143', /* SENT_FOR_GENOTYPING_DT */
                                      'CRS148', /* MOTHER_VACCINATED_DT */
                                      'CRS174' /* SEROLOGICALLY_CONFIRMD_DT */
                )
              and obs.public_health_case_uid is not null;

            if
                @debug = 'true'
                select @Proc_Step_Name as step, *
                from #OBS_DATE;

            SELECT @RowCount_no = @@ROWCOUNT;


            INSERT INTO [dbo].[job_flow_log]
            (batch_id, [Dataflow_Name], [package_Name], [Status_Type], [step_number], [step_name], [row_count])
            VALUES (@batch_id, 'CRS_CASE_DATAMART', 'CRS_CASE_DATAMART', 'START', @Proc_Step_no, @Proc_Step_Name,
                    @RowCount_no);

        COMMIT TRANSACTION;


        BEGIN TRANSACTION
            SET
                @PROC_STEP_NO = @PROC_STEP_NO + 1;
            SET
                @PROC_STEP_NAME = ' GENERATING #OBS_NUMERIC';

            select obs.public_health_case_uid,
                   imrdb.unique_cd     as cd,
                   imrdb.RDB_attribute as col_nm,
                   obs.response
            INTO #OBS_NUMERIC
            from dbo.v_nrt_srte_imrdbmapping imrdb
                     LEFT JOIN (SELECT *
                                FROM dbo.v_getobsnum
                                WHERE public_health_case_uid in (SELECT value FROM STRING_SPLIT(@inv_uids, ','))) obs
                               ON imrdb.unique_cd = obs.cd
            WHERE imrdb.unique_cd IN (
                                      'CRS010', /* GESTATIONAL_AGE_IN_WK_AT_BIRTH */
                                      'CRS011', /* CHILD_AGE_AT_THIS_DIAGNOSIS */
                                      'CRS013', /* BIRTH_WEIGHT */ 
                                      'CRS023', /* MOTHER_RASH_LAST_DAY_NBR */
                                      'CRS081', /* MOTHER_AGE_AT_GIVEN_BIRTH */
                                      'CRS083', /* MOTHER_LIVING_IN_US_YRS */
                                      'CRS084', /* AT_PREGNANCY_18YOUNGR_CHILDNBR */
                                      'CRS086', /* RUBELLAVACCD_18YOUNGR_CHILDNBR */
                                      'CRS092', /* PREGNANCY_MO_RUBELLA_SYMPTM_UP */
                                      'CRS158', /* PREVIOUS_PREGNANCY_NBR */
                                      'CRS159', /* TOTAL_LIVE_BIRTH_NBR */
                                      'CRS160' /* BIRTH_DELIVERED_IN_US_NBR */
                )
              and obs.public_health_case_uid is not null;

            if
                @debug = 'true'
                select @Proc_Step_Name as step, *
                from #OBS_NUMERIC;

            SELECT @RowCount_no = @@ROWCOUNT;


            INSERT INTO [dbo].[job_flow_log]
            (batch_id, [Dataflow_Name], [package_Name], [Status_Type], [step_number], [step_name], [row_count])
            VALUES (@batch_id, 'CRS_CASE_DATAMART', 'CRS_CASE_DATAMART', 'START', @Proc_Step_no, @Proc_Step_Name,
                    @RowCount_no);

        COMMIT TRANSACTION;


        BEGIN TRANSACTION
            SET
                @PROC_STEP_NO = @PROC_STEP_NO + 1;
            SET
                @PROC_STEP_NAME = 'UPDATE dbo.CRS_CASE';

            -- variables for the column lists
            -- must be ordered the same as those used in the insert statement
            DECLARE @obscoded_columns NVARCHAR(MAX) = '';
            SELECT @obscoded_columns =
                   COALESCE(STRING_AGG(CAST(QUOTENAME(col_nm) AS NVARCHAR(MAX)), ',') WITHIN GROUP (ORDER BY col_nm),
                            '')
            FROM (SELECT DISTINCT col_nm FROM #OBS_CODED) AS cols;

            DECLARE @obsnum_columns NVARCHAR(MAX) = '';
            SELECT @obsnum_columns =
                   COALESCE(STRING_AGG(CAST(QUOTENAME(col_nm) AS NVARCHAR(MAX)), ',') WITHIN GROUP (ORDER BY col_nm),
                            '')
            FROM (SELECT DISTINCT col_nm FROM #OBS_NUMERIC) AS cols;

            DECLARE @obstxt_columns NVARCHAR(MAX) = '';
            SELECT @obstxt_columns =
                   COALESCE(STRING_AGG(CAST(QUOTENAME(col_nm) AS NVARCHAR(MAX)), ',') WITHIN GROUP (ORDER BY col_nm),
                            '')
            FROM (SELECT DISTINCT col_nm FROM #OBS_TXT) AS cols;

            DECLARE @obsdate_columns NVARCHAR(MAX) = '';
            SELECT @obsdate_columns =
                   COALESCE(STRING_AGG(CAST(QUOTENAME(col_nm) AS NVARCHAR(MAX)), ',') WITHIN GROUP (ORDER BY col_nm),
                            '')
            FROM (SELECT DISTINCT col_nm FROM #OBS_DATE) AS cols;

            DECLARE @Update_sql NVARCHAR(MAX) = '';

            SET @Update_sql = '
        UPDATE tgt
        SET
        tgt.INVESTIGATION_KEY = src.INVESTIGATION_KEY,
        tgt.CONDITION_KEY = src.CONDITION_KEY,
        tgt.patient_key = src.patient_key,
        tgt.Investigator_key = src.Investigator_key,
        tgt.Physician_key = src.Physician_key,
        tgt.Reporter_key = src.Reporter_key,
        tgt.Rpt_Src_Org_key = src.Rpt_Src_Org_key,
        tgt.ADT_HSPTL_KEY = src.ADT_HSPTL_KEY,
        tgt.Inv_Assigned_dt_key = src.Inv_Assigned_dt_key,
        tgt.LDF_GROUP_KEY = src.LDF_GROUP_KEY,
        tgt.GEOCODING_LOCATION_KEY = src.GEOCODING_LOCATION_KEY
        ' + CASE
                WHEN @obscoded_columns != '' THEN ',' + (SELECT STRING_AGG('tgt.' +
                                                        CAST(QUOTENAME(col_nm) AS NVARCHAR(MAX)) +
                                                        ' = ovc.' +
                                                        CAST(QUOTENAME(col_nm) AS NVARCHAR(MAX)),
                                                        ',')
                    FROM (SELECT DISTINCT col_nm FROM #OBS_CODED) as cols)
            ELSE '' END
                + CASE
                      WHEN @obsnum_columns != '' THEN ',' + (SELECT STRING_AGG('tgt.' +
                                                                                CAST(QUOTENAME(col_nm) AS NVARCHAR(MAX)) +
                                                                               ' = ovn.' +
                                                                               CAST(QUOTENAME(col_nm) AS NVARCHAR(MAX)),
                                                                               ',')
                                                             FROM (SELECT DISTINCT col_nm FROM #OBS_NUMERIC) as cols)
                      ELSE '' END
                + CASE
                      WHEN @obstxt_columns != '' THEN ',' + (SELECT STRING_AGG('tgt.' +
                                                                               CAST(QUOTENAME(col_nm) AS NVARCHAR(MAX)) +
                                                                               ' = ovt.' +
                                                                               CAST(QUOTENAME(col_nm) AS NVARCHAR(MAX)),
                                                                               ',')
                                                             FROM (SELECT DISTINCT col_nm FROM #OBS_TXT) as cols)
                      ELSE '' END
                + CASE
                      WHEN @obsdate_columns != '' THEN ',' + (SELECT STRING_AGG('tgt.' +
                                                                                CAST(QUOTENAME(col_nm) AS NVARCHAR(MAX)) +
                                                                                ' = ovd.' +
                                                                                CAST(QUOTENAME(col_nm) AS NVARCHAR(MAX)),
                                                                                ',')
                                                              FROM (SELECT DISTINCT col_nm FROM #OBS_DATE) as cols)
                      ELSE '' END +
                              ' FROM
                              #CRS_CASE_INIT src
                              LEFT JOIN dbo.CRS_CASE tgt
                                  on src.INVESTIGATION_KEY = tgt.INVESTIGATION_KEY'
                + CASE
                      WHEN @obscoded_columns != '' THEN
                          ' LEFT JOIN (
                          SELECT public_health_case_uid, ' + @obscoded_columns + '
        FROM (
            SELECT
                public_health_case_uid,
                col_nm,
                response
            FROM
                #OBS_CODED
        ) AS SourceData
        PIVOT (
            MAX(response)
            FOR col_nm IN (' + @obscoded_columns + ')
        ) AS PivotTable) ovc
        ON ovc.public_health_case_uid = src.public_health_case_uid'
                      ELSE ' ' END +
                              + CASE
                                    WHEN @obsnum_columns != '' THEN
                                        ' LEFT JOIN (
                                        SELECT public_health_case_uid, ' + @obsnum_columns + '
        FROM (
            SELECT
                public_health_case_uid,
                col_nm,
                response
            FROM
                #OBS_NUMERIC
        ) AS SourceData
        PIVOT (
            MAX(response)
            FOR col_nm IN (' + @obsnum_columns + ')
        ) AS PivotTable) ovn
        ON ovn.public_health_case_uid = src.public_health_case_uid'
                                    ELSE ' ' END
                + CASE
                      WHEN @obstxt_columns != '' THEN
                          ' LEFT JOIN (
                          SELECT public_health_case_uid, ' + @obstxt_columns + '
        FROM (
            SELECT
                public_health_case_uid,
                col_nm,
                response
            FROM
                #OBS_TXT
        ) AS SourceData
        PIVOT (
            MAX(response)
            FOR col_nm IN (' + @obstxt_columns + ')
        ) AS PivotTable) ovt
        ON ovt.public_health_case_uid = src.public_health_case_uid'
                      ELSE ' ' END
                + CASE
                      WHEN @obsdate_columns != '' THEN
                          ' LEFT JOIN (
                          SELECT public_health_case_uid, ' + @obsdate_columns + '
        FROM (
            SELECT
                public_health_case_uid,
                col_nm,
                response
            FROM
                #OBS_DATE
        ) AS SourceData
        PIVOT (
            MAX(response)
            FOR col_nm IN (' + @obsdate_columns + ')
        ) AS PivotTable) ovd
        ON ovd.public_health_case_uid = src.public_health_case_uid'
                      ELSE ' ' END
                + ' WHERE
        tgt.INVESTIGATION_KEY IS NOT NULL
        AND src.public_health_case_uid IS NOT NULL;';

            if
                @debug = 'true'
                select @Proc_Step_Name as step, @Update_sql;

            exec sp_executesql @Update_sql;

            SELECT @RowCount_no = @@ROWCOUNT;


            INSERT INTO [dbo].[job_flow_log]
            (batch_id, [Dataflow_Name], [package_Name], [Status_Type], [step_number], [step_name], [row_count])
            VALUES (@batch_id, 'CRS_CASE_DATAMART', 'CRS_CASE_DATAMART', 'START', @Proc_Step_no, @Proc_Step_Name,
                    @RowCount_no);

        COMMIT TRANSACTION;

        BEGIN TRANSACTION;

        SET @PROC_STEP_NO = @PROC_STEP_NO + 1;
        SET @PROC_STEP_NAME = 'INSERT INTO dbo.CRS_CASE';


        -- Variables for the columns in the insert select statement
        -- Must be ordered the same as the original column lists
        DECLARE @obscoded_insert_columns NVARCHAR(MAX) = '';
        SELECT @obscoded_insert_columns = COALESCE(
                STRING_AGG('ovc.' + CAST(QUOTENAME(col_nm) AS NVARCHAR(MAX)), ',') WITHIN GROUP (ORDER BY col_nm), '')
        FROM (SELECT DISTINCT col_nm FROM #OBS_CODED) AS cols;

        DECLARE @obsnum_insert_columns NVARCHAR(MAX) = '';
        SELECT @obsnum_insert_columns = COALESCE(
                STRING_AGG('ovn.' + CAST(QUOTENAME(col_nm) AS NVARCHAR(MAX)), ',') WITHIN GROUP (ORDER BY col_nm), '')
        FROM (SELECT DISTINCT col_nm FROM #OBS_NUMERIC) AS cols;

        DECLARE @obstxt_insert_columns NVARCHAR(MAX) = '';
        SELECT @obstxt_insert_columns = COALESCE(
                STRING_AGG('ovt.' + CAST(QUOTENAME(col_nm) AS NVARCHAR(MAX)), ',') WITHIN GROUP (ORDER BY col_nm), '')
        FROM (SELECT DISTINCT col_nm FROM #OBS_TXT) AS cols;

        DECLARE @obsdate_insert_columns NVARCHAR(MAX) = '';
        SELECT @obsdate_insert_columns = COALESCE(
                STRING_AGG('ovd.' + CAST(QUOTENAME(col_nm) AS NVARCHAR(MAX)), ',') WITHIN GROUP (ORDER BY col_nm), '')
        FROM (SELECT DISTINCT col_nm FROM #OBS_DATE) AS cols;

        DECLARE @Insert_sql NVARCHAR(MAX) = ''


        SET @Insert_sql = '
        INSERT INTO dbo.CRS_CASE (
        INVESTIGATION_KEY,
        CONDITION_KEY,
        patient_key,
        Investigator_key,
        Physician_key,
        Reporter_key,
        Rpt_Src_Org_key,
        ADT_HSPTL_KEY,
        Inv_Assigned_dt_key,
        LDF_GROUP_KEY,
        GEOCODING_LOCATION_KEY
        ' + CASE
                  WHEN @obscoded_columns != '' THEN ',' + @obscoded_columns
                  ELSE '' END
            + CASE
                  WHEN @obsnum_columns != '' THEN ',' + @obsnum_columns
                  ELSE '' END
            + CASE
                  WHEN @obstxt_columns != '' THEN ',' + @obstxt_columns
                  ELSE '' END
            + CASE
                  WHEN @obsdate_columns != '' THEN ',' + @obsdate_columns
                  ELSE '' END +
                          ') SELECT
                            src.INVESTIGATION_KEY,
                            src.CONDITION_KEY,
                            src.patient_key,
                            src.Investigator_key,
                            src.Physician_key,
                            src.Reporter_key,
                            src.Rpt_Src_Org_key,
                            src.ADT_HSPTL_KEY,
                            src.Inv_Assigned_dt_key,
                            src.LDF_GROUP_KEY,
                            src.GEOCODING_LOCATION_KEY
            ' + CASE
                    WHEN @obscoded_columns != '' THEN ',' + @obscoded_insert_columns
                    ELSE '' END
            +
                CASE
                    WHEN @obsnum_columns != '' THEN ',' + @obsnum_insert_columns
                ELSE '' END
            +
                CASE
                    WHEN @obstxt_columns != '' THEN ',' + @obstxt_insert_columns
                ELSE '' END
            +
                CASE
                    WHEN @obsdate_columns != '' THEN ',' + @obsdate_insert_columns
                ELSE '' END
            +
            ' FROM #CRS_CASE_INIT src
            LEFT JOIN dbo.CRS_CASE tgt
                ON src.INVESTIGATION_KEY = tgt.INVESTIGATION_KEY
             '
            + CASE
                  WHEN @obscoded_columns != '' THEN
                      ' LEFT JOIN (
                      SELECT public_health_case_uid, ' + @obscoded_columns + '
        FROM (
            SELECT
                public_health_case_uid,
                col_nm,
                response
            FROM
                #OBS_CODED
        ) AS SourceData
        PIVOT (
            MAX(response)
            FOR col_nm IN (' + @obscoded_columns + ')
        ) AS PivotTable) ovc
        ON ovc.public_health_case_uid = src.public_health_case_uid'
                  ELSE ' ' END +
                          + CASE
                                WHEN @obsnum_columns != '' THEN
                                    ' LEFT JOIN (
                                    SELECT public_health_case_uid, ' + @obsnum_columns + '
        FROM (
            SELECT
                public_health_case_uid,
                col_nm,
                response
            FROM
                #OBS_NUMERIC
        ) AS SourceData
        PIVOT (
            MAX(response)
            FOR col_nm IN (' + @obsnum_columns + ')
        ) AS PivotTable) ovn
        ON ovn.public_health_case_uid = src.public_health_case_uid'
                                ELSE ' ' END
            + CASE
                  WHEN @obstxt_columns != '' THEN
                      ' LEFT JOIN (
                      SELECT public_health_case_uid, ' + @obstxt_columns + '
        FROM (
            SELECT
                public_health_case_uid,
                col_nm,
                response
            FROM
                #OBS_TXT
        ) AS SourceData
        PIVOT (
            MAX(response)
            FOR col_nm IN (' + @obstxt_columns + ')
        ) AS PivotTable) ovt
        ON ovt.public_health_case_uid = src.public_health_case_uid'
                  ELSE ' ' END
            + CASE
                  WHEN @obsdate_columns != '' THEN
                      ' LEFT JOIN (
                      SELECT public_health_case_uid, ' + @obsdate_columns + '
        FROM (
            SELECT
                public_health_case_uid,
                col_nm,
                response
            FROM
                #OBS_DATE
        ) AS SourceData
        PIVOT (
            MAX(response)
            FOR col_nm IN (' + @obsdate_columns + ')
        ) AS PivotTable) ovd
        ON ovd.public_health_case_uid = src.public_health_case_uid'
                  ELSE ' ' END
            + ' WHERE tgt.INVESTIGATION_KEY IS NULL
        AND src.public_health_case_uid IS NOT NULL';


        if
            @debug = 'true'
            select @Proc_Step_Name as step, @Insert_sql;

        exec sp_executesql @Insert_sql;


        SELECT @ROWCOUNT_NO = @@ROWCOUNT;

        INSERT INTO [DBO].[JOB_FLOW_LOG]
        (BATCH_ID, [DATAFLOW_NAME], [PACKAGE_NAME], [STATUS_TYPE], [STEP_NUMBER], [STEP_NAME], [ROW_COUNT])
        VALUES (@BATCH_ID, 'CRS_CASE_DATAMART', 'CRS_CASE_DATAMART', 'START', @PROC_STEP_NO, @PROC_STEP_NAME,
                @ROWCOUNT_NO);

        COMMIT TRANSACTION;


        INSERT INTO [dbo].[job_flow_log]
        (batch_id, [Dataflow_Name], [package_Name], [Status_Type], [step_number], [step_name], [row_count])
        VALUES (@batch_id, 'CRS_CASE_DATAMART', 'CRS_CASE_DATAMART', 'COMPLETE', 999, 'COMPLETE', 0);


    END TRY
    BEGIN CATCH


        IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;


        DECLARE
            @ErrorNumber INT = ERROR_NUMBER();
        DECLARE
            @ErrorLine INT = ERROR_LINE();
        DECLARE
            @ErrorMessage NVARCHAR(4000) = ERROR_MESSAGE();
        DECLARE
            @ErrorSeverity INT = ERROR_SEVERITY();
        DECLARE
            @ErrorState INT = ERROR_STATE();


        INSERT INTO [dbo].[job_flow_log] ( batch_id
                                         , [Dataflow_Name]
                                         , [package_Name]
                                         , [Status_Type]
                                         , [step_number]
                                         , [step_name]
                                         , [Error_Description]
                                         , [row_count])
        VALUES ( @batch_id
               , 'CRS_CASE_DATAMART'
               , 'CRS_CASE_DATAMART'
               , 'ERROR'
               , @Proc_Step_no
               , 'ERROR - ' + @Proc_Step_name
               , 'Step -' + CAST(@Proc_Step_no AS VARCHAR(3)) + ' -' + CAST(@ErrorMessage AS VARCHAR(500))
               , 0);


        return -1;

    END CATCH

END;
