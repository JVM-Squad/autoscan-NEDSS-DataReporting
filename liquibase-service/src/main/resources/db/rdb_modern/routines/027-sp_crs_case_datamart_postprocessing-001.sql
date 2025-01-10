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

        BEGIN TRANSACTION
            SET
                @PROC_STEP_NO = @PROC_STEP_NO + 1;
            SET
                @PROC_STEP_NAME = 'GENERATING #OBS_CODED';


            select public_health_case_uid,
                   unique_cd      as cd,
                   CASE
                       WHEN unique_cd = 'CRS090' THEN 'PRENATAL_CARE_OBTAINED_FRM_' +
                                                      CAST(ROW_NUMBER() OVER (PARTITION BY rom.public_health_case_uid, rom.unique_cd ORDER BY rom.unique_cd, cvg.nbs_uid DESC) AS NVARCHAR(50))
                       ELSE col_nm
                       END        AS col_nm,
                   coded_response as response
            INTO #OBS_CODED
            from dbo.v_rdb_obs_mapping rom
                     LEFT JOIN (SELECT code_desc_txt, nbs_uid
                                FROM dbo.v_nrt_srte_code_value_general
                                WHERE code_set_nm = 'RUB_PRE_CARE_T') cvg
                               ON rom.coded_response = cvg.code_desc_txt
            WHERE ((RDB_TABLE = 'CRS_Case' and db_field = 'code'))
              and public_health_case_uid in (SELECT value FROM STRING_SPLIT(@inv_uids, ','));


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

            select public_health_case_uid,
                   unique_cd    as cd,
                   col_nm,
                   txt_response as response
            INTO #OBS_TXT
            from dbo.v_rdb_obs_mapping
            WHERE ((RDB_TABLE = 'CRS_Case' and db_field = 'value_txt'))
              and public_health_case_uid in (SELECT value FROM STRING_SPLIT(@inv_uids, ','));

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

            select public_health_case_uid,
                   unique_cd     as cd,
                   col_nm,
                   date_response as response
            INTO #OBS_DATE
            from dbo.v_rdb_obs_mapping
            WHERE ((RDB_TABLE = 'CRS_Case' and db_field = 'from_time'))
              and public_health_case_uid in (SELECT value FROM STRING_SPLIT(@inv_uids, ','))
              AND unique_cd != 'CRS013';

            /*
                AND unique_cd != 'CRS013'

                This condition is in temporarily, as CRS013 is improperly labeled as a date value. It will cause an error if it is used in the date table.
            */

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

            select public_health_case_uid,
                   unique_cd        as cd,
                   col_nm,
                   numeric_response as response
            INTO #OBS_NUMERIC
            from dbo.v_rdb_obs_mapping
            WHERE ((RDB_TABLE = 'CRS_Case' and db_field = 'numeric_value_1') or unique_cd = 'CRS013')
              and public_health_case_uid in (SELECT value FROM STRING_SPLIT(@inv_uids, ','));

            /*
                or unique_cd = 'CRS013'

                This condition is written in so that CRS013 is captured properly
            */
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
            FROM (SELECT DISTINCT TRIM(value) AS col_nm FROM dbo.v_nrt_srte_imrdbmapping CROSS APPLY STRING_SPLIT(RDB_attribute, ',') where db_field = 'code' AND RDB_table = 'CRS_Case' ) AS cols;

            DECLARE @obsnum_columns NVARCHAR(MAX) = '';
            SELECT @obsnum_columns =
                   COALESCE(STRING_AGG(CAST(QUOTENAME(col_nm) AS NVARCHAR(MAX)), ',') WITHIN GROUP (ORDER BY col_nm),
                            '')
            FROM (SELECT DISTINCT RDB_ATTRIBUTE AS col_nm FROM dbo.v_nrt_srte_imrdbmapping where (RDB_TABLE = 'CRS_Case' and db_field = 'numeric_value_1') or unique_cd = 'CRS013') AS cols;

            DECLARE @obstxt_columns NVARCHAR(MAX) = '';
            SELECT @obstxt_columns =
                   COALESCE(STRING_AGG(CAST(QUOTENAME(col_nm) AS NVARCHAR(MAX)), ',') WITHIN GROUP (ORDER BY col_nm),
                            '')
            FROM (SELECT DISTINCT RDB_ATTRIBUTE AS col_nm FROM dbo.v_nrt_srte_imrdbmapping where db_field = 'value_txt' AND RDB_table = 'CRS_Case') AS cols;

            DECLARE @obsdate_columns NVARCHAR(MAX) = '';
            SELECT @obsdate_columns =
                   COALESCE(STRING_AGG(CAST(QUOTENAME(col_nm) AS NVARCHAR(MAX)), ',') WITHIN GROUP (ORDER BY col_nm),
                            '')
            FROM (SELECT DISTINCT RDB_ATTRIBUTE AS col_nm FROM dbo.v_nrt_srte_imrdbmapping where (RDB_TABLE = 'CRS_Case' and db_field = 'from_time') AND unique_cd != 'CRS013') AS cols;

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
                    FROM (SELECT DISTINCT TRIM(value) AS col_nm FROM dbo.v_nrt_srte_imrdbmapping CROSS APPLY STRING_SPLIT(RDB_attribute, ',') where db_field = 'code' AND RDB_table = 'CRS_Case' ) as cols)
            ELSE '' END
                + CASE
                      WHEN @obsnum_columns != '' THEN ',' + (SELECT STRING_AGG('tgt.' +
                                                                                CAST(QUOTENAME(col_nm) AS NVARCHAR(MAX)) +
                                                                               ' = ovn.' +
                                                                               CAST(QUOTENAME(col_nm) AS NVARCHAR(MAX)),
                                                                               ',')
                                                             FROM (SELECT DISTINCT RDB_ATTRIBUTE AS col_nm FROM dbo.v_nrt_srte_imrdbmapping where (RDB_TABLE = 'CRS_Case' and db_field = 'numeric_value_1') or unique_cd = 'CRS013') as cols)
                      ELSE '' END
                + CASE
                      WHEN @obstxt_columns != '' THEN ',' + (SELECT STRING_AGG('tgt.' +
                                                                               CAST(QUOTENAME(col_nm) AS NVARCHAR(MAX)) +
                                                                               ' = ovt.' +
                                                                               CAST(QUOTENAME(col_nm) AS NVARCHAR(MAX)),
                                                                               ',')
                                                             FROM (SELECT DISTINCT RDB_ATTRIBUTE AS col_nm FROM dbo.v_nrt_srte_imrdbmapping where db_field = 'value_txt' AND RDB_table = 'CRS_Case') as cols)
                      ELSE '' END
                + CASE
                      WHEN @obsdate_columns != '' THEN ',' + (SELECT STRING_AGG('tgt.' +
                                                                                CAST(QUOTENAME(col_nm) AS NVARCHAR(MAX)) +
                                                                                ' = ovd.' +
                                                                                CAST(QUOTENAME(col_nm) AS NVARCHAR(MAX)),
                                                                                ',')
                                                              FROM (SELECT DISTINCT RDB_ATTRIBUTE AS col_nm FROM dbo.v_nrt_srte_imrdbmapping where (RDB_TABLE = 'CRS_Case' and db_field = 'from_time') AND unique_cd != 'CRS013') as cols)
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
        FROM (SELECT DISTINCT TRIM(value) AS col_nm FROM dbo.v_nrt_srte_imrdbmapping CROSS APPLY STRING_SPLIT(RDB_attribute, ',') where db_field = 'code' AND RDB_table = 'CRS_Case' ) AS cols;

        DECLARE @obsnum_insert_columns NVARCHAR(MAX) = '';
        SELECT @obsnum_insert_columns = COALESCE(
                STRING_AGG('ovn.' + CAST(QUOTENAME(col_nm) AS NVARCHAR(MAX)), ',') WITHIN GROUP (ORDER BY col_nm), '')
        FROM (SELECT DISTINCT RDB_ATTRIBUTE AS col_nm FROM dbo.v_nrt_srte_imrdbmapping where (RDB_TABLE = 'CRS_Case' and db_field = 'numeric_value_1') or unique_cd = 'CRS013') AS cols;

        DECLARE @obstxt_insert_columns NVARCHAR(MAX) = '';
        SELECT @obstxt_insert_columns = COALESCE(
                STRING_AGG('ovt.' + CAST(QUOTENAME(col_nm) AS NVARCHAR(MAX)), ',') WITHIN GROUP (ORDER BY col_nm), '')
        FROM (SELECT DISTINCT RDB_ATTRIBUTE AS col_nm FROM dbo.v_nrt_srte_imrdbmapping where db_field = 'value_txt' AND RDB_table = 'CRS_Case') AS cols;

        DECLARE @obsdate_insert_columns NVARCHAR(MAX) = '';
        SELECT @obsdate_insert_columns = COALESCE(
                STRING_AGG('ovd.' + CAST(QUOTENAME(col_nm) AS NVARCHAR(MAX)), ',') WITHIN GROUP (ORDER BY col_nm), '')
        FROM (SELECT DISTINCT RDB_ATTRIBUTE AS col_nm FROM dbo.v_nrt_srte_imrdbmapping where (RDB_TABLE = 'CRS_Case' and db_field = 'from_time') AND unique_cd != 'CRS013') AS cols;

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
