CREATE OR ALTER PROCEDURE [dbo].[sp_generic_case_datamart_postprocessing]
    @phc_ids nvarchar(max),
    @debug bit = 'false'
AS

BEGIN

    /*
     * [Description]
     * This stored procedure is handles event based updates to Generic_Case datamart.
     * 1. Receives input list of public_health_case_uids. The public_health_case_uids are
     * determined using the condition codes associated to STD and HIV page builder investigations.
     * 2. Relevant dimensions and f_std_page_case fact table are used to build the records.
     * 3. The stored procedure inserts or updates records based on the INVESTIGATION_KEY.
     * */


    DECLARE @batch_id BIGINT;
    SET @batch_id = cast((format(getdate(),'yyyyMMddHHmmss')) as bigint);
    PRINT @batch_id;
    DECLARE @RowCount_no int;
    DECLARE @Proc_Step_no float= 0;
    DECLARE @Proc_Step_Name varchar(200)= '';


    BEGIN TRY

        SET @Proc_Step_Name = 'SP_Start';
        SET @PROC_STEP_NO = @PROC_STEP_NO + 1;



        BEGIN TRANSACTION;

        INSERT INTO dbo.job_flow_log ( batch_id
                                     , [Dataflow_Name]
                                     , [package_Name]
                                     , [Status_Type]
                                     , [step_number]
                                     , [step_name]
                                     , [row_count]
                                     , [Msg_Description1])
        VALUES ( @batch_id
               , 'GENERIC_CASE_DATAMART'
               , 'GENERIC_CASE_DATAMART'
               , 'START'
               , @Proc_Step_no
               , @Proc_Step_Name
               , 0
               , LEFT('ID List-' + @phc_ids, 500));

        COMMIT TRANSACTION;

        BEGIN TRANSACTION

        SET
            @PROC_STEP_NO = @PROC_STEP_NO + 1;
        SET
            @PROC_STEP_NAME = ' GENERATING #GENERIC_CASE_INIT';

        select
            inv.public_health_case_uid,
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
            i.GEOCODING_LOCATION_KEY,
            inv.effective_duration_amt as ILLNESS_DURATION,
            inv.effective_duration_unit_cd as ILLNESS_DURATION_UNIT,
            inv.pat_age_at_onset as PATIENT_AGE_AT_ONSET,
            inv.pat_age_at_onset_unit_cd as PATIENT_AGE_AT_ONSET_UNIT,
            inv.detection_method_cd as DETECTION_METHOD,
            inv.detection_method_desc_txt as DETECTION_METHOD_OTHER
        INTO #GENERIC_CASE_INIT
        from
	    dbo.nrt_investigation inv
        inner join dbo.v_common_inv_keys i
            on inv.public_health_case_uid = i.public_health_case_uid

        where
            inv.public_health_case_uid in (SELECT value FROM STRING_SPLIT(@phc_ids, ','))
            AND inv.investigation_form_cd = 'INV_FORM_GEN';

        if
            @debug = 'true'
            select @Proc_Step_Name as step, *
            from #GENERIC_CASE_INIT;

        SELECT @RowCount_no = @@ROWCOUNT;

        INSERT INTO [dbo].[job_flow_log]
        (batch_id, [Dataflow_Name], [package_Name], [Status_Type], [step_number], [step_name], [row_count])
        VALUES (@batch_id, 'GENERIC_CASE_DATAMART', 'GENERIC_CASE_DATAMART', 'START', @Proc_Step_no, @Proc_Step_Name, @RowCount_no);

        COMMIT TRANSACTION;


        BEGIN TRANSACTION

        SET
            @PROC_STEP_NO = @PROC_STEP_NO + 1;
        SET
            @PROC_STEP_NAME = ' GENERATING #OBS_CODED';

        select
            obs.public_health_case_uid,
            imrdb.unique_cd as cd,
            imrdb.RDB_attribute as col_nm,
            obs.response
        INTO #OBS_CODED
        from dbo.v_nrt_srte_imrdbmapping imrdb
        LEFT JOIN (
            SELECT * FROM dbo.v_getobscode
            WHERE public_health_case_uid in (SELECT value FROM STRING_SPLIT(@phc_ids, ','))
        ) obs
        ON imrdb.unique_cd = obs.cd
        WHERE
        imrdb.unique_cd IN (
           'INV113',     /* OTHER_RPT_SRC */
          'INV148',			/* DAYCARE_ASSOCIATION_FLG */
          'INV149',			/* FOOD_HANDLR_IND */
          'INV178',			/* PREGNANCY_STATUS */
          'INV179',			/* PID_IND */
          'INV189' 		/* CULTURE_IDENT_ORG_ID */
        );

        if
            @debug = 'true'
            select @Proc_Step_Name as step, *
            from #OBS_CODED;

        SELECT @RowCount_no = @@ROWCOUNT;
        INSERT INTO [dbo].[job_flow_log]
        (batch_id, [Dataflow_Name], [package_Name], [Status_Type], [step_number], [step_name], [row_count])
        VALUES (@batch_id, 'GENERIC_CASE_DATAMART', 'GENERIC_CASE_DATAMART', 'START', @Proc_Step_no, @Proc_Step_Name, @RowCount_no);

        COMMIT TRANSACTION;


        BEGIN TRANSACTION

        SET
            @PROC_STEP_NO = @PROC_STEP_NO + 1;
        SET
            @PROC_STEP_NAME = ' GENERATING #OBS_DATE';

        select
            obs.public_health_case_uid,
            imrdb.unique_cd as cd,
            imrdb.RDB_attribute as col_nm,
            obs.response
        INTO #OBS_DATE
        from dbo.v_nrt_srte_imrdbmapping imrdb
        LEFT JOIN (
            SELECT * FROM dbo.v_getobsdate
            WHERE public_health_case_uid in (SELECT value FROM STRING_SPLIT(@phc_ids, ','))
        ) obs
        ON imrdb.unique_cd = obs.cd
        WHERE
        imrdb.unique_cd IN (
           'INV184',		/* INV_HSPTL_ID */
	        'INV191'		/* BIRTH_HSPTL_ID */
        );

        if
            @debug = 'true'
            select @Proc_Step_Name as step, *
            from #OBS_DATE;

        SELECT @RowCount_no = @@ROWCOUNT;
        INSERT INTO [dbo].[job_flow_log]
        (batch_id, [Dataflow_Name], [package_Name], [Status_Type], [step_number], [step_name], [row_count])
        VALUES (@batch_id, 'GENERIC_CASE_DATAMART', 'GENERIC_CASE_DATAMART', 'START', @Proc_Step_no, @Proc_Step_Name, @RowCount_no);

        COMMIT TRANSACTION;

        BEGIN TRANSACTION

        SET
            @PROC_STEP_NO = @PROC_STEP_NO + 1;
        SET
            @PROC_STEP_NAME = ' GENERATING #OBS_NUMERIC';

        select
            obs.public_health_case_uid,
            imrdb.unique_cd as cd,
            imrdb.RDB_attribute as col_nm,
            obs.response
        INTO #OBS_NUMERIC
        from dbo.v_nrt_srte_imrdbmapping imrdb
        LEFT JOIN (
            SELECT * FROM dbo.v_getobsnum
            WHERE public_health_case_uid in (SELECT value FROM STRING_SPLIT(@phc_ids, ','))
        ) obs
        ON imrdb.unique_cd = obs.cd
        WHERE
        imrdb.unique_cd IN (
           'INV185'		/* DAYCARE_ID */
        );

        if
            @debug = 'true'
            select @Proc_Step_Name as step, *
            from #OBS_NUMERIC;

        SELECT @RowCount_no = @@ROWCOUNT;
        INSERT INTO [dbo].[job_flow_log]
        (batch_id, [Dataflow_Name], [package_Name], [Status_Type], [step_number], [step_name], [row_count])
        VALUES (@batch_id, 'GENERIC_CASE_DATAMART', 'GENERIC_CASE_DATAMART', 'START', @Proc_Step_no, @Proc_Step_Name, @RowCount_no);

        COMMIT TRANSACTION;


    BEGIN TRANSACTION

        SET
            @PROC_STEP_NO = @PROC_STEP_NO + 1;
        SET
            @PROC_STEP_NAME = 'UPDATE dbo.GENERIC_CASE';

        DECLARE @obscoded_columns NVARCHAR(MAX) = '';
        SELECT @obscoded_columns = COALESCE(STRING_AGG(CAST(QUOTENAME(col_nm) AS NVARCHAR(MAX)), ',') WITHIN GROUP (ORDER BY col_nm) , '')
        FROM (SELECT DISTINCT col_nm FROM #OBS_CODED) AS cols;

        DECLARE @obsnum_columns NVARCHAR(MAX) = '';
        SELECT @obsnum_columns = COALESCE(STRING_AGG(CAST(QUOTENAME(col_nm) AS NVARCHAR(MAX)), ',') WITHIN GROUP (ORDER BY col_nm) , '')
        FROM (SELECT DISTINCT col_nm FROM #OBS_NUMERIC) AS cols;

        DECLARE @obsdate_columns NVARCHAR(MAX) = '';
        SELECT @obsdate_columns = COALESCE(STRING_AGG(CAST(QUOTENAME(col_nm) AS NVARCHAR(MAX)), ',') WITHIN GROUP (ORDER BY col_nm) , '')
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
--         tgt.ADT_HSPTL_KEY = src.ADT_HSPTL_KEY,
        tgt.Inv_Assigned_dt_key = src.Inv_Assigned_dt_key,
        tgt.LDF_GROUP_KEY = src.LDF_GROUP_KEY,
        tgt.GEOCODING_LOCATION_KEY = src.GEOCODING_LOCATION_KEY,
        tgt.ILLNESS_DURATION = src.ILLNESS_DURATION,
        tgt.ILLNESS_DURATION_UNIT = src.ILLNESS_DURATION_UNIT,
        tgt.PATIENT_AGE_AT_ONSET = src.PATIENT_AGE_AT_ONSET,
        tgt.PATIENT_AGE_AT_ONSET_UNIT = src.PATIENT_AGE_AT_ONSET_UNIT,
        tgt.DETECTION_METHOD = src.DETECTION_METHOD,
        tgt.DETECTION_METHOD_OTHER = src.DETECTION_METHOD_OTHER'
  + CASE
                WHEN @obscoded_columns != '' THEN ',' + (SELECT STRING_AGG('tgt.' + CAST(QUOTENAME(col_nm) AS NVARCHAR(MAX)) + ' = ovc.' + CAST(QUOTENAME(col_nm) AS NVARCHAR(MAX)),',')
                                                 FROM #OBS_CODED)
            ELSE '' END
         + CASE
                WHEN @obsnum_columns != '' THEN ',' + (SELECT STRING_AGG('tgt.' + CAST(QUOTENAME(col_nm) AS NVARCHAR(MAX)) + ' = ovn.' + CAST(QUOTENAME(col_nm) AS NVARCHAR(MAX)),',')
                                                 FROM #OBS_NUMERIC)
            ELSE '' END
        + CASE
                WHEN @obsdate_columns != '' THEN ',' + (SELECT STRING_AGG('tgt.' + CAST(QUOTENAME(col_nm) AS NVARCHAR(MAX)) + ' = ovd.' + CAST(QUOTENAME(col_nm) AS NVARCHAR(MAX)),',')
                                                 FROM #OBS_DATE)
            ELSE '' END +
        ' FROM
        #GENERIC_CASE_INIT src
        LEFT JOIN dbo.GENERIC_CASE tgt
            on src.INVESTIGATION_KEY = tgt.INVESTIGATION_KEY'
        + CASE
              WHEN @obscoded_columns != '' THEN
        '
        + LEFT JOIN (
        SELECT public_health_case_uid, ' + @obscoded_columns + '
        FROM (
            SELECT
                public_health_case_uid,
                col_nm,
                response
            FROM
                #OBS_CODED
                where public_health_case_uid IS NOT NULL
        ) AS SourceData
        PIVOT (
            MAX(response)
            FOR col_nm IN (' + @obscoded_columns + ')
        ) AS PivotTable) ovc
        ON ovc.public_health_case_uid = src.public_health_case_uid'
        ELSE ' ' END +
        + CASE
              WHEN @obsnum_columns != '' THEN
        '
        + LEFT JOIN(
        SELECT public_health_case_uid, ' + @obsnum_columns + '
        FROM (
            SELECT
                public_health_case_uid,
                col_nm,
                response
            FROM
                #OBS_NUMERIC
                where public_health_case_uid IS NOT NULL
        ) AS SourceData
        PIVOT (
            MAX(response)
            FOR col_nm IN (' + @obsnum_columns + ')
        ) AS PivotTable) ovn
        ON ovn.public_health_case_uid = src.public_health_case_uid'
        ELSE ' ' END
        + CASE
              WHEN @obsdate_columns != '' THEN
        '
        + LEFT JOIN (
        SELECT public_health_case_uid, ' + @obsdate_columns + '
        FROM (
            SELECT
                public_health_case_uid,
                col_nm,
                response
            FROM
                #OBS_DATE
                where public_health_case_uid IS NOT NULL
        ) AS SourceData
        PIVOT (
            MAX(response)
            FOR col_nm IN (' + @obsdate_columns + ')
        ) AS PivotTable) ovd
        ON ovc.public_health_case_uid = src.public_health_case_uid'
        ELSE ' ' END
        + ' WHERE
        tgt.INVESTIGATION_KEY IS NOT NULL
        AND src.public_health_case_uid IS NOT NULL;';

        if
            @debug = 'true'
            select @Proc_Step_Name as step,  @Update_sql;

        BEGIN TRANSACTION;

        SET @PROC_STEP_NO = @PROC_STEP_NO + 1;
        SET @Proc_Step_Name = 'SP_COMPLETE';
        INSERT INTO [dbo].[job_flow_log]
        (batch_id,
         [Dataflow_Name],
         [package_Name],
         [Status_Type],
         [step_number],
         [step_name],
         [row_count]
        )
        VALUES
            (@batch_id,
             'GENERIC_CASE_DATAMART',
             'GENERIC_CASE_DATAMART',
             'COMPLETE',
             @Proc_Step_no,
             @Proc_Step_name,
             @RowCount_no
            );
        COMMIT TRANSACTION;


    END TRY


    BEGIN CATCH
        IF @@TRANCOUNT > 0
            BEGIN
                ROLLBACK TRANSACTION;
            END;
        DECLARE @ErrorNumber INT = ERROR_NUMBER();
        DECLARE @ErrorLine INT = ERROR_LINE();
        DECLARE @ErrorMessage NVARCHAR(4000) = ERROR_MESSAGE();
        DECLARE @ErrorSeverity INT = ERROR_SEVERITY();
        DECLARE @ErrorState INT = ERROR_STATE();

        INSERT INTO [dbo].[job_flow_log]( batch_id, [Dataflow_Name], [package_Name], [Status_Type], [step_number], [step_name], [Error_Description], [row_count] )
        VALUES( @Batch_id, 'STD_HIV_DATAMART', 'STD_HIV_DATAMART', 'ERROR', @Proc_Step_no, 'ERROR - '+@Proc_Step_name, 'Step -'+CAST(@Proc_Step_no AS varchar(3))+' -'+CAST(@ErrorMessage AS varchar(500)), 0 );
        RETURN -1;

    END CATCH;
END;