CREATE PROCEDURE [dbo].[sp_d_lab_test_postprocessing]
    @id_list nvarchar(max), @debug bit = 'false'
as

BEGIN

    --
    --UPDATE ACTIVITY_LOG_DETAIL SET
    --START_DATE=DATETIME();
    -- declare  @batch_id BIGINT


    declare @batch_id bigint;
    set @batch_id = cast((format(getdate(), 'yyMMddHHmmss')) as bigint);

    DECLARE @RowCount_no INT ;
    DECLARE @Proc_Step_no FLOAT = 0 ;
    DECLARE @Proc_Step_Name VARCHAR(200) = '' ;
    DECLARE @batch_start_time datetime2(7) = null ;
    DECLARE @batch_end_time datetime2(7) = null ;

    BEGIN TRY

        SET @Proc_Step_no = 1;
        SET @Proc_Step_Name = 'SP_Start';




        BEGIN TRANSACTION;


--create table updated_observation_List as



        SELECT @ROWCOUNT_NO = @@ROWCOUNT;

        INSERT INTO [DBO].[JOB_FLOW_LOG]
        (BATCH_ID,[DATAFLOW_NAME],[PACKAGE_NAME] ,[STATUS_TYPE],[STEP_NUMBER],[STEP_NAME],[ROW_COUNT])
        VALUES(@BATCH_ID,'D_LABTEST','D_LABTEST','START',  @PROC_STEP_NO,@PROC_STEP_NAME,@ROWCOUNT_NO);


        COMMIT TRANSACTION;

        BEGIN TRANSACTION;

        SET @PROC_STEP_NO =  @PROC_STEP_NO + 1 ;
        SET @PROC_STEP_NAME = ' GENERATING TMP_updated_observation_List ';

        select
            ctrl_cd_display_form, observation_uid, last_chg_time
        into #s_updated_lab
        from
            rdb_modern..nrt_observation with (nolock)
        where
            observation_uid  in (select value from string_split(@id_list, ','))
          and ctrl_cd_display_form in ('LabReport','MorbReport')
          and obs_domain_cd_st_1 ='Order';


        select observation_uid
        into #updated_observation_list
        from #s_updated_lab s
        union
        select act1.source_act_uid as observation_uid from #s_updated_lab s left outer join nbs_odse..act_relationship act1 with (nolock) on  s.observation_uid= act1.target_act_uid
        where source_act_uid is not null
        union
        select act2.source_act_uid as observation_uid from #s_updated_lab s left outer join nbs_odse..act_relationship act1 with (nolock)  on  s.observation_uid= act1.target_act_uid
                                                                            left outer join nbs_odse..act_relationship act2 with (nolock)  on act1.source_act_uid=act2.target_act_uid
        where act2.source_act_uid is not null
        union
        select act3.source_act_uid as observation_uid from #s_updated_lab s left outer join nbs_odse..act_relationship act1 with (nolock)  on  s.observation_uid= act1.target_act_uid
                                                                            left outer join nbs_odse..act_relationship act2 with (nolock) on act1.source_act_uid=act2.target_act_uid
                                                                            left outer join nbs_odse..act_relationship act3 with (nolock) on act2.source_act_uid=act3.target_act_uid
        where act3.source_act_uid is not null
        union
        select act4.source_act_uid as observation_uid from #s_updated_lab s left outer join nbs_odse..act_relationship act1 with (nolock) on  s.observation_uid= act1.target_act_uid
                                                                            left outer join nbs_odse..act_relationship act2 with (nolock) on act1.source_act_uid=act2.target_act_uid
                                                                            left outer join nbs_odse..act_relationship act3 with (nolock) on act2.source_act_uid=act3.target_act_uid
                                                                            left outer join nbs_odse..act_relationship act4 with (nolock) on act3.source_act_uid=act4.target_act_uid
        where act4.source_act_uid is not null;


        IF OBJECT_ID('#TMP_updated_observation_List', 'U') IS NOT NULL
            drop table  #TMP_updated_observation_List;

        select *
        into #TMP_updated_observation_List
        from #updated_observation_List
        ;

        create index idx_TMP_updated_observation_List_oid on #TMP_updated_observation_List(OBSERVATION_UID);

        --create table updated_LAB_TEST_list as

        SELECT @ROWCOUNT_NO = @@ROWCOUNT;

        INSERT INTO [DBO].[JOB_FLOW_LOG]
        (BATCH_ID,[DATAFLOW_NAME],[PACKAGE_NAME] ,[STATUS_TYPE],[STEP_NUMBER],[STEP_NAME],[ROW_COUNT])
        VALUES(@BATCH_ID,'D_LABTEST','D_LABTEST','START',  @PROC_STEP_NO,@PROC_STEP_NAME,@ROWCOUNT_NO);


        COMMIT TRANSACTION;

        BEGIN TRANSACTION;
        SET @PROC_STEP_NO =  @PROC_STEP_NO + 1 ;
        SET @PROC_STEP_NAME = 'GENERATING #TMP_updated_LAB_TEST_list ';


        IF OBJECT_ID('#TMP_updated_LAB_TEST_list', 'U') IS NOT NULL
            drop table  #TMP_updated_LAB_TEST_list;

        select LAB_TEST_uid, LAB_TEST_key
        into #TMP_updated_LAB_TEST_list
        from LAB_TEST
        where LAB_TEST_uid in (select observation_uid from #updated_observation_list)
        ;







        -- create table updated_LAB_RPT_USER_COMMENT as


        SELECT @ROWCOUNT_NO = @@ROWCOUNT;
        INSERT INTO [DBO].[JOB_FLOW_LOG]
        (BATCH_ID,[DATAFLOW_NAME],[PACKAGE_NAME] ,[STATUS_TYPE],[STEP_NUMBER],[STEP_NAME],[ROW_COUNT])
        VALUES(@BATCH_ID,'D_LABTEST','D_LABTEST','START',  @PROC_STEP_NO,@PROC_STEP_NAME,@ROWCOUNT_NO);


        COMMIT TRANSACTION;

        BEGIN TRANSACTION;
        SET @PROC_STEP_NO =  @PROC_STEP_NO + 1 ;
        SET @PROC_STEP_NAME = 'GENERATING TMP_updated_LAB_RPT_USER_COMMENT ';


        IF OBJECT_ID('#TMP_updated_LAB_RPT_USER_COMMENT', 'U') IS NOT NULL
            drop table  #TMP_updated_LAB_RPT_USER_COMMENT;


        select user_comment_key
        into  #TMP_updated_LAB_RPT_USER_COMMENT
        from LAB_RPT_USER_COMMENT
        where LAB_TEST_uid in (select observation_uid from  #TMP_updated_observation_List);


        -- create table updt_Test_Result_Grouping_LIST as

        SELECT @ROWCOUNT_NO = @@ROWCOUNT;
        INSERT INTO [DBO].[JOB_FLOW_LOG]
        (BATCH_ID,[DATAFLOW_NAME],[PACKAGE_NAME] ,[STATUS_TYPE],[STEP_NUMBER],[STEP_NAME],[ROW_COUNT])
        VALUES(@BATCH_ID,'D_LABTEST','D_LABTEST','START',  @PROC_STEP_NO,@PROC_STEP_NAME,@ROWCOUNT_NO);


        COMMIT TRANSACTION;

        BEGIN TRANSACTION;
        SET @PROC_STEP_NO =  @PROC_STEP_NO + 1 ;
        SET @PROC_STEP_NAME = 'GENERATING TMP_updt_Test_Result_Grouping_LIST ';


        IF OBJECT_ID('#TMP_updt_Test_Result_Grouping_LIST', 'U') IS NOT NULL
            drop table  #TMP_updt_Test_Result_Grouping_LIST ;

        select LAB_TEST_uid
        into #TMP_updt_Test_Result_Grouping_LIST
        from Test_Result_Grouping
        where LAB_TEST_uid in (select observation_uid from #TMP_updated_observation_List);


        -- create table updt_Lab_Result_Val_list as

        SELECT @ROWCOUNT_NO = @@ROWCOUNT;
        INSERT INTO [DBO].[JOB_FLOW_LOG]
        (BATCH_ID,[DATAFLOW_NAME],[PACKAGE_NAME] ,[STATUS_TYPE],[STEP_NUMBER],[STEP_NAME],[ROW_COUNT])
        VALUES(@BATCH_ID,'D_LABTEST','D_LABTEST','START',  @PROC_STEP_NO,@PROC_STEP_NAME,@ROWCOUNT_NO);


        COMMIT TRANSACTION;

        BEGIN TRANSACTION;
        SET @PROC_STEP_NO =  @PROC_STEP_NO + 1 ;
        SET @PROC_STEP_NAME = 'GENERATING #TMP_updt_Lab_Result_Val_list ';


        IF OBJECT_ID('#TMP_updt_Lab_Result_Val_list', 'U') IS NOT NULL
            drop table  #TMP_updt_Lab_Result_Val_list;


        select LAB_TEST_uid
        into #TMP_updt_Lab_Result_Val_list
        from Lab_Result_Val
        where LAB_TEST_uid in (select observation_uid from #TMP_updated_observation_List);


        -- create table updated_LAB_TEST_result_list as

        SELECT @ROWCOUNT_NO = @@ROWCOUNT;
        INSERT INTO [DBO].[JOB_FLOW_LOG]
        (BATCH_ID,[DATAFLOW_NAME],[PACKAGE_NAME] ,[STATUS_TYPE],[STEP_NUMBER],[STEP_NAME],[ROW_COUNT])
        VALUES(@BATCH_ID,'D_LABTEST','D_LABTEST','START',  @PROC_STEP_NO,@PROC_STEP_NAME,@ROWCOUNT_NO);


        COMMIT TRANSACTION;

        BEGIN TRANSACTION;
        SET @PROC_STEP_NO =  @PROC_STEP_NO + 1 ;
        SET @PROC_STEP_NAME = 'GENERATING #TMP_updated_LAB_TEST_result_list ';


        IF OBJECT_ID('#TMP_updated_LAB_TEST_result_list', 'U') IS NOT NULL
            drop table  #TMP_updated_LAB_TEST_result_list;


        select LAB_TEST_key
        into #TMP_updated_LAB_TEST_result_list
        from LAB_TEST_RESULT
        where LAB_TEST_uid in (select OBSERVATION_UID from #TMP_updated_observation_List);


        -- create table updT_Result_Comment_Grp_LIST as

        SELECT @ROWCOUNT_NO = @@ROWCOUNT;
        INSERT INTO [DBO].[JOB_FLOW_LOG]
        (BATCH_ID,[DATAFLOW_NAME],[PACKAGE_NAME] ,[STATUS_TYPE],[STEP_NUMBER],[STEP_NAME],[ROW_COUNT])
        VALUES(@BATCH_ID,'D_LABTEST','D_LABTEST','START',  @PROC_STEP_NO,@PROC_STEP_NAME,@ROWCOUNT_NO);


        COMMIT TRANSACTION;

        BEGIN TRANSACTION;
        SET @PROC_STEP_NO =  @PROC_STEP_NO + 1 ;
        SET @PROC_STEP_NAME = 'GENERATING #TMP_updT_Result_Comment_Grp_LIST ';


        IF OBJECT_ID('#TMP_updT_Result_Comment_Grp_LIST', 'U') IS NOT NULL
            drop table  #TMP_updT_Result_Comment_Grp_LIST;


        select LAB_TEST_uid
        into #TMP_updT_Result_Comment_Grp_LIST
        from RESULT_COMMENT_GROUP
        where LAB_TEST_uid in (select observation_uid from #TMP_updated_observation_List);


        -- create table updt_Lab_Result_Comment_list as

        SELECT @ROWCOUNT_NO = @@ROWCOUNT;
        INSERT INTO [DBO].[JOB_FLOW_LOG]
        (BATCH_ID,[DATAFLOW_NAME],[PACKAGE_NAME] ,[STATUS_TYPE],[STEP_NUMBER],[STEP_NAME],[ROW_COUNT])
        VALUES(@BATCH_ID,'D_LABTEST','D_LABTEST','START',  @PROC_STEP_NO,@PROC_STEP_NAME,@ROWCOUNT_NO);


        COMMIT TRANSACTION;

        BEGIN TRANSACTION;
        SET @PROC_STEP_NO =  @PROC_STEP_NO + 1 ;
        SET @PROC_STEP_NAME = ' GENERATING TMP_updt_Lab_Result_Comment_list ';


        IF OBJECT_ID('#TMP_updt_Lab_Result_Comment_list', 'U') IS NOT NULL
            drop table  #TMP_updt_Lab_Result_Comment_list;


        select LAB_TEST_uid
        into #TMP_updt_Lab_Result_Comment_list
        from Lab_Result_Comment
        where LAB_TEST_uid in (select observation_uid from #TMP_updated_observation_List);

        -- create table .updated_LAB_TEST_list as

        SELECT @ROWCOUNT_NO = @@ROWCOUNT;
        INSERT INTO [DBO].[JOB_FLOW_LOG]
        (BATCH_ID,[DATAFLOW_NAME],[PACKAGE_NAME] ,[STATUS_TYPE],[STEP_NUMBER],[STEP_NAME],[ROW_COUNT])
        VALUES(@BATCH_ID,'D_LABTEST','D_LABTEST','START',  @PROC_STEP_NO,@PROC_STEP_NAME,@ROWCOUNT_NO);


        COMMIT TRANSACTION;

        BEGIN TRANSACTION;
        SET @PROC_STEP_NO =  @PROC_STEP_NO + 1 ;
        SET @PROC_STEP_NAME = ' GENERATING TMP_updated_LAB_TEST_list ';


        IF OBJECT_ID('#TMP_updated_LAB_TEST_list', 'U') IS NOT NULL
            drop table  #TMP_updated_LAB_TEST_list;

        select *
        into TMP_updated_LAB_TEST_list
        from updated_LAB_TEST_list;


        -- create table s_edx_document1 as ( spoke with Pradeepp- need to get Max add time value only

        SELECT @ROWCOUNT_NO = @@ROWCOUNT;
        INSERT INTO [DBO].[JOB_FLOW_LOG]
        (BATCH_ID,[DATAFLOW_NAME],[PACKAGE_NAME] ,[STATUS_TYPE],[STEP_NUMBER],[STEP_NAME],[ROW_COUNT])
        VALUES(@BATCH_ID,'D_LABTEST','D_LABTEST','START',  @PROC_STEP_NO,@PROC_STEP_NAME,@ROWCOUNT_NO);


        COMMIT TRANSACTION;

        BEGIN TRANSACTION;
        SET @PROC_STEP_NO =  @PROC_STEP_NO + 1 ;
        SET @PROC_STEP_NAME = ' GENERATING TMP_s_edx_document1 ';


        IF OBJECT_ID('#TMP_s_edx_document1', 'U') IS NOT NULL
            drop table  #TMP_s_edx_document1;

        select EDX_Document_uid, act_uid, add_time
        into #TMP_s_edx_document1
        from (select EDX_Document_uid, act_uid, add_time ,ROW_NUMBER() OVER (PARTITION BY act_uid ORDER BY add_time DESC) rankno
              from nbs_odse..EDX_Document, #s_updated_lab s
              where  EDX_Document.act_uid=s.observation_Uid
             ) edx_lst
        where edx_lst.rankno = 1
        ;


        -- create table updated_participant as


        SELECT @ROWCOUNT_NO = @@ROWCOUNT;
        INSERT INTO [DBO].[JOB_FLOW_LOG]
        (BATCH_ID,[DATAFLOW_NAME],[PACKAGE_NAME] ,[STATUS_TYPE],[STEP_NUMBER],[STEP_NAME],[ROW_COUNT])
        VALUES(@BATCH_ID,'D_LABTEST','D_LABTEST','START',  @PROC_STEP_NO,@PROC_STEP_NAME,@ROWCOUNT_NO);


        COMMIT TRANSACTION;

        BEGIN TRANSACTION;
        SET @PROC_STEP_NO =  @PROC_STEP_NO + 1 ;
        SET @PROC_STEP_NAME = ' GENERATING TMP_updated_participant ';


        IF OBJECT_ID('#TMP_updated_participant', 'U') IS NOT NULL
            drop table  TMP_updated_participant;

        select act_uid,
               subject_entity_uid, type_cd,act_class_cd,record_status_cd,subject_class_cd,
               observation_uid
        into #TMP_updated_participant
        from nbs_odse..participation, #TMP_updated_observation_List obs
        where
            participation.act_uid=obs.observation_uid;

        CREATE NONCLUSTERED INDEX [idx_TMP_updated_participant_tcd] ON [dbo].[TMP_updated_participant]
            (
             [type_cd] ASC,
             [act_class_cd] ASC,
             [record_status_cd] ASC,
             [subject_class_cd] ASC
                )
            INCLUDE ( 	[act_uid],
                         [subject_entity_uid])
        ;

        -- Create Table merged_provider as

        SELECT @ROWCOUNT_NO = @@ROWCOUNT;
        INSERT INTO [DBO].[JOB_FLOW_LOG]
        (BATCH_ID,[DATAFLOW_NAME],[PACKAGE_NAME] ,[STATUS_TYPE],[STEP_NUMBER],[STEP_NAME],[ROW_COUNT])
        VALUES(@BATCH_ID,'D_LABTEST','D_LABTEST','START',  @PROC_STEP_NO,@PROC_STEP_NAME,@ROWCOUNT_NO);


        COMMIT TRANSACTION;

        BEGIN TRANSACTION;
        SET @PROC_STEP_NO =  @PROC_STEP_NO + 1 ;
        SET @PROC_STEP_NAME = ' GENERATING TMP_merged_provider ';


        IF OBJECT_ID('#TMP_merged_provider', 'U') IS NOT NULL
            drop table  #TMP_merged_provider;

        Select distinct a.provider_first_name, a.provider_last_name, a.provider_uid, a.provider_key,
                        b.root_extension_txt as person_id_val,
                        b.type_cd as patient_id_type,
                        --VS		 put(b.type_cd,$EI_TYPE.) as person_id_type_desc,   /* code_set_nm = EI_TYPE */
                        cvg.code_short_desc_txt as person_id_type_desc,   /* code_set_nm = EI_TYPE */
                        b.assigning_authority_cd as person_id_assign_auth_cd,
                        b.record_status_cd
        into #TMP_merged_provider
        from #TMP_updated_participant PART
                 LEFT JOIN d_provider a	  ON PART.SUBJECT_ENTITY_UID=a.provider_uid
                 LEFT JOIN nbs_odse..entity_id b On a.provider_uid = b.entity_uid
                 left join nbs_srte..code_value_general  as cvg on b.type_cd = cvg.code 	and cvg.code_set_nm = 'EI_TYPE'
        ;


        /* -- VS

        data filter_participants (rename = (subject_entity_uid = provider_uid));
        set updated_participant;
        where type_cd in ('ENT', 'ASS', 'VRF')
            and act_class_cd = 'OBS'
            and record_status_cd ='ACTIVE'
            and subject_class_cd = 'PSN';

        */

        SELECT @ROWCOUNT_NO = @@ROWCOUNT;
        INSERT INTO [DBO].[JOB_FLOW_LOG]
        (BATCH_ID,[DATAFLOW_NAME],[PACKAGE_NAME] ,[STATUS_TYPE],[STEP_NUMBER],[STEP_NAME],[ROW_COUNT])
        VALUES(@BATCH_ID,'D_LABTEST','D_LABTEST','START',  @PROC_STEP_NO,@PROC_STEP_NAME,@ROWCOUNT_NO);


        COMMIT TRANSACTION;

        BEGIN TRANSACTION;
        SET @PROC_STEP_NO =  @PROC_STEP_NO + 1 ;
        SET @PROC_STEP_NAME = ' GENERATING TMP_filter_participants ';


        IF OBJECT_ID('#TMP_filter_participants', 'U') IS NOT NULL
            drop table #TMP_filter_participants ;


        select act_uid,
               subject_entity_uid as provider_uid_filter,
               type_cd,act_class_cd,
               record_status_cd as record_status_cd_filter,
               subject_class_cd,
               observation_uid
        into #TMP_filter_participants
        from #TMP_updated_participant
        where type_cd in ('ENT', 'ASS', 'VRF')
          and act_class_cd = 'OBS'
          and record_status_cd ='ACTIVE'
          and subject_class_cd = 'PSN';


        -- create table participants as

        SELECT @ROWCOUNT_NO = @@ROWCOUNT;
        INSERT INTO [DBO].[JOB_FLOW_LOG]
        (BATCH_ID,[DATAFLOW_NAME],[PACKAGE_NAME] ,[STATUS_TYPE],[STEP_NUMBER],[STEP_NAME],[ROW_COUNT])
        VALUES(@BATCH_ID,'D_LABTEST','D_LABTEST','START',  @PROC_STEP_NO,@PROC_STEP_NAME,@ROWCOUNT_NO);


        COMMIT TRANSACTION;

        BEGIN TRANSACTION;
        SET @PROC_STEP_NO =  @PROC_STEP_NO + 1 ;
        SET @PROC_STEP_NAME = ' GENERATING TMP_participants ';


        IF OBJECT_ID('#TMP_participants', 'U') IS NOT NULL
            drop table    TMP_participants ;


        select *
        into #TMP_participants
        from #TMP_filter_participants tfp, #TMP_merged_provider tmp
        where tfp.provider_uid_filter= tmp.provider_uid;


        -- create table LAB_TESTinit_a as

        SELECT @ROWCOUNT_NO = @@ROWCOUNT;
        INSERT INTO [DBO].[JOB_FLOW_LOG]
        (BATCH_ID,[DATAFLOW_NAME],[PACKAGE_NAME] ,[STATUS_TYPE],[STEP_NUMBER],[STEP_NAME],[ROW_COUNT])
        VALUES(@BATCH_ID,'D_LABTEST','D_LABTEST','START',  @PROC_STEP_NO,@PROC_STEP_NAME,@ROWCOUNT_NO);


        COMMIT TRANSACTION;

        BEGIN TRANSACTION;
        SET @PROC_STEP_NO =  @PROC_STEP_NO + 1 ;
        SET @PROC_STEP_NAME = ' GENERATING TMP_LAB_TESTinit_a ';




        IF OBJECT_ID('#TMP_LAB_TESTinit_a', 'U') IS NOT NULL
            drop table  TMP_LAB_TESTinit_a;



        select	distinct
            obs.observation_uid			 as LAB_TEST_uid_Test,
            cast(1 as bigint)			 as parent_test_pntr,
            obs.observation_uid			 as LAB_TEST_pntr,
            obs.activity_to_time		 as LAB_TEST_dt,
            obs.method_cd				 as test_method_cd,
            cast(1 as bigint)			 as root_ordered_test_pntr,
            obs.method_desc_txt			 as test_method_cd_desc,
            obs.priority_cd				 as priority_cd,
            obs.target_site_cd			 as specimen_site,
            obs.target_site_desc_txt  	 as SPECIMEN_SITE_desc,
            obs.txt						 as Clinical_information,
            obs.obs_domain_cd_st_1 		 as LAB_TEST_Type,
            obs.cd					 	 as LAB_TEST_cd,
            obs.Cd_desc_txt				 as LAB_TEST_cd_desc,
            obs.Cd_system_cd			 as LAB_TEST_cd_sys_cd,
            obs.Cd_system_desc_txt		 as LAB_TEST_cd_sys_nm,
            obs.Alt_cd					 as Alt_LAB_TEST_cd,
            obs.Alt_cd_desc_txt			 as Alt_LAB_TEST_cd_desc,
            obs.Alt_cd_system_cd		 as Alt_LAB_TEST_cd_sys_cd,
            obs.Alt_cd_system_desc_txt 	 as Alt_LAB_TEST_cd_sys_nm,
            obs.effective_from_time	 	 as specimen_collection_dt,
            obs.local_id				 as lab_rpt_local_id,
            obs.shared_ind				 as lab_rpt_share_ind,
            obs.PROGRAM_JURISDICTION_OID as oid,
            obs.record_status_cd          as record_status_cd,
            obs.record_status_cd          as record_status_cd_for_result,
            obs.STATUS_CD	   		 	 as lab_rpt_status,
            obs.ADD_TIME				 as LAB_RPT_CREATED_DT,
            obs.ADD_USER_ID  		 	 as LAB_RPT_CREATED_BY,
            obs.rpt_to_state_time  		 as LAB_RPT_RECEIVED_BY_PH_DT,
            obs.LAST_CHG_TIME 			 as LAB_RPT_LAST_UPDATE_DT,
            obs.LAST_CHG_USER_ID		 as LAB_RPT_LAST_UPDATE_BY,
            obs.electronic_ind			 as ELR_IND,
            obs.jurisdiction_cd		     as Jurisdiction_cd,
            cast(null as [varchar](50)) as JURISDICTION_NM, -- VS put(obs.jurisdiction_cd, $JURCODE.)  as JURISDICTION_NM,
            obs.observation_uid			as Lab_Rpt_Uid,
            /*obs.PROG_AREA_CD,*/
            obs.activity_to_time   	 	as resulted_lab_report_date,
            obs.activity_to_time   	 	as sus_lab_report_date,
            loinc_con.condition_cd as condition_cd,
            cvg.code_short_desc_txt		as LAB_TEST_status,
            obs.PROCESSING_DECISION_CD
        into #TMP_LAB_TESTinit_a
        from #TMP_updated_observation_List tuol
                 left outer join nbs_odse..observation as obs on tuol.OBSERVATION_UID=obs.OBSERVATION_UID
                 left join   nbs_srte..loinc_condition as loinc_con 	on obs.cd = loinc_con.loinc_cd
                 left join 	nbs_srte..code_value_general  as cvg on obs.status_cd = cvg.code 	and cvg.code_set_nm = 'ACT_OBJ_ST'
        where obs.obs_domain_cd_st_1 in ('Order','Result','R_Order','R_Result', 'I_Order', 'I_Result', 'Order_rslt')
          and (obs.CTRL_CD_DISPLAY_FORM = 'LabReport' or obs.CTRL_CD_DISPLAY_FORM = 'LabReportMorb' or obs.CTRL_CD_DISPLAY_FORM is null)
        --order by obs.OBSERVATION_UID
        ;

        update TMP_LAB_TESTinit_a
        set jurisdiction_nm = (
            select code_short_desc_txt
            from nbs_srte..jurisdiction_code where code= TMP_LAB_TESTinit_a.Jurisdiction_cd and code_set_nm = 'S_JURDIC_C'
        )
        where Jurisdiction_cd is not null
        ;


        --vs PROC SORT DATA=s_edx_document NODUPKEY OUT=s_edx_document; BY act_uid;

        /* --VS --*/
        -- create table s_edx_document as

        SELECT @ROWCOUNT_NO = @@ROWCOUNT;
        INSERT INTO [DBO].[JOB_FLOW_LOG]
        (BATCH_ID,[DATAFLOW_NAME],[PACKAGE_NAME] ,[STATUS_TYPE],[STEP_NUMBER],[STEP_NAME],[ROW_COUNT])
        VALUES(@BATCH_ID,'D_LABTEST','D_LABTEST','START',  @PROC_STEP_NO,@PROC_STEP_NAME,@ROWCOUNT_NO);


        COMMIT TRANSACTION;

        BEGIN TRANSACTION;
        SET @PROC_STEP_NO =  @PROC_STEP_NO + 1 ;
        SET @PROC_STEP_NAME = ' GENERATING TMP_s_edx_document ';


        IF OBJECT_ID('#TMP_s_edx_document', 'U') IS NOT NULL
            drop table #TMP_s_edx_document;

        select EDX_Document_uid,
               act_uid,
               add_time ,
               CONVERT(varchar, add_time, 101) as add_timeSt, --VS put(datepart(add_time),mmddyy10.) as add_timeSt
               cast( null as varchar(500)) as document_link
        into #TMP_s_edx_document
        from #TMP_s_edx_document1
        ;

        /*
      options fmtsearch=(nbsfmt);
      DATA s_edx_document;
      set s_edx_document;
      LENGTH document_link $500;
      document_link =compbl('<a href="#" '|| compress('onClick="window.open(''/nbs/viewELRDocument.do?method=viewELRDocument&documentUid='
      || EDX_Document_uid || ' &dateReceivedHidden=' || add_timeSt ||''' ,''DocumentViewer'',''width=900,height=800,left=0,top=0,
      menubar=no,titlebar=no,toolbar=no,scrollbars=yes,location=no'');">View Lab Document</a>'));

      */

        update #TMP_s_edx_document
        set document_link =('<a href="#" '+ replace(('onClick="window.open(''/nbs/viewELRDocument.do?method=viewELRDocument&documentUid='
            + cast(EDX_Document_uid as varchar) + ' &dateReceivedHidden=' + add_timeSt +''' ,''DocumentViewer'',''width=900,height=800,left=0,top=0,
						menubar=no,titlebar=no,toolbar=no,scrollbars=yes,location=no'');">View Lab Document</a>'),' ',''));


        -- create table LAB_TESTinit as

        SELECT @ROWCOUNT_NO = @@ROWCOUNT;
        INSERT INTO [DBO].[JOB_FLOW_LOG]
        (BATCH_ID,[DATAFLOW_NAME],[PACKAGE_NAME] ,[STATUS_TYPE],[STEP_NUMBER],[STEP_NAME],[ROW_COUNT])
        VALUES(@BATCH_ID,'D_LABTEST','D_LABTEST','START',  @PROC_STEP_NO,@PROC_STEP_NAME,@ROWCOUNT_NO);


        COMMIT TRANSACTION;

        BEGIN TRANSACTION;
        SET @PROC_STEP_NO =  @PROC_STEP_NO + 1 ;
        SET @PROC_STEP_NAME = ' GENERATING TMP_LAB_TESTinit ';


        IF OBJECT_ID('#TMP_LAB_TESTinit', 'U') IS NOT NULL
            drop table  TMP_LAB_TESTinit;

        select distinct a.*, b.document_link
        into #TMP_LAB_TESTinit
        from #TMP_LAB_TESTinit_a a
                 left outer join #TMP_s_edx_document b on a.LAB_TEST_uid_test=b.act_uid;


        /*
        proc datasets memtype=DATA;
           delete s_edx_document LAB_TESTinit_a;
        */

        -- create table LAB_TEST_mat_init as


        SELECT @ROWCOUNT_NO = @@ROWCOUNT;
        INSERT INTO [DBO].[JOB_FLOW_LOG]
        (BATCH_ID,[DATAFLOW_NAME],[PACKAGE_NAME] ,[STATUS_TYPE],[STEP_NUMBER],[STEP_NAME],[ROW_COUNT])
        VALUES(@BATCH_ID,'D_LABTEST','D_LABTEST','START',  @PROC_STEP_NO,@PROC_STEP_NAME,@ROWCOUNT_NO);


        COMMIT TRANSACTION;

        BEGIN TRANSACTION;
        SET @PROC_STEP_NO =  @PROC_STEP_NO + 1 ;
        SET @PROC_STEP_NAME = ' GENERATING TMP_LAB_TEST_mat_init ';


        IF OBJECT_ID('#TMP_LAB_TEST_mat_init', 'U') IS NOT NULL
            drop table  #TMP_LAB_TEST_mat_init;


        select
            obs.observation_uid			 as LAB_TEST_uid_mat,
            mat.cd						 as specimen_src,
            mat.nm						 as specimen_nm,
            mat.description				 as Specimen_details,
            mat.qty						 as Specimen_collection_vol,
            mat.qty_unit_cd				 as Specimen_collection_vol_unit,
            mat.Cd_desc_txt				 as Specimen_desc,
            mat.Risk_cd					 as Danger_cd,
            mat.Risk_desc_txt			 as Danger_cd_desc
        into #TMP_LAB_TEST_mat_init
        from #TMP_updated_observation_List obs
                 inner join TMP_updated_participant	as par	on obs.observation_uid = par.act_uid
            and par.type_cd ='SPC'
            and par.subject_class_cd = 'MAT'
            and par.act_class_cd = 'OBS'
                 inner join nbs_odse..material	as mat	on par.subject_entity_uid = mat.material_uid order by obs.OBSERVATION_UID
        ;


        -- create table OBS_REASON as


        SELECT @ROWCOUNT_NO = @@ROWCOUNT;
        INSERT INTO [DBO].[JOB_FLOW_LOG]
        (BATCH_ID,[DATAFLOW_NAME],[PACKAGE_NAME] ,[STATUS_TYPE],[STEP_NUMBER],[STEP_NAME],[ROW_COUNT])
        VALUES(@BATCH_ID,'D_LABTEST','D_LABTEST','START',  @PROC_STEP_NO,@PROC_STEP_NAME,@ROWCOUNT_NO);


        COMMIT TRANSACTION;

        BEGIN TRANSACTION;
        SET @PROC_STEP_NO =  @PROC_STEP_NO + 1 ;
        SET @PROC_STEP_NAME = ' GENERATING TMP_OBS_REASON ';


        IF OBJECT_ID('#TMP_OBS_REASON', 'U') IS NOT NULL
            drop table #TMP_OBS_REASON;

        select
            obs.observation_uid,
            rsn.reason_desc_txt,
            rsn.reason_cd ,
            cast( null as varchar(4000)) as REASON_FOR_TEST_DESC,
            cast( null as varchar(2000)) as REASON_FOR_TEST_CD
        into  #TMP_OBS_REASON
        from #TMP_updated_observation_List obs
                 LEFT JOIN  nbs_odse..observation_reason	as rsn	on obs.observation_uid= rsn.observation_uid
        --order by obs.observation_uid
        ;

        /*
        DATA OBS_REASON;
        SET OBS_REASON;
        LENGTH REASON_FOR_TEST_DESC $4000;
        LENGTH REASON_FOR_TEST_CD $2000;
        */

        /*
        --VS
       DO UNTIL(LAST.OBSERVATION_UID);
           SET OBS_REASON;
           BY OBSERVATION_UID NOTSORTED;
       if(LENGTHN(COMPRESS(reason_desc_txt))> 0) and (LENGTHN(COMPRESS(reason_cd))> 0) and (LENGTHN(COMPRESS(REASON_FOR_TEST_DESC))= 0)
           then REASON_FOR_TEST_DESC= COMPRESS( reason_cd|| '(' || reason_desc_txt|| ')' || REASON_FOR_TEST_DESC) ;
       else if(LENGTHN(COMPRESS(reason_desc_txt))> 0) and (LENGTHN(COMPRESS(reason_cd))> 0) and (LENGTHN(COMPRESS(REASON_FOR_TEST_DESC))> 0)
           then REASON_FOR_TEST_DESC= COMPRESS(reason_cd|| '(' || reason_desc_txt|| ')|'|| REASON_FOR_TEST_DESC );

       if(LENGTHN(COMPRESS(reason_cd))> 0) and (LENGTHN(COMPRESS(REASON_FOR_TEST_CD))= 0)
           then REASON_FOR_TEST_CD= COMPRESS(reason_cd);
       else if(LENGTHN(COMPRESS(reason_cd))> 0) and (LENGTHN(COMPRESS(REASON_FOR_TEST_CD))> 0)
           then REASON_FOR_TEST_CD= COMPRESS(reason_cd|| '|' || REASON_FOR_TEST_CD );
       END;
       */

        SELECT @ROWCOUNT_NO = @@ROWCOUNT;
        INSERT INTO [DBO].[JOB_FLOW_LOG]
        (BATCH_ID,[DATAFLOW_NAME],[PACKAGE_NAME] ,[STATUS_TYPE],[STEP_NUMBER],[STEP_NAME],[ROW_COUNT])
        VALUES(@BATCH_ID,'D_LABTEST','D_LABTEST','START',  @PROC_STEP_NO,@PROC_STEP_NAME,@ROWCOUNT_NO);


        COMMIT TRANSACTION;

        BEGIN TRANSACTION;
        SET @PROC_STEP_NO =  @PROC_STEP_NO + 1 ;
        SET @PROC_STEP_NAME = ' GENERATING TMP_OBS_REASON_FINAL ';


        IF OBJECT_ID('#TMP_OBS_REASON_FINAL', 'U') IS NOT NULL
            drop table #TMP_OBS_REASON_FINAL ;


        SELECT DISTINCT LRV.observation_uid,
                        SUBSTRING(
                                (
                                    SELECT '|'+coalesce(ST1.reason_cd+'('+reason_desc_txt+')','')  AS [text()]
                                    FROM #TMP_OBS_REASON ST1
                                    WHERE ST1.observation_uid = LRV.observation_uid
                                    ORDER BY ST1.observation_uid
                                    FOR XML PATH ('')
                                ), 2, 1000) REASON_FOR_TEST_DESC,
                        SUBSTRING(
                                (
                                    SELECT '|'+ST1.reason_cd  AS [text()]
                                    FROM #TMP_OBS_REASON ST1
                                    WHERE ST1.observation_uid = LRV.observation_uid
                                    ORDER BY ST1.observation_uid
                                    FOR XML PATH ('')
                                ), 2, 1000) REASON_FOR_TEST_CD
        into #TMP_OBS_REASON_FINAL
        FROM #TMP_OBS_REASON LRV
        ;

        -- create table LAB_TEST_oth as
        SELECT @ROWCOUNT_NO = @@ROWCOUNT;
        INSERT INTO [DBO].[JOB_FLOW_LOG]
        (BATCH_ID,[DATAFLOW_NAME],[PACKAGE_NAME] ,[STATUS_TYPE],[STEP_NUMBER],[STEP_NAME],[ROW_COUNT])
        VALUES(@BATCH_ID,'D_LABTEST','D_LABTEST','START',  @PROC_STEP_NO,@PROC_STEP_NAME,@ROWCOUNT_NO);


        COMMIT TRANSACTION;

        BEGIN TRANSACTION;
        SET @PROC_STEP_NO =  @PROC_STEP_NO + 1 ;
        SET @PROC_STEP_NAME = ' GENERATING TMP_LAB_TEST_oth ';


        create index idx_TMP_participants_aid on #TMP_participants (act_uid);

        create index idx_TMP_OBS_REASON_FINAL_oid on  #TMP_OBS_REASON_FINAL(observation_uid);



        IF OBJECT_ID('#TMP_LAB_TEST_oth', 'U') IS NOT NULL
            drop table  TMP_LAB_TEST_oth;

        select
            obs.observation_uid			as LAB_TEST_uid_oth,
            oin.interpretation_cd		 as interpretation_flg,
            ai.root_extension_txt 		 as ACCESSION_NBR,
            (REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(obs.REASON_FOR_TEST_DESC,
                                                                     '&#x09;', CHAR(9)),
                                                             '&#x0A;', CHAR(10)),
                                                     '&#x0D;', CHAR(13)),
                                             '&#x20;', CHAR(32)),
                                     '&amp;', CHAR(38)),
                             '&lt;', CHAR(60)),
                     '&gt;', CHAR(62))) REASON_FOR_TEST_DESC,
            obs.REASON_FOR_TEST_CD,
            rtrim(par1.provider_first_name)+' '+rtrim(par1.provider_last_name) as transcriptionist_name,
            par1.person_id_assign_auth_cd as transcriptionist_ass_auth_cd,
            par1.person_id_type_desc  as Transcriptionist_Ass_Auth_Type,
            par1.person_id_val  as transcriptionist_id,
            rtrim(par2.provider_first_name)+' '+rtrim(par2.provider_last_name) as Assistant_Interpreter_Name,
            par2.person_id_assign_auth_cd  as Assistant_inter_ass_auth_cd,
            par2.person_id_type_desc  as Assistant_inter_ass_auth_type,
            par2.person_id_val  as Assistant_interpreter_id,
            rtrim(par3.provider_first_name)+' '+rtrim(par3.provider_last_name) as result_interpreter_name
        into #TMP_LAB_TEST_oth
        from #TMP_OBS_REASON_FINAL obs
                 left join 	nbs_odse..act_id  as ai	on obs.observation_uid = ai.ACT_UID
            and ai.type_cd='FN'
                 left join nbs_odse..observation_interp	as oin on obs.observation_uid = oin.observation_uid
            and oin.INTERPRETATION_CD <> ' '
            /*get transcriptionist*/
                 left join TMP_participants as par1	on obs.observation_uid = par1.act_uid
            and par1.type_cd = 'ENT'
            /*get assistant_interpreter*/
                 left join  TMP_participants as par2 on obs.observation_uid= par2.act_uid
            and par2.type_cd = 'ASS'
            /*get result_interpreter*/
                 left join TMP_participants as par3	on obs.observation_uid= par3.act_uid
            and par3.type_cd = 'VRF'
        --order by obs.OBSERVATION_UID
        ;


        /* --VS
        --VS

        data LAB_TEST1 output;
        merge LAB_TEST_oth LAB_TEST_mat_init LAB_TESTinit;
        by LAB_TEST_uid;

        */

        SELECT @ROWCOUNT_NO = @@ROWCOUNT;
        INSERT INTO [DBO].[JOB_FLOW_LOG]
        (BATCH_ID,[DATAFLOW_NAME],[PACKAGE_NAME] ,[STATUS_TYPE],[STEP_NUMBER],[STEP_NAME],[ROW_COUNT])
        VALUES(@BATCH_ID,'D_LABTEST','D_LABTEST','START',  @PROC_STEP_NO,@PROC_STEP_NAME,@ROWCOUNT_NO);


        COMMIT TRANSACTION;

        BEGIN TRANSACTION;
        SET @PROC_STEP_NO =  @PROC_STEP_NO + 1 ;
        SET @PROC_STEP_NAME = ' GENERATING TMP_LAB_TEST1_uid ';


        IF OBJECT_ID('#TMP_LAB_TEST1_uid', 'U') IS NOT NULL
            drop table #TMP_LAB_TEST1_uid;

        select  LAB_TEST_uid_OTH AS LAB_TEST_uid
        into #TMP_LAB_TEST1_uid
        from  #TMP_LAB_TEST_oth  union
        select LAB_TEST_uid_mat 	 from #TMP_LAB_TEST_mat_init  union
        select LAB_TEST_uid_test	 from #TMP_LAB_TESTinit

        ;

        SELECT @ROWCOUNT_NO = @@ROWCOUNT;
        INSERT INTO [DBO].[JOB_FLOW_LOG]
        (BATCH_ID,[DATAFLOW_NAME],[PACKAGE_NAME] ,[STATUS_TYPE],[STEP_NUMBER],[STEP_NAME],[ROW_COUNT])
        VALUES(@BATCH_ID,'D_LABTEST','D_LABTEST','START',  @PROC_STEP_NO,@PROC_STEP_NAME,@ROWCOUNT_NO);


        COMMIT TRANSACTION;

        BEGIN TRANSACTION;
        SET @PROC_STEP_NO =  @PROC_STEP_NO + 1 ;
        SET @PROC_STEP_NAME = ' GENERATING TMP_LAB_TEST1_TMP ';


        IF OBJECT_ID('#TMP_LAB_TEST1_TMP', 'U') IS NOT NULL
            drop table #TMP_LAB_TEST1_TMP ;


        select  lt1.*,lto.*,ltmi.*,lti.*, lti.Lab_Rpt_Uid as Lab_Rpt_Uid_Test1
        into #TMP_LAB_TEST1_TMP
        from #TMP_LAB_TEST1_uid lt1
                 left outer join #TMP_LAB_TEST_oth  lto on lt1.LAB_TEST_uid = lto.LAB_TEST_uid_oth
                 left outer join #TMP_LAB_TEST_mat_init  ltmi  on lt1.LAB_TEST_uid = ltmi.LAB_TEST_uid_mat
                 left outer join #TMP_LAB_TESTinit  lti on lt1.LAB_TEST_uid = lti.LAB_TEST_uid_Test
        ;

        ALTER TABLE #TMP_LAB_TEST1_TMP 	DROP COLUMN Lab_Rpt_Uid;



        /*
        proc datasets memtype=DATA;
           delete LAB_TEST_mat_init LAB_TESTinit OBS_REASON LAB_TEST_oth;
        */



        /* --vs

        data LabReportMorb (keep=lab_rpt_uid);
        set LAB_TEST1;
        where  LAB_TEST_type in ('Order', 'Result', 'Order_rslt') and  oid =4;

        */

        SELECT @ROWCOUNT_NO = @@ROWCOUNT;
        INSERT INTO [DBO].[JOB_FLOW_LOG]
        (BATCH_ID,[DATAFLOW_NAME],[PACKAGE_NAME] ,[STATUS_TYPE],[STEP_NUMBER],[STEP_NAME],[ROW_COUNT])
        VALUES(@BATCH_ID,'D_LABTEST','D_LABTEST','START',  @PROC_STEP_NO,@PROC_STEP_NAME,@ROWCOUNT_NO);


        COMMIT TRANSACTION;

        BEGIN TRANSACTION;
        SET @PROC_STEP_NO =  @PROC_STEP_NO + 1 ;
        SET @PROC_STEP_NAME = ' GENERATING TMP_LabReportMorb ';


        IF OBJECT_ID('#TMP_LabReportMorb', 'U') IS NOT NULL
            drop table  #TMP_LabReportMorb;


        select *
        into #TMP_LabReportMorb
        from #TMP_LAB_TEST1_TMP
        where  LAB_TEST_type in ('Order', 'Result', 'Order_rslt') and  oid =4
        ;



        -- create table Morb_OID as

        SELECT @ROWCOUNT_NO = @@ROWCOUNT;
        INSERT INTO [DBO].[JOB_FLOW_LOG]
        (BATCH_ID,[DATAFLOW_NAME],[PACKAGE_NAME] ,[STATUS_TYPE],[STEP_NUMBER],[STEP_NAME],[ROW_COUNT])
        VALUES(@BATCH_ID,'D_LABTEST','D_LABTEST','START',  @PROC_STEP_NO,@PROC_STEP_NAME,@ROWCOUNT_NO);


        COMMIT TRANSACTION;

        BEGIN TRANSACTION;
        SET @PROC_STEP_NO =  @PROC_STEP_NO + 1 ;
        SET @PROC_STEP_NAME = ' GENERATING TMP_Morb_OID ';


        IF OBJECT_ID('#TMP_Morb_OID', 'U') IS NOT NULL
            drop table  #TMP_Morb_OID ;

        select l.*,
               o.PROGRAM_JURISDICTION_OID as Morb_oid,
               l.Lab_Rpt_Uid_Test1 as Lab_Rpt_Uid_Mor,
               l.LAB_TEST_uid as LAB_TEST_uid_mor ,
               l.LAB_TEST_uid_oth as LAB_TEST_uid_oth_mor
        into  #TMP_Morb_OID
        from #TMP_LabReportMorb l,
             nbs_odse..act_relationship ar,
             nbs_odse..observation o
        where ar.source_act_uid = l.Lab_Rpt_Uid_Test1
          and ar.target_act_uid = o.observation_uid
          and o.CTRL_CD_DISPLAY_FORM = 'MorbReport'
        ;

        ALTER TABLE #TMP_Morb_OID 	DROP COLUMN Lab_Rpt_Uid_Test1,LAB_TEST_uid,LAB_TEST_uid_oth ;

        /*
        proc sort data = Morb_oid;by lab_rpt_uid;
        proc sort data =  LAB_TEST1;by lab_rpt_uid;

        data LAB_TEST1;
            merge Morb_OID LAB_TEST1;
            by lab_rpt_uid;

        */

        SELECT @ROWCOUNT_NO = @@ROWCOUNT;
        INSERT INTO [DBO].[JOB_FLOW_LOG]
        (BATCH_ID,[DATAFLOW_NAME],[PACKAGE_NAME] ,[STATUS_TYPE],[STEP_NUMBER],[STEP_NAME],[ROW_COUNT])
        VALUES(@BATCH_ID,'D_LABTEST','D_LABTEST','START',  @PROC_STEP_NO,@PROC_STEP_NAME,@ROWCOUNT_NO);


        COMMIT TRANSACTION;

        BEGIN TRANSACTION;
        SET @PROC_STEP_NO =  @PROC_STEP_NO + 1 ;
        SET @PROC_STEP_NAME = ' GENERATING TMP_LAB_TEST1_uid2 ';


        IF OBJECT_ID('#TMP_LAB_TEST1_uid2', 'U') IS NOT NULL
            drop table  #TMP_LAB_TEST1_uid2;

        select  lab_rpt_uid_mor as   lab_rpt_uid
        into #TMP_LAB_TEST1_uid2
        from  #TMP_Morb_OID  union
        select Lab_Rpt_Uid_Test1 	 from  #TMP_LAB_TEST1_TMP

        ;

        SELECT @ROWCOUNT_NO = @@ROWCOUNT;
        INSERT INTO [DBO].[JOB_FLOW_LOG]
        (BATCH_ID,[DATAFLOW_NAME],[PACKAGE_NAME] ,[STATUS_TYPE],[STEP_NUMBER],[STEP_NAME],[ROW_COUNT])
        VALUES(@BATCH_ID,'D_LABTEST','D_LABTEST','START',  @PROC_STEP_NO,@PROC_STEP_NAME,@ROWCOUNT_NO);


        COMMIT TRANSACTION;

        BEGIN TRANSACTION;
        SET @PROC_STEP_NO =  @PROC_STEP_NO + 1 ;
        SET @PROC_STEP_NAME = ' GENERATING TMP_LAB_TEST1 ';


        IF OBJECT_ID('#TMP_LAB_TEST1', 'U') IS NOT NULL
            drop table #TMP_LAB_TEST1 ;


        select  lt1.*,lto.*,tmo.Morb_oid,Cast (null as  [varchar](100)) as PROCESSING_DECISION_DESC
        into #TMP_LAB_TEST1
        from #TMP_LAB_TEST1_uid2 lt1
                 left outer join #TMP_LAB_TEST1_TMP  lto on lt1.lab_rpt_uid = lto.Lab_Rpt_Uid_Test1
                 left outer join #TMP_Morb_OID  tmo on lt1.lab_rpt_uid = tmo.lab_rpt_uid_mor
        ;

        /* --vs
        data LAB_TEST1;
            set LAB_TEST1;
            if morb_oid~=. then oid = morb_oid;
        */
        update #TMP_LAB_TEST1
        set oid = morb_oid
        where rtrim(morb_oid) is not null
        ;




        /* -- VS
        data LAB_TEST1 (drop=morb_oid);
        set LAB_TEST1;
        PROCESSING_DECISION_DESC=PUT(PROCESSING_DECISION_CD,$APROCDNF.);

        */

        update #TMP_LAB_TEST1
        set #TMP_LAB_TEST1.PROCESSING_DECISION_DESC = cvg.[code_short_desc_txt]
        from nbs_srte..Code_value_general cvg,
             #TMP_LAB_TEST1 tlt1
        where cvg.code_set_nm = 'STD_NBS_PROCESSING_DECISION_ALL'
          and   tlt1.PROCESSING_DECISION_CD = cvg.code
          and   tlt1.PROCESSING_DECISION_CD is not null
        ;


        update #TMP_LAB_TEST1
        set PROCESSING_DECISION_DESC = PROCESSING_DECISION_CD
        WHERE PROCESSING_DECISION_CD is not null
          AND PROCESSING_DECISION_DESC IS NULL
        ;

        /**********************************/
        /* update parent of R_Result */

        SELECT @ROWCOUNT_NO = @@ROWCOUNT;
        INSERT INTO [DBO].[JOB_FLOW_LOG]
        (BATCH_ID,[DATAFLOW_NAME],[PACKAGE_NAME] ,[STATUS_TYPE],[STEP_NUMBER],[STEP_NAME],[ROW_COUNT])
        VALUES(@BATCH_ID,'D_LABTEST','D_LABTEST','START',  @PROC_STEP_NO,@PROC_STEP_NAME,@ROWCOUNT_NO);


        COMMIT TRANSACTION;

        BEGIN TRANSACTION;
        SET @PROC_STEP_NO =  @PROC_STEP_NO + 1 ;
        SET @PROC_STEP_NAME = ' GENERATING TMP_R_Result_to_R_Order ';


        IF OBJECT_ID('#TMP_R_Result_to_R_Order', 'U') IS NOT NULL
            drop table #TMP_R_Result_to_R_Order;


        -- create table R_Result_to_R_Order as
        select 	act.source_act_uid		'LAB_TEST_uid',	--as LAB_TEST_uid label='R_Result_uid',
                  act.target_act_uid		'parent_test_pntr'	---as parent_test_pntr label='R_Order_uid'
        into #TMP_R_Result_to_R_Order
        from 	dboTMP_LAB_TEST1 as tst,
            /*R_Result_to_R_Order*/
                nbs_odse..act_relationship	as act
        where	 tst.LAB_TEST_uid = act.source_act_uid
            /*and act.type_cd = 'COMP'*/
          and act.target_class_cd ='OBS'
          and act.source_class_cd ='OBS'
          and tst.LAB_TEST_type IN ('R_Result', 'I_Result')
        ;


        /* update root of R_Result */
        SELECT @ROWCOUNT_NO = @@ROWCOUNT;
        INSERT INTO [DBO].[JOB_FLOW_LOG]
        (BATCH_ID,[DATAFLOW_NAME],[PACKAGE_NAME] ,[STATUS_TYPE],[STEP_NUMBER],[STEP_NAME],[ROW_COUNT])
        VALUES(@BATCH_ID,'D_LABTEST','D_LABTEST','START',  @PROC_STEP_NO,@PROC_STEP_NAME,@ROWCOUNT_NO);


        COMMIT TRANSACTION;

        BEGIN TRANSACTION;
        SET @PROC_STEP_NO =  @PROC_STEP_NO + 1 ;
        SET @PROC_STEP_NAME = ' GENERATING TMP_R_Result_to_R_Order_to_Order ';


        IF OBJECT_ID('#TMP_R_Result_to_R_Order_to_Order', 'U') IS NOT NULL
            drop table   TMP_R_Result_to_R_Order_to_Order;


        -- create table R_Result_to_R_Order_to_Order as
        select 	tst.*,
                  coalesce(tst2.record_status_cd, tst3.record_status_cd, tst4.record_status_cd )
                                                     as record_status_cd_for_result_drug ,
                  act2.target_act_uid				as root_thru_srpt,
                  act3.target_act_uid				as root_thru_refr,
                  coalesce(act2.target_act_uid, 	act4.target_act_uid)
                                                     as root_ordered_test_pntr -- VS label='Order uid'
        into TMP_R_Result_to_R_Order_to_Order
        from 	TMP_R_Result_to_R_Order	as tst
                    /*R_Order to Order */
                    left join	nbs_odse..act_relationship as act2 		on tst.parent_test_pntr = act2.source_act_uid
            and act2.type_cd = 'SPRT'
            and act2.target_class_cd = 'OBS'
            and act2.source_class_cd ='OBS'
                    left join 	TMP_LAB_TEST1 as tst2    on   tst2.LAB_TEST_uid = act2.target_act_uid

            /*R_Order to Result to Order */
                    left join	nbs_odse..act_relationship as act3	on tst.parent_test_pntr  = act3.source_act_uid
            and act3.type_cd = 'REFR' 		and act3.target_class_cd = 'OBS'
            and act3.source_class_cd ='OBS'
                    left join 	TMP_LAB_TEST1 as tst3    on   tst3.LAB_TEST_uid = act3.target_act_uid
                    left join	nbs_odse..act_relationship as act4 	on act3.target_act_uid = act4.source_act_uid
            and act4.type_cd = 'COMP'
            and act4.target_class_cd = 'OBS'
            and act4.source_class_cd ='OBS'
                    left join 	TMP_LAB_TEST1 as tst4    on   tst4.LAB_TEST_uid = act4.target_act_uid
        ;


        /*
        proc sort data = R_Result_to_R_Order_to_Order;
        by LAB_TEST_uid;
        proc sort data = LAB_TEST1;
        by LAB_TEST_uid;
        */


        /*
        ---VS *******

        data LAB_TEST1;
        merge LAB_TEST1 R_Result_to_R_Order_to_Order
            (keep=LAB_TEST_uid parent_test_pntr root_ordered_test_pntr
             record_status_cd_for_result_drug)ilts_to
            ;
        by LAB_TEST_uid;
        */

        SELECT @ROWCOUNT_NO = @@ROWCOUNT;
        INSERT INTO [DBO].[JOB_FLOW_LOG]
        (BATCH_ID,[DATAFLOW_NAME],[PACKAGE_NAME] ,[STATUS_TYPE],[STEP_NUMBER],[STEP_NAME],[ROW_COUNT])
        VALUES(@BATCH_ID,'D_LABTEST','D_LABTEST','START',  @PROC_STEP_NO,@PROC_STEP_NAME,@ROWCOUNT_NO);


        COMMIT TRANSACTION;

        BEGIN TRANSACTION;
        SET @PROC_STEP_NO =  @PROC_STEP_NO + 1 ;
        SET @PROC_STEP_NAME = ' GENERATING TMP_LAB_TEST1_testuid ';


        IF OBJECT_ID('#TMP_LAB_TEST1_testuid', 'U') IS NOT NULL
            drop table  TMP_LAB_TEST1_testuid;


        select lt1.LAB_TEST_uid
        into TMP_LAB_TEST1_testuid
        from TMP_LAB_TEST1 lt1
        union
        select rrr.LAB_TEST_uid
        from TMP_R_Result_to_R_Order_to_Order rrr
        ;


        SELECT @ROWCOUNT_NO = @@ROWCOUNT;
        INSERT INTO [DBO].[JOB_FLOW_LOG]
        (BATCH_ID,[DATAFLOW_NAME],[PACKAGE_NAME] ,[STATUS_TYPE],[STEP_NUMBER],[STEP_NAME],[ROW_COUNT])
        VALUES(@BATCH_ID,'D_LABTEST','D_LABTEST','START',  @PROC_STEP_NO,@PROC_STEP_NAME,@ROWCOUNT_NO);


        COMMIT TRANSACTION;

        BEGIN TRANSACTION;
        SET @PROC_STEP_NO =  @PROC_STEP_NO + 1 ;
        SET @PROC_STEP_NAME = ' GENERATING TMP_LAB_TEST1_final ';


        IF OBJECT_ID('#TMP_LAB_TEST1_final', 'U') IS NOT NULL
            drop table  TMP_LAB_TEST1_final;

        select dimc.LAB_TEST_uid as LAB_TEST_uid_final,
               tlt1.*,
               trr.[record_status_cd_for_result_drug] ,
               trr.[root_thru_srpt] ,
               trr.[root_thru_refr] ,
               coalesce(trr.parent_test_pntr,tlt1.parent_test_pntr) as parent_test_pntr1,
               coalesce(trr.root_ordered_test_pntr,tlt1.root_ordered_test_pntr) as root_ordered_test_pntr1
        into #TMP_LAB_TEST1_final
        from #TMP_LAB_TEST1_testuid DIMC
                 LEFT OUTER JOIN  #TMP_LAB_TEST1 tlt1 ON  tlt1.LAB_TEST_uid  =  dimc.LAB_TEST_uid
                 LEFT OUTER JOIN  #TMP_R_Result_to_R_Order_to_Order trr ON  trr.LAB_TEST_uid  =  dimc.LAB_TEST_uid
        ;



        update TMP_LAB_TEST1_final
        set  parent_test_pntr = parent_test_pntr1,
             root_ordered_test_pntr = root_ordered_test_pntr1
        ;



        /* update root order test and parent of R_Order */


        -- create table R_Order_to_Result as


        SELECT @ROWCOUNT_NO = @@ROWCOUNT;
        INSERT INTO [DBO].[JOB_FLOW_LOG]
        (BATCH_ID,[DATAFLOW_NAME],[PACKAGE_NAME] ,[STATUS_TYPE],[STEP_NUMBER],[STEP_NAME],[ROW_COUNT])
        VALUES(@BATCH_ID,'D_LABTEST','D_LABTEST','START',  @PROC_STEP_NO,@PROC_STEP_NAME,@ROWCOUNT_NO);


        COMMIT TRANSACTION;

        BEGIN TRANSACTION;
        SET @PROC_STEP_NO =  @PROC_STEP_NO + 1 ;
        SET @PROC_STEP_NAME = ' GENERATING TMP_R_Order_to_Result ';


        IF OBJECT_ID('#TMP_R_Order_to_Result', 'U') IS NOT NULL
            drop table  TMP_R_Order_to_Result;


        select 	act.source_act_uid			as LAB_TEST_uid ,--label='R_Order_uid',
                  act.target_act_uid			as parent_test_pntr, --label='Result_uid',
                  act2.target_act_uid			as root_ordered_test_pntr, --label='Order uid',
                  tst2.record_status_cd as record_status_cd --label='record_status_cd_for_result'
        into TMP_R_Order_to_Result
        from 	TMP_LAB_TEST1_final as tst,
                TMP_LAB_TEST1_final as tst2,
                nbs_odse..act_relationship	as act,
                nbs_odse..act_relationship	as act2
        where tst.LAB_TEST_type IN( 'R_Order','I_Order')
          and tst.LAB_TEST_uid = act.source_act_uid
          and act.type_cd = 'REFR'
          and act.target_class_cd ='OBS'
          and act.source_class_cd ='OBS'
          and act.target_act_uid = act2.source_act_uid
          and act2.type_cd = 'COMP'
          and act2.target_class_cd ='OBS'
          and act2.source_class_cd ='OBS'
          and tst2.LAB_TEST_uid = act2.target_act_uid
        ;



        /*VS
        data LAB_TEST1;
        merge LAB_TEST1 R_Order_to_Result;
        by LAB_TEST_uid;
        */




        /* update root and parent of Result */


        SELECT @ROWCOUNT_NO = @@ROWCOUNT;
        INSERT INTO [DBO].[JOB_FLOW_LOG]
        (BATCH_ID,[DATAFLOW_NAME],[PACKAGE_NAME] ,[STATUS_TYPE],[STEP_NUMBER],[STEP_NAME],[ROW_COUNT])
        VALUES(@BATCH_ID,'D_LABTEST','D_LABTEST','START',  @PROC_STEP_NO,@PROC_STEP_NAME,@ROWCOUNT_NO);


        COMMIT TRANSACTION;

        BEGIN TRANSACTION;
        SET @PROC_STEP_NO =  @PROC_STEP_NO + 1 ;
        SET @PROC_STEP_NAME = ' GENERATING TMP_LAB_TEST1_final_testuid ';


        IF OBJECT_ID('#TMP_LAB_TEST1_final_testuid', 'U') IS NOT NULL
            drop table  TMP_LAB_TEST1_final_testuid;


        select lt1.LAB_TEST_uid
        into TMP_LAB_TEST1_final_testuid
        from TMP_LAB_TEST1_final lt1
        union
        select rrr.LAB_TEST_uid
        from TMP_R_Order_to_Result rrr
        ;


        SELECT @ROWCOUNT_NO = @@ROWCOUNT;
        INSERT INTO [DBO].[JOB_FLOW_LOG]
        (BATCH_ID,[DATAFLOW_NAME],[PACKAGE_NAME] ,[STATUS_TYPE],[STEP_NUMBER],[STEP_NAME],[ROW_COUNT])
        VALUES(@BATCH_ID,'D_LABTEST','D_LABTEST','START',  @PROC_STEP_NO,@PROC_STEP_NAME,@ROWCOUNT_NO);


        COMMIT TRANSACTION;

        BEGIN TRANSACTION;
        SET @PROC_STEP_NO =  @PROC_STEP_NO + 1 ;
        SET @PROC_STEP_NAME = ' GENERATING TMP_LAB_TEST1_final_result ';


        IF OBJECT_ID('#TMP_LAB_TEST1_final_result', 'U') IS NOT NULL
            drop table  TMP_LAB_TEST1_final_result;

        /*
        select dimc.LAB_TEST_uid as LAB_TEST_uid_final_result,
                tlt1.*,
                coalesce(tlt1.[record_status_cd], trr.[record_status_cd]) as record_status_cd2,
               coalesce(tlt1.parent_test_pntr,trr.parent_test_pntr) as parent_test_pntr2,
               coalesce(tlt1.root_ordered_test_pntr,trr.root_ordered_test_pntr) as root_ordered_test_pntr2
        into TMP_LAB_TEST1_final_result
        from#TMP_LAB_TEST1_final_testuid DIMC
                              LEFT OUTER JOIN  #TMP_LAB_TEST1_final tlt1 ON  tlt1.LAB_TEST_uid  =  dimc.LAB_TEST_uid
                              LEFT OUTER JOIN#TMP_R_Order_to_Result trr ON  trr.LAB_TEST_uid  =  dimc.LAB_TEST_uid
        ;

        */


        select dimc.LAB_TEST_uid as LAB_TEST_uid_final_result,
               tlt1.*,
               coalesce(trr.[record_status_cd], tlt1.[record_status_cd]) as record_status_cd2,
               coalesce(trr.parent_test_pntr,tlt1.parent_test_pntr) as parent_test_pntr2,
               coalesce(trr.root_ordered_test_pntr,tlt1.root_ordered_test_pntr) as root_ordered_test_pntr2
        into #TMP_LAB_TEST1_final_result
        from #TMP_LAB_TEST1_final_testuid DIMC
                 LEFT OUTER JOIN  #TMP_LAB_TEST1_final tlt1 ON  tlt1.LAB_TEST_uid  =  dimc.LAB_TEST_uid
                 LEFT OUTER JOIN  #TMP_R_Order_to_Result trr ON  trr.LAB_TEST_uid  =  dimc.LAB_TEST_uid
        ;

        update TMP_LAB_TEST1_final_result
        set record_status_cd = record_status_cd2,
            parent_test_pntr = parent_test_pntr2,
            root_ordered_test_pntr = root_ordered_test_pntr2
        ;


        -- create table Result_to_Order as


        SELECT @ROWCOUNT_NO = @@ROWCOUNT;
        INSERT INTO [DBO].[JOB_FLOW_LOG]
        (BATCH_ID,[DATAFLOW_NAME],[PACKAGE_NAME] ,[STATUS_TYPE],[STEP_NUMBER],[STEP_NAME],[ROW_COUNT])
        VALUES(@BATCH_ID,'D_LABTEST','D_LABTEST','START',  @PROC_STEP_NO,@PROC_STEP_NAME,@ROWCOUNT_NO);


        COMMIT TRANSACTION;

        BEGIN TRANSACTION;
        SET @PROC_STEP_NO =  @PROC_STEP_NO + 1 ;
        SET @PROC_STEP_NAME = ' GENERATING TMP_Result_to_Order ';


        IF OBJECT_ID('#TMP_Result_to_Order', 'U') IS NOT NULL
            drop table  TMP_Result_to_Order;

        select 	act.source_act_uid			as LAB_TEST_uid ,--label='Result_uid',
                  act.target_act_uid			as parent_test_pntr ,--label='Order_uid',
                  act.target_act_uid			as root_ordered_test_pntr ,--label='Order uid',
                  tst2.record_status_cd as record_status_cd --label='record_status_cd_for_result'
        into  TMP_Result_to_Order
        from 	TMP_LAB_TEST1_final_result as tst,
                TMP_LAB_TEST1_final_result as tst2,
                nbs_odse..act_relationship	as act
        where	tst.LAB_TEST_uid = act.source_act_uid
          and tst.LAB_TEST_type in ('Result', 'Order_rslt')
          and act.type_cd = 'COMP'
          and act.target_class_cd ='OBS'
          and act.source_class_cd ='OBS'
          and tst2.LAB_TEST_uid = act.target_act_uid
        ;


        /*
        data LAB_TEST1;
        merge LAB_TEST1 Result_to_Order;
        by LAB_TEST_uid;
        */


        SELECT @ROWCOUNT_NO = @@ROWCOUNT;
        INSERT INTO [DBO].[JOB_FLOW_LOG]
        (BATCH_ID,[DATAFLOW_NAME],[PACKAGE_NAME] ,[STATUS_TYPE],[STEP_NUMBER],[STEP_NAME],[ROW_COUNT])
        VALUES(@BATCH_ID,'D_LABTEST','D_LABTEST','START',  @PROC_STEP_NO,@PROC_STEP_NAME,@ROWCOUNT_NO);


        COMMIT TRANSACTION;

        BEGIN TRANSACTION;
        SET @PROC_STEP_NO =  @PROC_STEP_NO + 1 ;
        SET @PROC_STEP_NAME = ' GENERATING TMP_LAB_TEST1_final_orderuid ';


        IF OBJECT_ID('#TMP_LAB_TEST1_final_orderuid', 'U') IS NOT NULL
            drop table  TMP_LAB_TEST1_final_orderuid;


        select lt1.LAB_TEST_uid
        into TMP_LAB_TEST1_final_orderuid
        from TMP_LAB_TEST1_final_result lt1
        union
        select rrr.LAB_TEST_uid
        from TMP_Result_to_Order rrr
        ;


        SELECT @ROWCOUNT_NO = @@ROWCOUNT;
        INSERT INTO [DBO].[JOB_FLOW_LOG]
        (BATCH_ID,[DATAFLOW_NAME],[PACKAGE_NAME] ,[STATUS_TYPE],[STEP_NUMBER],[STEP_NAME],[ROW_COUNT])
        VALUES(@BATCH_ID,'D_LABTEST','D_LABTEST','START',  @PROC_STEP_NO,@PROC_STEP_NAME,@ROWCOUNT_NO);


        COMMIT TRANSACTION;

        BEGIN TRANSACTION;
        SET @PROC_STEP_NO =  @PROC_STEP_NO + 1 ;
        SET @PROC_STEP_NAME = ' GENERATING TMP_LAB_TEST1_final_order ';


        create index idx_TMP_LAB_TEST1_final_orderuid  on #TMP_LAB_TEST1_final_orderuid (LAB_TEST_uid) ;

        create index idx_TMP_LAB_TEST1_final_result_uid on #TMP_LAB_TEST1_final_result(LAB_TEST_uid) ;

        create index idx_TMP_Result_to_Order_uid on #TMP_Result_to_Order(LAB_TEST_uid) ;


        IF OBJECT_ID('#TMP_LAB_TEST1_final_order', 'U') IS NOT NULL
            drop table  TMP_LAB_TEST1_final_order;

        select distinct dimc.LAB_TEST_uid as LAB_TEST_uid_final_order,
                        tlt1.*,
                        coalesce( trr.[record_status_cd],tlt1.[record_status_cd]) as record_status_cd3,
                        coalesce(trr.parent_test_pntr,tlt1.parent_test_pntr) as parent_test_pntr3,
                        coalesce(trr.root_ordered_test_pntr,tlt1.root_ordered_test_pntr) as root_ordered_test_pntr3
        into TMP_LAB_TEST1_final_order
        from #TMP_LAB_TEST1_final_orderuid DIMC
                 LEFT OUTER JOIN  #TMP_LAB_TEST1_final_result tlt1 ON  tlt1.LAB_TEST_uid  =  dimc.LAB_TEST_uid
                 LEFT OUTER JOIN  #TMP_Result_to_Order trr ON  trr.LAB_TEST_uid  =  dimc.LAB_TEST_uid
        ;


        update #TMP_LAB_TEST1_final_order
        set record_status_cd = record_status_cd3,
            parent_test_pntr = parent_test_pntr3,
            root_ordered_test_pntr = root_ordered_test_pntr3
        ;






        /*
        proc datasets memtype=DATA;
           delete Result_to_Order R_Order_to_Result R_Result_to_R_Order_to_Order;
        */



        /*	update root and parent of Order, which is itself*/
        /*data TMP_LAB_TEST1_final_order;
        set TMP_LAB_TEST1_final_order;
            if LAB_TEST_type = 'Order' then do;
                parent_test_pntr = LAB_TEST_pntr;
                root_ordered_test_pntr = LAB_TEST_pntr;
            end;
        */

        update #TMP_LAB_TEST1_final_order
        set parent_test_pntr = LAB_TEST_pntr,
            root_ordered_test_pntr = LAB_TEST_pntr
        where LAB_TEST_type = 'Order'
        ;



        /****creating Root_Ordered_Test_Nm column in LAB_TEST***/

        create index idx_root_ordered_test_pntr on TMP_LAB_TEST1_final_order(root_ordered_test_pntr);


        -- create table LAB_TEST2 as

        SELECT @ROWCOUNT_NO = @@ROWCOUNT;
        INSERT INTO [DBO].[JOB_FLOW_LOG]
        (BATCH_ID,[DATAFLOW_NAME],[PACKAGE_NAME] ,[STATUS_TYPE],[STEP_NUMBER],[STEP_NAME],[ROW_COUNT])
        VALUES(@BATCH_ID,'D_LABTEST','D_LABTEST','START',  @PROC_STEP_NO,@PROC_STEP_NAME,@ROWCOUNT_NO);


        COMMIT TRANSACTION;

        BEGIN TRANSACTION;
        SET @PROC_STEP_NO =  @PROC_STEP_NO + 1 ;
        SET @PROC_STEP_NAME = ' GENERATING TMP_LAB_TEST2 ';



        create index idx_TMP_LAB_TEST1_final_order_pntr on  TMP_LAB_TEST1_final_order(root_ordered_test_pntr);


        IF OBJECT_ID('#TMP_LAB_TEST2', 'U') IS NOT NULL
            drop table  #TMP_LAB_TEST2 ;


        select tst.*,
               obs.Cd_desc_txt 'Root_Ordered_Test_Nm'
        into #TMP_LAB_TEST2
        from  #TMP_LAB_TEST1_final_order as tst
                  left outer join nbs_odse..observation as obs on  tst.root_ordered_test_pntr = obs.observation_uid
        ;


        /******creating LAB_TEST column in LAB_TEST***/

        -- create table LAB_TEST3 as

        SELECT @ROWCOUNT_NO = @@ROWCOUNT;
        INSERT INTO [DBO].[JOB_FLOW_LOG]
        (BATCH_ID,[DATAFLOW_NAME],[PACKAGE_NAME] ,[STATUS_TYPE],[STEP_NUMBER],[STEP_NAME],[ROW_COUNT])
        VALUES(@BATCH_ID,'D_LABTEST','D_LABTEST','START',  @PROC_STEP_NO,@PROC_STEP_NAME,@ROWCOUNT_NO);


        COMMIT TRANSACTION;

        BEGIN TRANSACTION;
        SET @PROC_STEP_NO =  @PROC_STEP_NO + 1 ;
        SET @PROC_STEP_NAME = ' GENERATING TMP_LAB_TEST3 ';


        IF OBJECT_ID('#TMP_LAB_TEST3', 'U') IS NOT NULL
            drop table  TMP_LAB_TEST3;

        select tst.*, obs.Cd_desc_txt 'Parent_Test_Nm'
        into #TMP_LAB_TEST3
        from  #TMP_LAB_TEST2 as tst
                  left outer join nbs_odse..observation as obs on tst.parent_test_pntr = obs.observation_uid
        ;


        /*Setting SPECIMEN_ADD_TIME &  SPECIMEN_LAST_CHANGE_TIME*/

        create index idx_LAB_TEST_uid on TMP_LAB_TEST3(LAB_TEST_uid);


        -- create table LAB_TEST4 as

        SELECT @ROWCOUNT_NO = @@ROWCOUNT;
        INSERT INTO [DBO].[JOB_FLOW_LOG]
        (BATCH_ID,[DATAFLOW_NAME],[PACKAGE_NAME] ,[STATUS_TYPE],[STEP_NUMBER],[STEP_NAME],[ROW_COUNT])
        VALUES(@BATCH_ID,'D_LABTEST','D_LABTEST','START',  @PROC_STEP_NO,@PROC_STEP_NAME,@ROWCOUNT_NO);


        COMMIT TRANSACTION;

        BEGIN TRANSACTION;
        SET @PROC_STEP_NO =  @PROC_STEP_NO + 1 ;
        SET @PROC_STEP_NAME = ' GENERATING TMP_LAB_TEST4 ';


        IF OBJECT_ID('#TMP_LAB_TEST4', 'U') IS NOT NULL
            drop table #TMP_LAB_TEST4 ;


        select tst.*,
               obs.add_time as SPECIMEN_ADD_TIME,
               obs1.last_chg_time as SPECIMEN_LAST_CHANGE_TIME
        into #TMP_LAB_TEST4
        from #TMP_LAB_TEST3 as tst
                 left join nbs_odse..observation as obs	on tst.LAB_TEST_uid = obs.observation_uid
            and obs.obs_domain_cd_st_1 = 'Order'
                 left join nbs_odse..observation as obs1	on tst.LAB_TEST_uid = obs1.observation_uid
            and obs1.obs_domain_cd_st_1 = 'Order'
        ;


        /*Issue arose when the OID value was set to 4 for Result, R_Order & R_Result which resulted in
        Resulted test values not populating the line list lab report. This work around will get the value
        of the Order Test OID and set the values of its children (Result, R_Result) to the same value.
        This fix will bring up the resulted values in the line list.
        */


        -- create table order_test as



        SELECT @ROWCOUNT_NO = @@ROWCOUNT;
        INSERT INTO [DBO].[JOB_FLOW_LOG]
        (BATCH_ID,[DATAFLOW_NAME],[PACKAGE_NAME] ,[STATUS_TYPE],[STEP_NUMBER],[STEP_NAME],[ROW_COUNT])
        VALUES(@BATCH_ID,'D_LABTEST','D_LABTEST','START',  @PROC_STEP_NO,@PROC_STEP_NAME,@ROWCOUNT_NO);


        COMMIT TRANSACTION;

        BEGIN TRANSACTION;
        SET @PROC_STEP_NO =  @PROC_STEP_NO + 1 ;
        SET @PROC_STEP_NAME = ' GENERATING TMP_order_test ';


        IF OBJECT_ID('#TMP_order_test', 'U') IS NOT NULL
            drop table #TMP_order_test ;



        select
            oid ,
            root_ordered_test_pntr
        into TMP_order_test
        from TMP_LAB_TEST4
        where LAB_TEST_Type = 'Order' and oid <> 4
        ;


        create index idx_root_ordered_test_pntr on TMP_LAB_TEST4(root_ordered_test_pntr);
        create index idx_root_ordered_test_pntr on TMP_order_test(root_ordered_test_pntr);


        alter table TMP_LAB_TEST4 drop column oid ;


        /*note: When the OID is null that means this lab report is needing assignment of jurisdiction*/

        -- create table LAB_TEST as

        --select ' I AM HERE';


        SELECT @ROWCOUNT_NO = @@ROWCOUNT;
        INSERT INTO [DBO].[JOB_FLOW_LOG]
        (BATCH_ID,[DATAFLOW_NAME],[PACKAGE_NAME] ,[STATUS_TYPE],[STEP_NUMBER],[STEP_NAME],[ROW_COUNT])
        VALUES(@BATCH_ID,'D_LABTEST','D_LABTEST','START',  @PROC_STEP_NO,@PROC_STEP_NAME,@ROWCOUNT_NO);


        COMMIT TRANSACTION;

        BEGIN TRANSACTION;
        SET @PROC_STEP_NO =  @PROC_STEP_NO + 1 ;
        SET @PROC_STEP_NAME = ' GENERATING TMP_LAB_TEST ';


        IF OBJECT_ID('#TMP_LAB_TEST', 'U') IS NOT NULL
            drop table #TMP_LAB_TEST ;



        select distinct
            lab.*,
            ord.oid as order_oid --VS
        into TMP_LAB_TEST
        from TMP_LAB_TEST4 lab
                 left join	TMP_order_test ord	on lab.root_ordered_test_pntr=ord.root_ordered_test_pntr
        ;

        /*Issue arrose where only the Order Test and not its related Result, R_Result were not showing. This
        will resolve this isse by merging order test specific attribute values into the Result & R_Result records*/

        /* -- VS ********

        data Merge_Order
        (keep = root_ordered_test_pntr
        ACCESSION_NBR
        LAB_RPT_CREATED_BY
        LAB_RPT_CREATED_DT
        JURISDICTION_CD
        JURISDICTION_NM
        LAB_TEST_dt
        specimen_collection_dt
        LAB_RPT_RECEIVED_BY_PH_DT
        LAB_RPT_LAST_UPDATE_DT
        LAB_RPT_LAST_UPDATE_BY
        ELR_IND
        specimen_src
        specimen_site
        Specimen_desc
        SPECIMEN_SITE_desc
        LAB_RPT_LOCAL_ID
        record_status_cd
        );
        set LAB_TEST;
        Where LAB_TEST_Type = 'Order';


        data Merge_Order;
        set Merge_Order;
        If record_status_cd = '' then record_status_cd = 'ACTIVE';
            If record_status_cd = 'UNPROCESSED' then record_status_cd = 'ACTIVE';
            If record_status_cd = 'UNPROCESSED_PREV_D' then record_status_cd = 'ACTIVE';
            If record_status_cd = 'PROCESSED' then record_status_cd = 'ACTIVE';
            If record_status_cd = 'LOG_DEL' then record_status_cd = 'INACTIVE';

        */

        SELECT @ROWCOUNT_NO = @@ROWCOUNT;
        INSERT INTO [DBO].[JOB_FLOW_LOG]
        (BATCH_ID,[DATAFLOW_NAME],[PACKAGE_NAME] ,[STATUS_TYPE],[STEP_NUMBER],[STEP_NAME],[ROW_COUNT])
        VALUES(@BATCH_ID,'D_LABTEST','D_LABTEST','START',  @PROC_STEP_NO,@PROC_STEP_NAME,@ROWCOUNT_NO);


        COMMIT TRANSACTION;

        BEGIN TRANSACTION;
        SET @PROC_STEP_NO =  @PROC_STEP_NO + 1 ;
        SET @PROC_STEP_NAME = ' GENERATING TMP_Merge_Order ';


        IF OBJECT_ID('#TMP_Merge_Order', 'U') IS NOT NULL
            drop table  TMP_Merge_Order ;



        select
            root_ordered_test_pntr  as 	root_ordered_test_pntr_merge,
            ACCESSION_NBR as ACCESSION_NBR_merge	,
            LAB_RPT_CREATED_BY	as LAB_RPT_CREATED_BY_merge ,
            LAB_RPT_CREATED_DT	,
            JURISDICTION_CD	,
            JURISDICTION_NM	,
            LAB_TEST_dt	,
            specimen_collection_dt	,
            LAB_RPT_RECEIVED_BY_PH_DT	,
            LAB_RPT_LAST_UPDATE_DT	,
            LAB_RPT_LAST_UPDATE_BY	,
            ELR_IND as ELR_IND1 	,
            specimen_src	,
            specimen_site	,
            Specimen_desc	,
            SPECIMEN_SITE_desc	,
            LAB_RPT_LOCAL_ID	,
            record_status_cd as record_status_cd_merge
        into TMP_Merge_Order
        from TMP_LAB_TEST
        Where LAB_TEST_Type = 'Order'
        ;




        update TMP_Merge_Order
        set  record_status_cd_merge = 'ACTIVE'
        where  record_status_cd_merge in ( '' ,	'UNPROCESSED',	'UNPROCESSED_PREV_D',	'PROCESSED' )
        ;

        update TMP_Merge_Order
        set  record_status_cd_merge = 'INACTIVE'
        where  record_status_cd_merge = 'LOG_DEL'
        ;


        update TMP_LAB_TEST
        set record_status_cd = record_status_cd_for_result_drug
        where  record_status_cd =''
        ;

        update TMP_LAB_TEST
        set LAB_TEST_DT = resulted_lab_report_date
        where  LAB_TEST_TYPE ='Result'
        ;


        update TMP_LAB_TEST
        set LAB_TEST_DT = sus_lab_report_date
        where LAB_TEST_TYPE ='Order_rslt'
        ;

        alter table TMP_LAB_TEST
            drop column
                     ACCESSION_NBR	,
                 LAB_RPT_CREATED_BY	,
                 LAB_RPT_CREATED_DT	,
                 JURISDICTION_CD	,
                 JURISDICTION_NM	,
                 LAB_TEST_dt	,
                 specimen_collection_dt	,
                 LAB_RPT_RECEIVED_BY_PH_DT	,
                 LAB_RPT_LAST_UPDATE_DT	,
                 LAB_RPT_LAST_UPDATE_BY	,
                --	ELR_IND	,
                 resulted_lab_report_date	,
                 sus_lab_report_date	,
                 specimen_src	,
                 specimen_site	,
                 Specimen_desc	,
                 SPECIMEN_SITE_desc	,
                 LAB_RPT_LOCAL_ID	,
                 record_status_cd_for_result	,
                record_status_cd_for_result_drug
        ;


        update TMP_LAB_TEST
        set  record_status_cd = 'ACTIVE'
        where  record_status_cd in ( '' ,	'UNPROCESSED',	'UNPROCESSED_PREV_D',	'PROCESSED' )
           or record_status_cd is null
        ;

        update TMP_LAB_TEST
        set  record_status_cd = 'INACTIVE'
        where  record_status_cd = 'LOG_DEL'
        ;


        update TMP_LAB_TEST
        set  TEST_METHOD_CD_DESC = null
        where rtrim(TEST_METHOD_CD_DESC) = ''
        ;





        /*
        data LAB_TEST;
            MERGE Merge_Order LAB_TEST;
            BY root_ordered_test_pntr;
        */



        SELECT @ROWCOUNT_NO = @@ROWCOUNT;
        INSERT INTO [DBO].[JOB_FLOW_LOG]
        (BATCH_ID,[DATAFLOW_NAME],[PACKAGE_NAME] ,[STATUS_TYPE],[STEP_NUMBER],[STEP_NAME],[ROW_COUNT])
        VALUES(@BATCH_ID,'D_LABTEST','D_LABTEST','START',  @PROC_STEP_NO,@PROC_STEP_NAME,@ROWCOUNT_NO);


        COMMIT TRANSACTION;

        BEGIN TRANSACTION;
        SET @PROC_STEP_NO =  @PROC_STEP_NO + 1 ;
        SET @PROC_STEP_NAME = ' GENERATING TMP_LAB_TEST_final_root_ordered_test_pntr ';


        IF OBJECT_ID('#TMP_LAB_TEST_final_root_ordered_test_pntr', 'U') IS NOT NULL
            drop table  TMP_LAB_TEST_final_root_ordered_test_pntr;

        select  root_ordered_test_pntr AS LAB_TEST_ptnr
        into #TMP_LAB_TEST_final_root_ordered_test_pntr
        from #TMP_LAB_TEST
        union
        select root_ordered_test_pntr_merge
        from #TMP_Merge_Order
        ;



        SELECT @ROWCOUNT_NO = @@ROWCOUNT;
        INSERT INTO [DBO].[JOB_FLOW_LOG]
        (BATCH_ID,[DATAFLOW_NAME],[PACKAGE_NAME] ,[STATUS_TYPE],[STEP_NUMBER],[STEP_NAME],[ROW_COUNT])
        VALUES(@BATCH_ID,'D_LABTEST','D_LABTEST','START',  @PROC_STEP_NO,@PROC_STEP_NAME,@ROWCOUNT_NO);


        COMMIT TRANSACTION;

        BEGIN TRANSACTION;
        SET @PROC_STEP_NO =  @PROC_STEP_NO + 1 ;
        SET @PROC_STEP_NAME = ' GENERATING TMP_LAB_TEST_final';


        IF OBJECT_ID('#TMP_LAB_TEST_final', 'U') IS NOT NULL
            drop table #TMP_LAB_TEST_final ;


        select  lt1.*,lto.*, ltmi.*
        into #TMP_LAB_TEST_final
        from #TMP_LAB_TEST_final_root_ordered_test_pntr lt1
                 left outer join #TMP_LAB_TEST  lto        on lt1.LAB_TEST_ptnr = lto.root_ordered_test_pntr
                 left outer join #TMP_Merge_Order  ltmi  on lt1.LAB_TEST_ptnr = ltmi.root_ordered_test_pntr_merge
        ;

        update #TMP_LAB_TEST_final
        set ELR_IND = ELR_IND1
        where ELR_IND1 is not null
        ;

        update #TMP_LAB_TEST_final
        set record_status_cd = record_status_cd_merge
        where record_status_cd_merge is not null
        ;




        alter table #TMP_LAB_TEST_final
            drop column  ELR_IND1;

        create index complex_index on #TMP_LAB_TEST_final(root_ordered_Test_pntr, LAB_TEST_pntr);



        -- create table L_LAB_TEST_N  AS


        SELECT @ROWCOUNT_NO = @@ROWCOUNT;
        INSERT INTO [DBO].[JOB_FLOW_LOG]
        (BATCH_ID,[DATAFLOW_NAME],[PACKAGE_NAME] ,[STATUS_TYPE],[STEP_NUMBER],[STEP_NAME],[ROW_COUNT])
        VALUES(@BATCH_ID,'D_LABTEST','D_LABTEST','START',  @PROC_STEP_NO,@PROC_STEP_NAME,@ROWCOUNT_NO);


        COMMIT TRANSACTION;

        BEGIN TRANSACTION;
        SET @PROC_STEP_NO =  @PROC_STEP_NO + 1 ;
        SET @PROC_STEP_NAME = ' GENERATING tmp_L_LAB_TEST_N ';


        IF OBJECT_ID('#TMP_L_LAB_TEST_N', 'U') IS NOT NULL
            drop table  #TMP_L_LAB_TEST_N;


        CREATE TABLE #TMP_L_LAB_TEST_N
            (
                [LAB_TEST_id]  [int] IDENTITY(1,1) NOT NULL,
                [LAB_TEST_UID] [numeric](20, 0) NULL,
                [LAB_TEST_KEY] [numeric](18, 0) NULL
            ) ON [PRIMARY]
        ;



        insert into #TMP_L_LAB_TEST_N	([LAB_TEST_UID])
        SELECT DISTINCT tlt.LAB_TEST_UID
        FROM #TMP_LAB_TEST_final tlt
        EXCEPT
        SELECT lt.LAB_TEST_UID
        FROM LAB_TEST lt
        ;



        UPDATE #tmp_L_LAB_TEST_N
        SET LAB_TEST_KEY= LAB_TEST_ID + coalesce((SELECT MAX(LAB_TEST_KEY) FROM #L_LAB_TEST),0)



        DELETE FROM TMP_L_LAB_TEST_N WHERE LAB_TEST_UID IS NULL;



        SELECT @ROWCOUNT_NO = @@ROWCOUNT;

        INSERT INTO [DBO].[JOB_FLOW_LOG]
        (BATCH_ID,[DATAFLOW_NAME],[PACKAGE_NAME] ,[STATUS_TYPE],[STEP_NUMBER],[STEP_NAME],[ROW_COUNT])
        VALUES(@BATCH_ID,'D_LABTEST','D_LABTEST','START',  @PROC_STEP_NO,@PROC_STEP_NAME,@ROWCOUNT_NO);


        COMMIT TRANSACTION;

        BEGIN TRANSACTION;
        SET @PROC_STEP_NO =  @PROC_STEP_NO + 1 ;
        SET @PROC_STEP_NAME = 'INSERTING INTO L_LAB_TEST';


        INSERT INTO L_LAB_TEST
        ([LAB_TEST_KEY]
        ,[LAB_TEST_UID])
        SELECT    [LAB_TEST_KEY],
                  [LAB_TEST_UID]
        FROM #TMP_L_LAB_TEST_N
        ;





        --%DBLOAD (L_LAB_TEST, L_LAB_TEST_N);
        /*proc sort data = .LAB_TEST tagsort;
        By root_ordered_Test_pntr LAB_TEST_pntr;
        */

        SELECT @ROWCOUNT_NO = @@ROWCOUNT;

        INSERT INTO [DBO].[JOB_FLOW_LOG]
        (BATCH_ID,[DATAFLOW_NAME],[PACKAGE_NAME] ,[STATUS_TYPE],[STEP_NUMBER],[STEP_NAME],[ROW_COUNT])
        VALUES(@BATCH_ID,'D_LABTEST','D_LABTEST','START',  @PROC_STEP_NO,@PROC_STEP_NAME,@ROWCOUNT_NO);


        COMMIT TRANSACTION;

        BEGIN TRANSACTION;
        SET @PROC_STEP_NO =  @PROC_STEP_NO + 1 ;
        SET @PROC_STEP_NAME = ' GENERATING #TMP_D_LAB_TEST_N ';

        create index idx_tmp_L_LAB_TEST_N_uid1 on #TMP_L_LAB_TEST_N(LAB_TEST_UID);

        create index idx_TMP_LAB_TEST_final_uid1 on #TMP_LAB_TEST_final(LAB_TEST_UID);


        IF OBJECT_ID('#TMP_D_LAB_TEST_N', 'U') IS NOT NULL
            drop table  #TMP_D_LAB_TEST_N;




        -- create table D_LAB_TEST_N AS
        SELECT distinct  lt.* , ltn.[LAB_TEST_KEY]
        INTO #TMP_D_LAB_TEST_N
        FROM #TMP_LAB_TEST_final  lt,
             #TMP_L_LAB_TEST_N ltn
        WHERE lt.LAB_TEST_UID=ltn.LAB_TEST_UID
        ;

        /*
        PROC SORT DATA=D_LAB_TEST_N NODUPKEY OUT=D_LAB_TEST_N; BY LAB_TEST_key;
        DATA D_LAB_TEST_N;
        SET D_LAB_TEST_N;
        RDB_LAST_REFRESH_TIME=DATETIME();

        %checkerr;
        %DBLOAD (LAB_TEST, D_LAB_TEST_N);
        */

        UPDATE  #TMP_D_LAB_TEST_N  SET JURISdiction_nm = NULL  where JURISdiction_nm = '' ;

        UPDATE  #TMP_D_LAB_TEST_N  SET [ACCESSION_NBR_merge] = NULL  where [ACCESSION_NBR_merge] = '' ;

        UPDATE  #TMP_D_LAB_TEST_N  SET [SPECIMEN_DESC] = NULL  where [SPECIMEN_DESC] = '' ;

        UPDATE  #TMP_D_LAB_TEST_N  SET [SPECIMEN_SRC] = NULL  where [SPECIMEN_SRC] = '' ;

        UPDATE  #TMP_D_LAB_TEST_N  SET [CLINICAL_INFORMATION] = NULL  where [CLINICAL_INFORMATION] = '' ;

        UPDATE  #TMP_D_LAB_TEST_N  SET REASON_FOR_TEST_DESC = NULL  where REASON_FOR_TEST_DESC = '' ;

        UPDATE  #TMP_D_LAB_TEST_N  SET REASON_FOR_TEST_CD = NULL  where REASON_FOR_TEST_CD = '' ;


        SELECT @ROWCOUNT_NO = @@ROWCOUNT;
        INSERT INTO [DBO].[JOB_FLOW_LOG]
        (BATCH_ID,[DATAFLOW_NAME],[PACKAGE_NAME] ,[STATUS_TYPE],[STEP_NUMBER],[STEP_NAME],[ROW_COUNT])
        VALUES(@BATCH_ID,'D_LABTEST','D_LABTEST','START',  @PROC_STEP_NO,@PROC_STEP_NAME,@ROWCOUNT_NO);


        COMMIT TRANSACTION;

        BEGIN TRANSACTION;
        SET @PROC_STEP_NO =  @PROC_STEP_NO + 1 ;
        SET @PROC_STEP_NAME = 'insert into .LAB_TEST';




        insert into .LAB_TEST
        (
          [LAB_TEST_STATUS]
        ,[LAB_TEST_KEY]
        ,[LAB_RPT_LOCAL_ID]
        ,[TEST_METHOD_CD]
        ,[TEST_METHOD_CD_DESC]
        ,[LAB_RPT_SHARE_IND]
        ,[LAB_TEST_CD]
        ,[ELR_IND]
        ,[LAB_RPT_UID]
        ,[LAB_TEST_CD_DESC]
        ,[INTERPRETATION_FLG]
        ,[LAB_RPT_RECEIVED_BY_PH_DT]
        ,[LAB_RPT_CREATED_BY]
        ,[REASON_FOR_TEST_DESC]
        ,[REASON_FOR_TEST_CD]
        ,[LAB_RPT_LAST_UPDATE_BY]
        ,[LAB_TEST_DT]
        ,[LAB_RPT_CREATED_DT]
        ,[LAB_TEST_TYPE]
        ,[LAB_RPT_LAST_UPDATE_DT]
        ,[JURISDICTION_CD]
        ,[LAB_TEST_CD_SYS_CD]
        ,[LAB_TEST_CD_SYS_NM]
        ,[JURISDICTION_NM]
        ,[OID]
        ,[ALT_LAB_TEST_CD]
        ,[LAB_RPT_STATUS]
        ,[DANGER_CD_DESC]
        ,ALT_LAB_TEST_CD_DESC
        ,[ACCESSION_NBR]
        ,[SPECIMEN_SRC]
        ,[PRIORITY_CD]
        ,[ALT_LAB_TEST_CD_SYS_CD]
        ,[ALT_LAB_TEST_CD_SYS_NM]
        ,[SPECIMEN_SITE]
        ,[SPECIMEN_DETAILS]
        ,[DANGER_CD]
        ,[SPECIMEN_COLLECTION_VOL]
        ,[SPECIMEN_COLLECTION_VOL_UNIT]
        ,[SPECIMEN_DESC]
        ,[SPECIMEN_SITE_DESC]
        ,[CLINICAL_INFORMATION]
        ,[LAB_TEST_UID]
        ,[ROOT_ORDERED_TEST_PNTR]
        ,[PARENT_TEST_PNTR]
        ,[LAB_TEST_PNTR]
        ,[SPECIMEN_ADD_TIME]
        ,[SPECIMEN_LAST_CHANGE_TIME]
        ,[SPECIMEN_COLLECTION_DT]
        ,[SPECIMEN_NM]
        ,[ROOT_ORDERED_TEST_NM]
        ,[PARENT_TEST_NM]
        ,[TRANSCRIPTIONIST_NAME]
        ,[TRANSCRIPTIONIST_ID]
        ,[TRANSCRIPTIONIST_ASS_AUTH_CD]
        ,[TRANSCRIPTIONIST_ASS_AUTH_TYPE]
        ,[ASSISTANT_INTERPRETER_NAME]
        ,[ASSISTANT_INTERPRETER_ID]
        ,[ASSISTANT_INTER_ASS_AUTH_CD]
        ,[ASSISTANT_INTER_ASS_AUTH_TYPE]
        ,[RESULT_INTERPRETER_NAME]
        ,[RECORD_STATUS_CD]
        ,[RDB_LAST_REFRESH_TIME]
        ,[CONDITION_CD]
        ,[PROCESSING_DECISION_CD]
        ,[PROCESSING_DECISION_DESC]
        )
        select rtrim( cast( LAB_TEST_STATUS AS varchar(50)))
             ,[LAB_TEST_KEY]
             ,rtrim( cast( LAB_RPT_LOCAL_ID AS varchar(50)))
             ,rtrim( cast( TEST_METHOD_CD AS varchar(199)))
             ,rtrim( cast( TEST_METHOD_CD_DESC AS varchar(199)))
             ,rtrim( cast( LAB_RPT_SHARE_IND AS varchar(50)))
             ,rtrim( cast( LAB_TEST_CD AS varchar(1000)))
             ,rtrim( cast( ELR_IND AS varchar(50)))
             ,[LAB_RPT_UID]
             ,rtrim( cast( LAB_TEST_CD_DESC AS varchar(2000)))
             ,rtrim( cast( INTERPRETATION_FLG AS varchar(20)))
             ,[LAB_RPT_RECEIVED_BY_PH_DT]
             ,[LAB_RPT_CREATED_BY_MERGE]
             ,rtrim( cast( REASON_FOR_TEST_DESC AS varchar(4000)))
             ,rtrim( cast( REASON_FOR_TEST_CD AS varchar(4000)))
             ,[LAB_RPT_LAST_UPDATE_BY]
             ,[LAB_TEST_DT]
             ,[LAB_RPT_CREATED_DT]
             ,rtrim( cast( LAB_TEST_TYPE AS varchar(50)))
             ,[LAB_RPT_LAST_UPDATE_DT]
             ,rtrim( cast( JURISDICTION_CD AS varchar(20)))
             ,rtrim( cast( LAB_TEST_CD_SYS_CD AS varchar(50)))
             ,rtrim( cast( LAB_TEST_CD_SYS_NM AS varchar(100)))
             ,rtrim( cast( JURISDICTION_NM AS varchar(50)))
             ,order_OID
             ,rtrim( cast( ALT_LAB_TEST_CD AS varchar(50)))
             , cast( LAB_RPT_STATUS AS char(1))
             ,rtrim( cast( DANGER_CD_DESC AS varchar(100)))
             ,rtrim( cast( ALT_LAB_TEST_CD_DESC AS varchar(1000)))
             ,rtrim( cast( ACCESSION_NBR_MERGE AS varchar(199)))
             ,rtrim( cast( SPECIMEN_SRC AS varchar(50)))
             ,rtrim( cast( PRIORITY_CD AS varchar(20)))
             ,rtrim( cast( ALT_LAB_TEST_CD_SYS_CD AS varchar(50)))
             ,rtrim( cast( ALT_LAB_TEST_CD_SYS_NM AS varchar(100)))
             ,rtrim( cast( SPECIMEN_SITE AS varchar(20)))
             ,rtrim( cast( SPECIMEN_DETAILS AS varchar(1000)))
             ,rtrim( cast( DANGER_CD AS varchar(20)))
             ,rtrim( cast( SPECIMEN_COLLECTION_VOL AS varchar(20)))
             ,rtrim( cast( SPECIMEN_COLLECTION_VOL_UNIT AS varchar(50)))
             ,rtrim( cast( SPECIMEN_DESC AS varchar(1000)))
             ,rtrim( cast( SPECIMEN_SITE_DESC AS varchar(100)))
             ,rtrim( cast( CLINICAL_INFORMATION AS varchar(1000)))
             ,[LAB_TEST_UID]
             ,[ROOT_ORDERED_TEST_PNTR]
             ,[PARENT_TEST_PNTR]
             ,[LAB_TEST_PNTR]
             ,[SPECIMEN_ADD_TIME]
             ,[SPECIMEN_LAST_CHANGE_TIME]
             ,[SPECIMEN_COLLECTION_DT]
             ,rtrim( cast( SPECIMEN_NM AS varchar(100)))
             ,rtrim( cast( ROOT_ORDERED_TEST_NM AS varchar(1000)))
             ,rtrim( cast( PARENT_TEST_NM AS varchar(1000)))
             ,rtrim( cast( TRANSCRIPTIONIST_NAME AS varchar(300)))
             ,rtrim( cast( TRANSCRIPTIONIST_ID AS varchar(100)))
             ,rtrim( cast( TRANSCRIPTIONIST_ASS_AUTH_CD AS varchar(199)))
             ,rtrim( cast( TRANSCRIPTIONIST_ASS_AUTH_TYPE AS varchar(100)))
             ,rtrim( cast( ASSISTANT_INTERPRETER_NAME AS varchar(300)))
             ,rtrim( cast( ASSISTANT_INTERPRETER_ID AS varchar(100)))
             ,rtrim( cast( ASSISTANT_INTER_ASS_AUTH_CD AS varchar(199)))
             ,rtrim( cast( ASSISTANT_INTER_ASS_AUTH_TYPE AS varchar(100)))
             ,rtrim( cast( RESULT_INTERPRETER_NAME AS varchar(300)))
             ,rtrim( cast( RECORD_STATUS_CD AS varchar(8)))
             ,GETDATE()
             ,rtrim( cast( CONDITION_CD AS varchar(20)))
             ,rtrim( cast( PROCESSING_DECISION_CD AS varchar(50)))
             ,rtrim( cast( PROCESSING_DECISION_DESC AS varchar(50)))
        FROM [RDB].[dbo]TMP_D_LAB_TEST_N
        ;


        /*-------------------------------------------------------

            Lab_Report_User_Comment Dimension

            Note: Comments under the Order Test object (LAB214)
        ---------------------------------------------------------*/

        create index idx_LAB_TEST_uid on TMP_D_LAB_TEST_N(LAB_TEST_uid);



        -- create table Lab_Rpt_User_Comment as

        SELECT @ROWCOUNT_NO = @@ROWCOUNT;
        INSERT INTO [DBO].[JOB_FLOW_LOG]
        (BATCH_ID,[DATAFLOW_NAME],[PACKAGE_NAME] ,[STATUS_TYPE],[STEP_NUMBER],[STEP_NAME],[ROW_COUNT])
        VALUES(@BATCH_ID,'D_LABTEST','D_LABTEST','START',  @PROC_STEP_NO,@PROC_STEP_NAME,@ROWCOUNT_NO);


        COMMIT TRANSACTION;

        BEGIN TRANSACTION;
        SET @PROC_STEP_NO =  @PROC_STEP_NO + 1 ;
        SET @PROC_STEP_NAME = ' GENERATING TMP_Lab_Rpt_User_Comment ';


        IF OBJECT_ID('#TMP_Lab_Rpt_User_Comment', 'U') IS NOT NULL
            drop table  TMP_Lab_Rpt_User_Comment;

        CREATE TABLE [dbo].[TMP_Lab_Rpt_User_Comment](
                                                         [LAB_COMMENT_id]  [int] IDENTITY(1,1) NOT NULL,
                                                         [LAB_TEST_Key] [bigint]  NULL,
                                                         [LAB_TEST_uid] [bigint] NULL,
                                                         [COMMENTS_FOR_ELR_DT] [datetime] NULL,
                                                         [USER_COMMENT_CREATED_BY] [bigint] NULL,
                                                         [USER_RPT_COMMENTS] [varchar](8000) NULL,
                                                         [RECORD_STATUS_CD] [varchar](8) NOT NULL,
                                                         [observation_uid] [bigint] NOT NULL,
                                                         USER_COMMENT_KEY [bigint],
                                                         [RDB_LAST_REFRESH_TIME] [datetime] NULL
                                                     ) ON [PRIMARY]
        ;


        INSERT INTO TMP_Lab_Rpt_User_Comment
        select 	distinct tdltn.LAB_TEST_Key,
                           tdltn.lab_rpt_uid as LAB_TEST_uid,
                           lab214.activity_to_time	'COMMENTS_FOR_ELR_DT' ,
                           lab214.add_user_id		'USER_COMMENT_CREATED_BY' ,
                           REPLACE(REPLACE(ovt.value_txt, CHAR(13), ' '), CHAR(10), ' ')	'USER_RPT_COMMENTS',--TRANSLATE(ovt.value_txt,' ' ,'0D0A'x)	'USER_RPT_COMMENTS' ,
                           tdltn.record_status_cd        'RECORD_STATUS_CD' ,
                           lab214.observation_uid,
                           NULL,
                           NULL
        from 	TMP_D_LAB_TEST_N	    as tdltn,
                nbs_odse..act_relationship 	as ar1,
                nbs_odse..observation		as obs,
                nbs_odse..act_relationship 	as ar2,
                nbs_odse..observation		as lab214,
                nbs_odse..obs_value_txt 	as ovt
        where   ovt.value_txt is not null
          and tdltn.LAB_TEST_uid = ar1.target_act_uid
          and ar1.type_cd = 'APND'
          and ar1.source_act_uid = obs.observation_uid
          and obs.OBS_DOMAIN_CD_ST_1 ='C_Order'
          and obs.observation_uid = ar2.target_act_uid
          and ar2.source_act_uid = lab214.observation_uid
          and ar2.type_cd = 'COMP'
          and lab214.OBS_DOMAIN_CD_ST_1 ='C_Result'
          and lab214.observation_uid = ovt.observation_uid

        ;


        SELECT @ROWCOUNT_NO = @@ROWCOUNT;

        update #TMP_Lab_Rpt_User_Comment
        set  record_status_cd = 'ACTIVE'
        where  record_status_cd in ( '' ,	'UNPROCESSED',	'PROCESSED' )
        ;

        update #TMP_Lab_Rpt_User_Comment
        set  record_status_cd = 'INACTIVE'
        where  record_status_cd = 'LOG_DEL'
        ;

        /*
        data  Lab_Rpt_User_Comment;
        set Lab_Rpt_User_Comment;
        if LAB_TEST_key =. then LAB_TEST_key =1;

        %assign_key(LAB_RPT_USER_COMMENT, USER_COMMENT_KEY);

        ALTER TABLE LAB_RPT_USER_COMMENT ADD User_Comment_key_MAX_VAL  NUMERIC;

        UPDATE  LAB_RPT_USER_COMMENT SET User_Comment_key_MAX_VAL=(SELECT MAX(User_Comment_key) FROM .LAB_RPT_USER_COMMENT);




        DATA .Lab_Rpt_User_Comment;
        SET Lab_Rpt_User_Comment;
        IF USER_COMMENT_KEY_MAX_VAL  ~=. AND USER_COMMENT_KEY~=1 THEN USER_COMMENT_KEY= USER_COMMENT_KEY+USER_COMMENT_KEY_MAX_VAL;
        */




        UPDATE #TMP_Lab_Rpt_User_Comment
        SET USER_COMMENT_KEY= [LAB_COMMENT_id] + coalesce((SELECT MAX(USER_COMMENT_KEY) FROM#Lab_Rpt_User_Comment),1)


        /*--VS
        delete from TMP_Lab_Rpt_User_Comment
        where USER_COMMENT_KEY=1 and USER_COMMENT_KEY_MAX_VAL >0;
        */

        delete from #TMP_LAB_RPT_USER_COMMENT
        where LAB_TEST_KEY= null;

        UPDATE #TMP_Lab_Rpt_User_Comment
        set RDB_LAST_REFRESH_TIME=getdate();


        UPDATE #TMP_Lab_Rpt_User_Comment
        set [USER_RPT_COMMENTS]= null
        where [USER_RPT_COMMENTS] = ''
        ;


        ---%DBLOAD (LAB_RPT_USER_COMMENT, .Lab_Rpt_User_Comment);

        -- create table Lab_Rpt_User_Comment as

        INSERT INTO [DBO].[JOB_FLOW_LOG]
        (BATCH_ID,[DATAFLOW_NAME],[PACKAGE_NAME] ,[STATUS_TYPE],[STEP_NUMBER],[STEP_NAME],[ROW_COUNT])
        VALUES(@BATCH_ID,'D_LABTEST','D_LABTEST','START',  @PROC_STEP_NO,@PROC_STEP_NAME,@ROWCOUNT_NO);


        COMMIT TRANSACTION;

        BEGIN TRANSACTION;
        SET @PROC_STEP_NO =  @PROC_STEP_NO + 1 ;
        SET @PROC_STEP_NAME = 'INSERTING INTO Lab_Rpt_User_Comment';



        insert into Lab_Rpt_User_Comment
        (
          [USER_COMMENT_KEY]
        ,[USER_RPT_COMMENTS]
        ,[COMMENTS_FOR_ELR_DT]
        ,[USER_COMMENT_CREATED_BY]
        ,[LAB_TEST_KEY]
        ,[RECORD_STATUS_CD]
        ,[LAB_TEST_UID]
        ,[RDB_LAST_REFRESH_TIME]
        )
        select [USER_COMMENT_KEY]
             ,rtrim( cast( [USER_RPT_COMMENTS] AS varchar(2000)))
             ,[COMMENTS_FOR_ELR_DT]
             ,[USER_COMMENT_CREATED_BY]
             ,[LAB_TEST_KEY]
             ,rtrim( cast( [RECORD_STATUS_CD] AS varchar(8)))
             ,[LAB_TEST_UID]
             ,[RDB_LAST_REFRESH_TIME]
        FROM #TMP_LAB_RPT_USER_COMMENT
        ;


        SELECT @ROWCOUNT_NO = @@ROWCOUNT;
        INSERT INTO [DBO].[JOB_FLOW_LOG]
        (BATCH_ID,[DATAFLOW_NAME],[PACKAGE_NAME] ,[STATUS_TYPE],[STEP_NUMBER],[STEP_NAME],[ROW_COUNT])
        VALUES(@BATCH_ID,'D_LABTEST','D_LABTEST','START',  @PROC_STEP_NO,@PROC_STEP_NAME,@ROWCOUNT_NO);


        COMMIT TRANSACTION;


        IF OBJECT_ID('#TMP_updated_observation_List', 'U') IS NOT NULL
            drop table  TMP_updated_observation_List;

        IF OBJECT_ID('#TMP_updated_LAB_TEST_list', 'U') IS NOT NULL
            drop table  #TMP_updated_LAB_TEST_list;

        IF OBJECT_ID('#TMP_updated_LAB_RPT_USER_COMMENT', 'U') IS NOT NULL
            drop table  TMP_updated_LAB_RPT_USER_COMMENT;

        IF OBJECT_ID('#TMP_updt_Test_Result_Grouping_LIST', 'U') IS NOT NULL
            drop table  TMP_updt_Test_Result_Grouping_LIST ;

        IF OBJECT_ID('#TMP_updt_Lab_Result_Val_list', 'U') IS NOT NULL
            drop table  TMP_updt_Lab_Result_Val_list;

        IF OBJECT_ID('#TMP_updated_LAB_TEST_result_list', 'U') IS NOT NULL
            drop table  TMP_updated_LAB_TEST_result_list;

        IF OBJECT_ID('#TMP_updT_Result_Comment_Grp_LIST', 'U') IS NOT NULL
            drop table  TMP_updT_Result_Comment_Grp_LIST;

        IF OBJECT_ID('#TMP_updt_Lab_Result_Comment_list', 'U') IS NOT NULL
            drop table  TMP_updt_Lab_Result_Comment_list;

        IF OBJECT_ID('#TMP_updated_LAB_TEST_list', 'U') IS NOT NULL
            drop table  TMP_updated_LAB_TEST_list;

        IF OBJECT_ID('#TMP_s_edx_document1', 'U') IS NOT NULL
            drop table  #TMP_s_edx_document1;


        IF OBJECT_ID('#TMP_merged_provider', 'U') IS NOT NULL
            drop table  #TMP_merged_provider;

        IF OBJECT_ID('#TMP_filter_participants', 'U') IS NOT NULL
            drop table   #TMP_filter_participants ;

        IF OBJECT_ID('#TMP_participants', 'U') IS NOT NULL
            drop table    #TMP_participants ;

        IF OBJECT_ID('#TMP_LAB_TESTinit_a', 'U') IS NOT NULL
            drop table  #TMP_LAB_TESTinit_a;

        IF OBJECT_ID('#TMP_s_edx_document', 'U') IS NOT NULL
            drop table #TMP_s_edx_document;

        IF OBJECT_ID('#TMP_LAB_TESTinit', 'U') IS NOT NULL
            drop table   #TMP_LAB_TESTinit;

        IF OBJECT_ID('#TMP_LAB_TEST_mat_init', 'U') IS NOT NULL
            drop table  #TMP_LAB_TEST_mat_init;

        IF OBJECT_ID('#TMP_OBS_REASON', 'U') IS NOT NULL
            drop table  #TMP_OBS_REASON;

        IF OBJECT_ID('#TMP_OBS_REASON_FINAL', 'U') IS NOT NULL
            drop table #TMP_OBS_REASON_FINAL ;

        IF OBJECT_ID('#TMP_LAB_TEST_oth', 'U') IS NOT NULL
            drop table  #TMP_LAB_TEST_oth;

        IF OBJECT_ID('#TMP_LAB_TEST1_uid', 'U') IS NOT NULL
            drop table  #TMP_LAB_TEST1_uid;

        IF OBJECT_ID('#TMP_LAB_TEST1_TMP', 'U') IS NOT NULL
            drop table #TMP_LAB_TEST1_TMP ;

        IF OBJECT_ID('#TMP_LabReportMorb', 'U') IS NOT NULL
            drop table  #TMP_LabReportMorb;

        IF OBJECT_ID('#TMP_Morb_OID', 'U') IS NOT NULL
            drop table  #TMP_Morb_OID ;

        IF OBJECT_ID('#TMP_LAB_TEST1_uid2', 'U') IS NOT NULL
            drop table  #TMP_LAB_TEST1_uid2;

        IF OBJECT_ID('#TMP_LAB_TEST1', 'U') IS NOT NULL
            drop table #TMP_LAB_TEST1 ;

        IF OBJECT_ID('#TMP_R_Result_to_R_Order', 'U') IS NOT NULL
            drop table #TMP_R_Result_to_R_Order;

        IF OBJECT_ID('#TMP_R_Result_to_R_Order_to_Order', 'U') IS NOT NULL
            drop table   #TMP_R_Result_to_R_Order_to_Order;

        IF OBJECT_ID('#TMP_LAB_TEST1_testuid', 'U') IS NOT NULL
            drop table  #TMP_LAB_TEST1_testuid;

        IF OBJECT_ID('#TMP_LAB_TEST1_final', 'U') IS NOT NULL
            drop table  #TMP_LAB_TEST1_final;

        IF OBJECT_ID('#TMP_R_Order_to_Result', 'U') IS NOT NULL
            drop table  #TMP_R_Order_to_Result;

        IF OBJECT_ID('#TMP_LAB_TEST1_final_testuid', 'U') IS NOT NULL
            drop table  #TMP_LAB_TEST1_final_testuid;

        IF OBJECT_ID('#TMP_LAB_TEST1_final_result', 'U') IS NOT NULL
            drop table  #TMP_LAB_TEST1_final_result;

        IF OBJECT_ID('#TMP_Result_to_Order', 'U') IS NOT NULL
            drop table  #TMP_Result_to_Order;

        IF OBJECT_ID('#TMP_LAB_TEST1_final_orderuid', 'U') IS NOT NULL
            drop table  #TMP_LAB_TEST1_final_orderuid;

        IF OBJECT_ID('#TMP_LAB_TEST1_final_order', 'U') IS NOT NULL
            drop table  #TMP_LAB_TEST1_final_order;

        IF OBJECT_ID('#TMP_LAB_TEST2', 'U') IS NOT NULL
            drop table  #TMP_LAB_TEST2 ;

        IF OBJECT_ID('#TMP_LAB_TEST3', 'U') IS NOT NULL
            drop table  #TMP_LAB_TEST3;

        IF OBJECT_ID('#TMP_LAB_TEST4', 'U') IS NOT NULL
            drop table #TMP_LAB_TEST4 ;

        IF OBJECT_ID('#TMP_order_test', 'U') IS NOT NULL
            drop table #TMP_order_test ;

        --IF OBJECT_ID('#TMP_LAB_TEST', 'U') IS NOT NULL
        --        	         drop table #TMP_LAB_TEST ;

        IF OBJECT_ID('#TMP_Merge_Order', 'U') IS NOT NULL
            drop table  #TMP_Merge_Order ;

        IF OBJECT_ID('#TMP_LAB_TEST_final_root_ordered_test_pntr', 'U') IS NOT NULL
            drop table  #TMP_LAB_TEST_final_root_ordered_test_pntr;

        --IF OBJECT_ID('#TMP_LAB_TEST_final', 'U') IS NOT NULL
        --      drop table #TMP_LAB_TEST_final ;

        IF OBJECT_ID('#TMP_L_LAB_TEST_N', 'U') IS NOT NULL
            drop table  #TMP_L_LAB_TEST_N;

        IF OBJECT_ID('#TMP_Lab_Rpt_User_Comment', 'U') IS NOT NULL
            drop table  #TMP_Lab_Rpt_User_Comment;


        BEGIN TRANSACTION;
        SET @PROC_STEP_NO =  @PROC_STEP_NO + 1 ;

        SET @Proc_Step_Name = 'SP_COMPLETE';


        INSERT INTO [dbo].[job_flow_log] (
                                           batch_id
                                         ,[Dataflow_Name]
                                         ,[package_Name]
                                         ,[Status_Type]
                                         ,[step_number]
                                         ,[step_name]
                                         ,[row_count]
        )
        VALUES
            (
              @batch_id,
              'D_LabTest'
            ,'D_LabTest'
            ,'COMPLETE'
            ,@Proc_Step_no
            ,@Proc_Step_name
            ,@RowCount_no
            );


        COMMIT TRANSACTION;
    END TRY

    BEGIN CATCH


        IF @@TRANCOUNT > 0   ROLLBACK TRANSACTION;



        DECLARE @ErrorNumber INT = ERROR_NUMBER();
        DECLARE @ErrorLine INT = ERROR_LINE();
        DECLARE @ErrorMessage NVARCHAR(4000) = ERROR_MESSAGE();
        DECLARE @ErrorSeverity INT = ERROR_SEVERITY();
        DECLARE @ErrorState INT = ERROR_STATE();


        INSERT INTO [dbo].[job_flow_log] (
                                           batch_id
                                         ,[Dataflow_Name]
                                         ,[package_Name]
                                         ,[Status_Type]
                                         ,[step_number]
                                         ,[step_name]
                                         ,[Error_Description]
                                         ,[row_count]
        )
        VALUES
            (
              @batch_id
            ,'D_LABTEST'
            ,'D_LABTEST'
            ,'ERROR'
            ,@Proc_Step_no
            ,'ERROR - '+ @Proc_Step_name
            , 'Step -' +CAST(@Proc_Step_no AS VARCHAR(3))+' -' +CAST(@ErrorMessage AS VARCHAR(500))
            ,0
            );


        return -1 ;

    END CATCH

END

    ;








