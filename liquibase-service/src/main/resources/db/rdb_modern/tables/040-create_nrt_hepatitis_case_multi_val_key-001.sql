-- table is not dropped and recreated so as to stay consistent with the design of nrt_interview_key

IF NOT EXISTS (SELECT 1 FROM sysobjects WHERE name = 'nrt_hepatitis_case_multi_val_key' and xtype = 'U')
    BEGIN

        CREATE TABLE dbo.nrt_hepatitis_case_multi_val_key (
          HEP_MULTI_VAL_DATA_KEY bigint IDENTITY(1,1) NOT NULL,
          HEP_MULTI_VAL_GRP_KEY  bigint NOT NULL,
          public_health_case_id bigint NULL,
          selection_number bigint NULL,
          source_table VARCHAR(20) NULL
        );
        declare @max bigint;
        select @max=max(HEP_MULTI_VAL_DATA_KEY)+1 from dbo.hep_multi_value_field;
        select @max;
        if @max IS NULL   --check when max is returned as null
            SET @max = 2; -- default to 2, as default record with key = 1 is not stored in D_INTERVIEW_NOTE
        DBCC CHECKIDENT ('dbo.nrt_hepatitis_case_multi_val_key', RESEED, @max);

    END


