IF NOT EXISTS (SELECT 1 FROM sysobjects WHERE name = 'nrt_hepatitis_case_multi_val_key' and xtype = 'U')
    BEGIN

        CREATE TABLE dbo.nrt_hepatitis_case_multi_val_key (
          HEP_MULTI_VAL_DATA_KEY bigint IDENTITY(1,1) NOT NULL,
          HEP_MULTI_VAL_GRP_KEY  bigint NOT NULL,
          public_health_case_uid bigint NULL,
          selection_number bigint NULL
        );
        declare @max bigint;
        select @max=max(HEP_MULTI_VAL_DATA_KEY)+1 from dbo.hep_multi_value_field;
        select @max;
        if @max IS NULL   --check when max is returned as null
            SET @max = 2; -- default to 2
        DBCC CHECKIDENT ('dbo.nrt_hepatitis_case_multi_val_key', RESEED, @max);

    END

IF NOT EXISTS (SELECT 1 FROM dbo.hep_multi_value_field)
    BEGIN

        INSERT INTO dbo.HEP_MULTI_VALUE_FIELD
        (
            HEP_MULTI_VAL_GRP_KEY,
            HEP_MULTI_VAL_DATA_KEY
        )
        SELECT 1,1;

    END;
