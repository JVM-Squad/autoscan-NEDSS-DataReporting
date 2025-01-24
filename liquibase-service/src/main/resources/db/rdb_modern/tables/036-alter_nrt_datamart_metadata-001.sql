IF EXISTS (SELECT 1 FROM sysobjects WHERE name = 'nrt_datamart_metadata' and xtype = 'U')
    BEGIN
        --Increase varchar length according to accomodate data
        IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = object_id('nrt_datamart_metadata') AND name='Stored_Procedure' AND max_length=36)
            BEGIN
                ALTER TABLE dbo.nrt_datamart_metadata
                ALTER COLUMN Stored_Procedure VARCHAR(200)
            END
    END;
