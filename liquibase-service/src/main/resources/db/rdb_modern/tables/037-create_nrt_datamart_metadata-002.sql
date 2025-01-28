IF EXISTS (SELECT 1 FROM sysobjects WHERE name = 'nrt_datamart_metadata' and xtype = 'U')
    BEGIN
        /*CNDE-2046: Generic_Case Datamart condition code addition script.*/
        IF NOT EXISTS (SELECT 1 FROM dbo.nrt_datamart_metadata ndm WHERE ndm.Datamart = 'Generic_Case')
            BEGIN
            INSERT INTO dbo.nrt_datamart_metadata
            SELECT condition_cd,
                   condition_desc_txt,
                   'Generic_Case',
                   'sp_generic_case_datamart_postprocessing'
            FROM
                (SELECT distinct cc.condition_cd, cc.condition_desc_txt
                 FROM NBS_SRTE.dbo.Condition_code cc
                 WHERE (cc.investigation_form_cd IS NOT NULL and cc.investigation_form_cd LIKE 'INV_FORM_GEN%')
                ) gen_codes
            WHERE NOT EXISTS
                      (SELECT 1
                       FROM dbo.nrt_datamart_metadata ndm
                       WHERE ndm.condition_cd = std_hiv_codes.condition_cd);
            END;

    END;
