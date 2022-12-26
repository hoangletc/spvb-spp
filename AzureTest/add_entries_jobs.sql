INSERT INTO dbo.HL_ETL_JOB
    (JOB_NAME, ETL_TYPE, SRC_TABLE, TGT_TABLE, SRC_FOLDER_NAME, TRG_FOLDER_NAME)
VALUES
    ('SAP2Landing', 'SRI', NULL, NULL, 'sharepoint', 'landing'),
    ('Landing2STG', 'SDE',  )
)