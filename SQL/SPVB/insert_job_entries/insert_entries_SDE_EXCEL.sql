INSERT INTO [dbo].[SAP_ETL_JOB] (
    JOB_ID
    , JOB_GROUP_NAME
    , BATCH_GROUP
    , PIPELINE
    , JOB_NAME
    , JOB_DESC
    , ACTIVE_FLG
    , JOB_TYPE
    , ETL_TYPE
    , SRC_TABLE
    , TGT_TABLE
    , IS_FULLLOAD
    , FILTER_CONDITION
    , SCRIPT
    , JOB_ORDER
    , LAST_STEP
    , IS_CONTINUE_ON_ERROR
    , CREATED_DT
    , LAST_UPDATED_DT
    , SOURCE
    , SOURCE_FOLDER
    , PRUNE_DAY
    , FIRST_ROW_AS_HEADER
)
VALUES (
    20420
    , 'SDE_EXCEL2STG'
    , 7
    , 'SPVB_SAP_SDE_EXCEL2STG'
    , 'STG.W_EXCEL_SPP_MAPPING_ASSETTYPE_DS'
    , 'Load data from EXCEL (Blob) to STG (dev)'
    , 'Y'
    , 'SDE'
    , 'SDE'
    , 'MAPPING_ASSET_TYPE.xlsx'
    , 'STG.W_EXCEL_SPP_MAPPING_ASSETTYPE_DS'
    , 'Y'
    , 'Sheet1~A1:C100'
    , NULL
    , 100
    , -1
    , 'N'
    , DATEADD(HH, 7, GETDATE())
    , DATEADD(HH, 7, GETDATE())
    , 'AZURE'
    , 'raw_excel_file'
    , 0
    , 'Y'
);