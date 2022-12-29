DECLARE @dt [datetime] = GETDATE()

insert into [dbo].[SAP_ETL_JOB]
    (
    JOB_ID,
    JOB_GROUP_NAME,
    BATCH_GROUP,
    PIPELINE,
    JOB_NAME,
    JOB_DESC,
    ACTIVE_FLG,
    JOB_TYPE,
    ETL_TYPE,
    SRC_TABLE,
    TGT_TABLE,
    IS_FULLLOAD,
    SCRIPT,
    JOB_ORDER,
    LAST_STEP,
    IS_CONTINUE_ON_ERROR,
    CREATED_DT,
    LAST_UPDATED_DT,
    SOURCE,
    PRUNE_DAY,
    SOURCE_FOLDER
    )
VALUES
    (
        20427,
        'SDE_JSON2STG',
        7,
        'SPVB_SAP_SDE_JSON2STG',
        'STG.W_CMMS_ASSET_FS',
        'Load data from JSON (dev) to STG (dev)',
        'Y',
        'SDE',
        'SDE',
        'asset.json',
        'STG.W_CMMS_ASSET_FS',
        'Y',
        NULL,
        100,
        -1,
        'N',
        @dt,
        @dt,
        'JSON',
        0,
        'API Sample Data/edited'
    );
