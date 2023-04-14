DECLARE @dt [datetime] = DATEADD(HH, 7, GETDATE());

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
    PRUNE_DAY
    )
VALUES
    (
        20198,
        'SDE_PARQUET2STG',
        7,
        'DEV_LANDING2STG',
        'STG.W_SAP_MBEWH_DS',
        'Load data from Parquet (dev) to STG (dev)',
        'Y',
        'SDE',
        'SDE',
        'MBEWH',
        'STG.W_SAP_MBEWH_DS',
        'Y',
        NULL,
        100,
        -1,
        'N',
        @dt,
        @dt,
        'AZURE',
        0
    );
