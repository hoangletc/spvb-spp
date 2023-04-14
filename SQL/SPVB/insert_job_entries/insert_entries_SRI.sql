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
    -- SAP2Parquet
    (
        20199,
        'SRI_SAP2PARQUET',
        7,
        'SPVB_SAP_SRI_SAP2PARQUET',
        'MBEWH',
        'Load data from SAP (prod) to parquet (dev)',
        'Y',
        'SRI',
        'SRI',
        'SAPHANADB.MBEWH',
        'MBEWH',
        'Y',
        'SELECT * FROM SAPHANADB.MBEWH',
        100,
        -1,
        'N',
        @dt,
        @dt,
        'MBEWH',
        0
    );
