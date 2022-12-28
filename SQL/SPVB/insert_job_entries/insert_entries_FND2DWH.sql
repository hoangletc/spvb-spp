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
    DYNAMIC_INTEGRATION_ID
    )
VALUES
    -- SAP2Parquet
    (
        20130,
        'SIL_FND2DWH',
        7,
        'SPVB_SAP_SIL_FND2DWH',
        'FND.W_SAP_ANLH_D',
        'Load data from STG (dev) to FND (dev)',
        'Y',
        'SIL',
        'SIL',
        'FND.W_SAP_ANLH_D',
        'dbo.W_SAP_ANLH_DW',
        'Y',
        'EXEC [dbo].[HL_SAP_proc_fnd2dwh] @p_batch_id=@P_BATCH_ID_VALUE',
        100,
        -1,
        'N',
        @dt,
        @dt,
        'STG',
        0,
        'CONCAT_WS(''~'', MANDT, BUKRS, ANLN1)'
);

