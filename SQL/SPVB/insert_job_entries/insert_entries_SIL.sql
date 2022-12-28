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
        20410,
        'SIL_STG2FND',
        7,
        'SPVB_SAP_SIL_STG2FND',
        'FND.W_SAP_MATDOC_D',
        'Load data from STG (dev) to FND (dev)',
        'Y',
        'SIL',
        'SIL',
        'STG.W_SAP_MATDOC_FS_TEMP',
        'FND.W_SAP_MATDOC_F_TEMP',
        'Y',
        'EXEC [dbo].[SAP_proc_load_stg_to_fnd] @p_tgt_table=''FND.W_SAP_MATDOC_D'', @p_batch_id=@P_BATCH_ID_VALUE',
        100,
        -1,
        'N',
        @dt,
        @dt,
        'STG',
        0,
        'CONCAT_WS(''~'', MANDT, MATNR, BWKEY)'
);

