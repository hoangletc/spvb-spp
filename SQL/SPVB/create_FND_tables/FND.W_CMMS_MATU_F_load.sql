SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

ALTER PROC [dbo].[SAP_proc_load_w_cmms_spp_fnd_matu_d]
    @p_batch_id [bigint]
AS
BEGIN
    DECLARE	@tgt_TableName nvarchar(200) = N'FND.W_CMMS_MATU_F',
			@sql nvarchar(max),
	        @column_name varchar(4000),
	        @no_row bigint	,
			@v_batch_id bigint = @p_batch_id,
			@v_job_id bigint = null,
			@v_jobinstance_id bigint,
			@v_logtype_id bigint,
			@p_src_chk_column varchar(32),
			@p_tgt_chk_column varchar(32),
			@p_return_code bigint,
			@p_return_msg varchar(4000),
			@src_rownum	bigint,
			@tgt_rownum bigint,
			@tgt_chk_value float,
			@src_chk_value float,			
			@v_src_tablename varchar(100),
			@v_tgt_tablename varchar(100),
			@v_return_msg varchar(4000),
			@v_return_status varchar(100),
			@isExistSSAS char(1),
	        @isFullload char(1),
			@PartitionCol nvarchar(100),
			@v_message varchar(max),
			@p_error_code varchar(4000),
			@p_error_message varchar(4000),
			@frequencyPartition nvarchar(10),
			@p_g_job_status_running varchar(100),
			@p_g_job_status_success varchar(100),
			@p_g_job_status_failed  varchar(100),
			@p_g_job_status_aborted varchar(100),
			@p_job_status varchar(100) = 'SUCCESS'

    set @v_job_id= (
        select top 1
        JOB_ID
    from [dbo].[SAP_ETL_JOB]
    where 1=1 and ACTIVE_FLG='Y' and TGT_TABLE = @tgt_TableName
    )
    set @v_jobinstance_id = convert(
        bigint, 
        convert(varchar, @v_batch_id) + convert(varchar, @v_job_id)
    )
    set @v_src_tablename = (
        select top 1
        SRC_TABLE
    from [dbo].[SAP_ETL_JOB]
    where 1=1
        and ACTIVE_FLG = 'Y'
        and TGT_TABLE = @tgt_TableName
    )

    print '@v_jobinstance_id is ' + cast(@v_jobinstance_id as varchar)
    print '@v_src_tablename ' + @v_src_tablename

    execute	 [dbo].[SAP_proc_etl_util_start_job_instance]
		@p_tgt_table_name 	= @tgt_TableName,
		@p_batch_id 		= @v_batch_id,
		@p_job_instance_id  = @v_jobinstance_id OUTPUT,
		@p_src_chk_column 	= @p_src_chk_column OUTPUT,
		@p_tgt_chk_column 	= @p_tgt_chk_column OUTPUT,
		@p_return_code 		= @p_return_code OUTPUT,
		@p_return_msg 		= @p_return_msg OUTPUT


    BEGIN TRY
		-- 1. Check existence of (and remove) temp table
        PRINT '1. Check existence and remove of temp table'

        IF OBJECT_ID(N'tempdb..#W_CMMS_MATU_F_tmp') IS NOT NULL 
        BEGIN
            PRINT N'DELETE temporary table #W_CMMS_MATU_F_tmp'
            DROP Table #W_CMMS_MATU_F_tmp
        END;


		-- 2. Select everything into temp table
        PRINT '2. Select everything into temp table'

        SELECT
              CONVERT(BIGINT, MATU_ID)                          AS MATU_ID
            , CONVERT(BIGINT, INVUSE_ID)                        AS INVUSE_ID
            , CONVERT(BIGINT, INVUSELINE_ID)                    AS INVUSELINE_ID
            
            , CONVERT(DECIMAL(38, 20), [CURBAL])                AS [CURBAL]
            , CONVERT(NVARCHAR(100), [LOCATION])                AS [LOCATION]
            , CONVERT(NVARCHAR(100), [LINE_TYPE])               AS [LINE_TYPE]
            , CONVERT(DECIMAL(38, 20), [UNITCOST])              AS [UNITCOST]
            , CONVERT(DECIMAL(38, 20), [QTY_REQUESTED])         AS [QTY_REQUESTED]
            , CONVERT(NVARCHAR(100), [REFWO])                   AS [REFWO]
            , CONVERT(NVARCHAR(100), [STORELOC])                AS [STORELOC]
            , CONVERT(NVARCHAR(100), [DESCRIPTION])             AS [DESCRIPTION]
            , CONVERT(DECIMAL(38, 20), [LINECOST])              AS [LINECOST]
            , CONVERT(NVARCHAR(100), [BINNUM])                  AS [BINNUM]
            , CONVERT(NVARCHAR(100), [CURRENCY_CODE])           AS [CURRENCY_CODE]
            , CONVERT(NVARCHAR(100), [PONUM])                   AS [PONUM]
            , CONVERT(NVARCHAR(100), [ISSUE_UNIT])              AS [ISSUE_UNIT]
            , CONVERT(NVARCHAR(100), [ITEM_NUM])                AS [ITEM_NUM]
            , CONVERT(NVARCHAR(100), [MRNUM])                   AS [MRNUM]
            , CONVERT(DECIMAL(38, 20), [ACTUAL_COST])           AS [ACTUAL_COST]
            , CONVERT(DECIMAL(38, 20), [EXCHANGERATE])          AS [EXCHANGERATE]
            , CONVERT(DATETIME2, [TRANSDATE], 103)              AS [TRANSDATE]
            , CONVERT(DATETIME2, [ACTUALDATE], 103)             AS [ACTUALDATE]
            , CONVERT(NVARCHAR(100), [ASSET_NUM])               AS [ASSET_NUM]
            , CONVERT(NVARCHAR(100), [TO_SITEID])               AS [TO_SITEID]
            , CONVERT(NVARCHAR(100), [ISSUE_TYPE])              AS [ISSUE_TYPE]
            , CONVERT(NVARCHAR(100), [ORG_ID])                  AS [ORG_ID]
            , CONVERT(DECIMAL(38, 20), [QUANTITY])              AS [QUANTITY]

            , CONVERT(NVARCHAR, MATU_ID)                        AS W_INTEGRATION_ID
            , 'N'                                               AS W_DELETE_FLG
            , 'N' 											    AS W_UPDATE_FLG
            , 8                                                 AS W_DATASOURCE_NUM_ID
            , DATEADD(HH, 7, GETDATE())                         AS W_INSERT_DT
            , DATEADD(HH, 7, GETDATE())                         AS W_UPDATE_DT
            , @p_batch_id                                       AS W_BATCH_ID
        INTO #W_CMMS_MATU_F_tmp
        FROM [STG].[W_CMMS_MATU_FS]


        -- 3. Update main table using W_INTEGRATION_ID
        PRINT '3. Update main table using W_INTEGRATION_ID'

        -- 3.1. Mark existing records by flag 'Y'
        PRINT '3.1. Mark existing records by flag ''Y'''

        UPDATE #W_CMMS_MATU_F_tmp
		SET W_UPDATE_FLG = 'Y'
		FROM #W_CMMS_MATU_F_tmp tg
        INNER JOIN [FND].[W_CMMS_MATU_F] sc
        ON sc.W_INTEGRATION_ID = tg.W_INTEGRATION_ID

        -- 3.2. Start updating
        PRINT '3.2. Start updating'

		UPDATE [FND].[W_CMMS_MATU_F]
		SET
            MATU_ID = src.MATU_ID
            , INVUSE_ID = src.INVUSE_ID
            , INVUSELINE_ID = src.INVUSELINE_ID

            , [CURBAL] = src.[CURBAL]
            , [LOCATION] = src.[LOCATION]
            , [LINE_TYPE] = src.[LINE_TYPE]
            , [UNITCOST] = src.[UNITCOST]
            , [QTY_REQUESTED] = src.[QTY_REQUESTED]
            , [REFWO] = src.[REFWO]
            , [STORELOC] = src.[STORELOC]
            , [DESCRIPTION] = src.[DESCRIPTION]
            , [LINECOST] = src.[LINECOST]
            , [BINNUM] = src.[BINNUM]
            , [CURRENCY_CODE] = src.[CURRENCY_CODE]
            , [PONUM] = src.[PONUM]
            , [ISSUE_UNIT] = src.[ISSUE_UNIT]
            , [ITEM_NUM] = src.[ITEM_NUM]
            , [MRNUM] = src.[MRNUM]
            , [ACTUAL_COST] = src.[ACTUAL_COST]
            , [EXCHANGERATE] = src.[EXCHANGERATE]
            , [TRANSDATE] = src.[TRANSDATE]
            , [ACTUALDATE] = src.[ACTUALDATE]
            , [ASSET_NUM] = src.[ASSET_NUM]
            , [TO_SITEID] = src.[TO_SITEID]
            , [ISSUE_TYPE] = src.[ISSUE_TYPE]
            , [ORG_ID] = src.[ORG_ID]
            , [QUANTITY] = src.[QUANTITY]

			, W_DELETE_FLG = src.W_DELETE_FLG
			, W_DATASOURCE_NUM_ID = src.W_DATASOURCE_NUM_ID
			, W_INSERT_DT = src.W_INSERT_DT
			, W_BATCH_ID = src.W_BATCH_ID
			, W_INTEGRATION_ID = src.W_INTEGRATION_ID
			, W_UPDATE_DT = DATEADD(HH, 7, GETDATE())
        FROM [FND].[W_CMMS_MATU_F] tgt
        INNER JOIN #W_CMMS_MATU_F_tmp src ON src.W_INTEGRATION_ID = tgt.W_INTEGRATION_ID


	    -- 4. Insert non-existed records to main table from temp table
        PRINT '4. Insert non-existed records to main table from temp table'

        INSERT INTO [FND].[W_CMMS_MATU_F](
            MATU_ID
            , INVUSE_ID
            , INVUSELINE_ID
            
            , [CURBAL]
            , [LOCATION]
            , [LINE_TYPE]
            , [UNITCOST]
            , [QTY_REQUESTED]
            , [REFWO]
            , [STORELOC]
            , [DESCRIPTION]
            , [LINECOST]
            , [BINNUM]
            , [CURRENCY_CODE]
            , [PONUM]
            , [ISSUE_UNIT]
            , [ITEM_NUM]
            , [MRNUM]
            , [ACTUAL_COST]
            , [EXCHANGERATE]
            , [TRANSDATE]
            , [ACTUALDATE]
            , [ASSET_NUM]
            , [TO_SITEID]
            , [ISSUE_TYPE]
            , [ORG_ID]
            , [QUANTITY]

            , W_DELETE_FLG
            , W_DATASOURCE_NUM_ID
            , W_INSERT_DT
            , W_UPDATE_DT
            , W_BATCH_ID
            , W_INTEGRATION_ID
        )
        SELECT
            MATU_ID
            , INVUSE_ID
            , INVUSELINE_ID
            
            , [CURBAL]
            , [LOCATION]
            , [LINE_TYPE]
            , [UNITCOST]
            , [QTY_REQUESTED]
            , [REFWO]
            , [STORELOC]
            , [DESCRIPTION]
            , [LINECOST]
            , [BINNUM]
            , [CURRENCY_CODE]
            , [PONUM]
            , [ISSUE_UNIT]
            , [ITEM_NUM]
            , [MRNUM]
            , [ACTUAL_COST]
            , [EXCHANGERATE]
            , [TRANSDATE]
            , [ACTUALDATE]
            , [ASSET_NUM]
            , [TO_SITEID]
            , [ISSUE_TYPE]
            , [ORG_ID]
            , [QUANTITY]

            , W_DELETE_FLG
            , W_DATASOURCE_NUM_ID
            , W_INSERT_DT
            , W_UPDATE_DT
            , W_BATCH_ID
            , W_INTEGRATION_ID
        FROM #W_CMMS_MATU_F_tmp
        where W_UPDATE_FLG = 'N'		

		/*delete & re-insert data refresh*/
		DELETE FROM [dbo].[SAP_ETL_DATAFRESH_CONF] WHERE UPPER(TABLE_NAME) = UPPER(@tgt_TableName)

		INSERT INTO [dbo].[SAP_ETL_DATAFRESH_CONF](
            TABLE_NAME,
            REFRESH_DATE,
            IS_FULLLOAD,
            IS_EXIST_SSAS,
            LAST_UPDATE_DATE
        )
        SELECT DISTINCT
            @tgt_TableName,
            NULL,
            'Y',
            'Y',
            DATEADD(HH, 7, GETDATE())
        FROM (
                SELECT *
            FROM W_CMMS_MATU_F
            ) M
        WHERE 1=1
            AND W_BATCH_ID = @p_batch_id
            AND W_DELETE_FLG = 'N'

            SET @src_rownum = ( SELECT COUNT(1)
        FROM #W_CMMS_MATU_F_tmp );
            SET @tgt_rownum = ( 
                SELECT
            COUNT(DISTINCT W_INTEGRATION_ID)
        FROM FND.W_CMMS_MATU_F
        WHERE 1=1
            AND W_DELETE_FLG = 'N'
            AND W_BATCH_ID = @p_batch_id
            );

	END TRY


	BEGIN CATCH
		set @p_job_status = 'FAILED'
		set @p_error_message = ERROR_MESSAGE()
		print @p_error_message
	END CATCH

    execute	[dbo].[SAP_proc_etl_util_end_job_instance]
			@p_job_instance_id 	= @v_jobinstance_id,
			@p_return_code 		= @p_return_code OUTPUT,
			@p_return_msg 		= @p_return_msg OUTPUT,
			@p_status_code 		= @p_job_status,
			@p_error_message 	= @p_error_message,
			@src_rownum 		= @src_rownum,
			@tgt_rownum 		= @tgt_rownum,
			@src_chk_value 		= @src_chk_value,
			@tgt_chk_value 		= @tgt_chk_value
END
GO