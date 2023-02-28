SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [dbo].[CMMS_proc_load_w_spp_inve_d]
    @p_batch_id [bigint]
AS
BEGIN
    DECLARE	@tgt_TableName nvarchar(200) = N'dbo.W_CMMS_INVE_D',
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

        IF OBJECT_ID(N'tempdb..#W_CMMS_INVE_D_tmp') IS NOT NULL 
        BEGIN
            PRINT N'DELETE temporary table #W_CMMS_INVE_D_tmp'
            DROP Table #W_CMMS_INVE_D_tmp
        END;


		-- 2. Select everything into temp table
        PRINT '2. Select everything into temp table'

        SELECT
            CONVERT(nvarchar(100), ISSUE_UNIT)          AS ISSUE_UNIT
            , MIN_LEVEL                                 AS MIN_LEVEL
            , MAX_LEVEL                                 AS MAX_LEVEL
            , CONVERT(nvarchar(100), INVE_ID)           AS INVE_ID
            , CONVERT(nvarchar(100), SITE_ID)           AS SITE_ID
            , CONVERT(nvarchar(1000), STATUS_DESC)      AS STATUS_DESC
            , CONVERT(nvarchar(100), ORDER_UNIT)        AS ORDER_UNIT
            , CONVERT(nvarchar(100), ITEM_NUM)          AS ITEM_NUM
            , ISNULL(
                LAST_ISSUE_DATE, 
                CONVERT(DATETIME2, LAST_ISSUE_DATE)
            )                                           AS LAST_ISSUE_DATE
            , CONVERT(nvarchar(100), [LOCATION])        AS [LOCATION]

            , CONVERT(
                nvarchar(100), 
                INVE_ID
            )                                           AS W_INTEGRATION_ID
            , 'N'                                       AS W_DELETE_FLG
            , 8                                         AS W_DATASOURCE_NUM_ID
            , DATEADD(HH, 7, GETDATE())                 AS W_INSERT_DT
            , DATEADD(HH, 7, GETDATE())                 AS W_UPDATE_DT
            , W_BATCH_ID                                AS W_BATCH_ID
            , 'N'                                       AS W_UPDATE_FLG
        INTO #W_CMMS_INVE_D_tmp
        FROM [FND].[W_CMMS_INVE_D] AST


        -- 3. Update main table using W_INTEGRATION_ID
        PRINT '3. Update main table using W_INTEGRATION_ID'

        -- 3.1. Mark existing records by flag 'Y'
        PRINT '3.1. Mark existing records by flag ''Y'''

        UPDATE #W_CMMS_INVE_D_tmp
		SET W_UPDATE_FLG = 'Y'
		FROM #W_CMMS_INVE_D_tmp tg
        INNER JOIN [dbo].[W_CMMS_INVE_D] sc
        ON sc.W_INTEGRATION_ID = tg.W_INTEGRATION_ID

        -- 3.2. Start updating
        PRINT '3.2. Start updating'

		UPDATE [dbo].[W_CMMS_INVE_D]
		SET
            ISSUE_UNIT = src.ISSUE_UNIT
            , MIN_LEVEL = src.MIN_LEVEL
            , MAX_LEVEL = src.MAX_LEVEL
            , INVE_ID = src.INVE_ID
            , SITE_ID = src.SITE_ID
            , STATUS_DESC = src.STATUS_DESC
            , ORDER_UNIT = src.ORDER_UNIT
            , ITEM_NUM = src.ITEM_NUM
            , LAST_ISSUE_DATE = src.LAST_ISSUE_DATE
            , [LOCATION] = src.LOCATION

			, W_DELETE_FLG = src.W_DELETE_FLG
			, W_DATASOURCE_NUM_ID = src.W_DATASOURCE_NUM_ID
			, W_INSERT_DT = src.W_INSERT_DT
			, W_BATCH_ID = src.W_BATCH_ID
			, W_INTEGRATION_ID = src.W_INTEGRATION_ID
			, W_UPDATE_DT = DATEADD(HH, 7, GETDATE())
        FROM [dbo].[W_CMMS_INVE_D] tgt
        INNER JOIN #W_CMMS_INVE_D_tmp src ON src.W_INTEGRATION_ID = tgt.W_INTEGRATION_ID


	    -- 4. Insert non-existed records to main table from temp table
        PRINT '4. Insert non-existed records to main table from temp table'

        INSERT INTO [dbo].[W_CMMS_INVE_D](
            ISSUE_UNIT
            , MIN_LEVEL
            , MAX_LEVEL
            , INVE_ID
            , SITE_ID
            , STATUS_DESC
            , ORDER_UNIT
            , ITEM_NUM
            , LAST_ISSUE_DATE
            , [LOCATION]

            , W_DELETE_FLG
            , W_DATASOURCE_NUM_ID
            , W_INSERT_DT
            , W_UPDATE_DT
            , W_BATCH_ID
            , W_INTEGRATION_ID
        )
        SELECT
            ISSUE_UNIT
            , MIN_LEVEL
            , MAX_LEVEL
            , INVE_ID
            , SITE_ID
            , STATUS_DESC
            , ORDER_UNIT
            , ITEM_NUM
            , LAST_ISSUE_DATE
            , [LOCATION]

            , W_DELETE_FLG
            , W_DATASOURCE_NUM_ID
            , W_INSERT_DT
            , W_UPDATE_DT
            , W_BATCH_ID
            , W_INTEGRATION_ID
        FROM #W_CMMS_INVE_D_tmp
        where W_UPDATE_FLG = 'N'		

		/*delete & re-insert data refresh*/
		DELETE FROM [dbo].[SAP_ETL_DATAFRESH_CONF] WHERE UPPER(TABLE_NAME) = UPPER(@tgt_TableName)

		INSERT INTO [dbo].[SAP_ETL_DATAFRESH_CONF] (
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
            FROM W_CMMS_INVE_D
            ) M
        WHERE 1=1
            AND W_BATCH_ID = @p_batch_id
            AND W_DELETE_FLG = 'N'

            SET @src_rownum = ( SELECT COUNT(1)
        FROM #W_CMMS_INVE_D_tmp );
            SET @tgt_rownum = ( 
                SELECT
                    COUNT(DISTINCT W_INTEGRATION_ID)
                FROM W_CMMS_INVE_D
                WHERE 1=1
                    AND W_DELETE_FLG = 'N'
                    AND W_BATCH_ID = @p_batch_id);

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