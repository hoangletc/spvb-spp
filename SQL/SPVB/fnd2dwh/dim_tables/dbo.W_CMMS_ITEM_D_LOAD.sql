SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

IF (OBJECT_ID('[dbo].[CMMS_proc_load_w_spp_item_d]') is not null)
BEGIN
    DROP PROCEDURE [dbo].[CMMS_proc_load_w_spp_item_d]
END;
GO

CREATE PROC [dbo].[CMMS_proc_load_w_spp_item_d]
    @p_batch_id [bigint]
AS
BEGIN
    DECLARE	@tgt_TableName nvarchar(200) = N'dbo.W_CMMS_ITEM_D',
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

        IF OBJECT_ID(N'tempdb..#W_CMMS_ITEM_D_tmp') IS NOT NULL 
        BEGIN
            PRINT N'DELETE temporary table #W_CMMS_ITEM_D_tmp'
            DROP Table #W_CMMS_ITEM_D_tmp
        END;


		-- 2. Select everything into temp table
        PRINT '2. Select everything into temp table'

		SELECT
            CONVERT(nvarchar(1000), [DESCRIPTION])                  AS [DESCRIPTION]
            , CONVERT(nvarchar(100), ISKIT)                          AS ISKIT
            , CONVERT(nvarchar(100), ISSUE_UNIT)                     AS ISSUE_UNIT
            , CONVERT(nvarchar(100), ITEM_NUM)                       AS ITEM_NUM
            , CONVERT(nvarchar(100), ITEM_TYPE)                      AS ITEM_TYPE
            , CONVERT(nvarchar(100), LOT_TYPE)                       AS LOT_TYPE
            , CONVERT(nvarchar(100), LOT_TYPE_DESCRIPTION)           AS LOT_TYPE_DESCRIPTION
            , CONVERT(nvarchar(100), ORDER_UNIT)                     AS ORDER_UNIT
            , CONVERT(nvarchar(100), SPP_CLASSIFICATION)             AS SPP_CLASSIFICATION
            , CONVERT(
                nvarchar(100),
                CASE WHEN SPP_CLASSIFICATION_DESCRIPTION = ''
                THEN 'Others'
                ELSE SPP_CLASSIFICATION_DESCRIPTION END
            )                                                        AS SPP_CLASSIFICATION_DESCRIPTION
            , CONVERT(nvarchar(100), SPVB_ITEM_MUSTNO)               AS SPVB_ITEM_MUSTNO
            , CONVERT(nvarchar(100), SPVB_MAX)                       AS SPVB_MAX
            , CONVERT(nvarchar(100), SPVB_MIN)                       AS SPVB_MIN
            , CONVERT(nvarchar(100), SPVB_MUSTRETURN)                AS SPVB_MUSTRETURN
            , CONVERT(nvarchar(100), SPVB_PLANT)                     AS SPVB_PLANT
            , CONVERT(nvarchar(100), [STATUS])                       AS [STATUS]
            , CONVERT(nvarchar(100), STATUS_DESCRIPTION)             AS STATUS_DESCRIPTION
            , ITEM_ID                                               AS ITEM_ID
            , CONVERT(nvarchar(300), SPVB_PRODUCTLINE)              AS SPVB_PRODUCTLINE
            , CONVERT(nvarchar(300), SPVB_MACHINE)                  AS SPVB_MACHINE

			, CONVERT(
                nvarchar(100), 
                CONCAT_WS('~', ITEM_ID, ITEM_NUM, ISSUE_UNIT, SPP_CLASSIFICATION)
                )                                                   AS W_INTEGRATION_ID
			, 'N'                                                   AS W_DELETE_FLG
			, 1                                                     AS W_DATASOURCE_NUM_ID
			, GETDATE()                                             AS W_INSERT_DT
			, GETDATE()                                             AS W_UPDATE_DT
			, NULL                                                  AS W_BATCH_ID
            , 'N'                                                   AS W_UPDATE_FLG
    INTO #W_CMMS_ITEM_D_tmp
    FROM [FND].[W_CMMS_ITEM_D] F

        -- 3. Update main table using W_INTEGRATION_ID
        PRINT '3. Update main table using W_INTEGRATION_ID'

        -- 3.1. Mark existing records by flag 'Y'
        PRINT '3.1. Mark existing records by flag ''Y'''

        UPDATE #W_CMMS_ITEM_D_tmp
		SET W_UPDATE_FLG = 'Y'
		FROM #W_CMMS_ITEM_D_tmp tg
        INNER JOIN [dbo].[W_CMMS_ITEM_D] sc
        ON sc.W_INTEGRATION_ID = tg.W_INTEGRATION_ID

        -- 3.2. Start updating
        PRINT '3.2. Start updating'

		UPDATE [dbo].[W_CMMS_ITEM_D]
		SET 
			[DESCRIPTION] = src.DESCRIPTION
            , ISKIT = src.ISKIT
            , ISSUE_UNIT = src.ISSUE_UNIT
            , ITEM_NUM = src.ITEM_NUM
            , ITEM_TYPE = src.ITEM_TYPE
            , LOT_TYPE = src.LOT_TYPE
            , LOT_TYPE_DESCRIPTION = src.LOT_TYPE_DESCRIPTION
            , ORDER_UNIT = src.ORDER_UNIT
            , SPP_CLASSIFICATION = src.SPP_CLASSIFICATION
            , SPP_CLASSIFICATION_DESCRIPTION = src.SPP_CLASSIFICATION_DESCRIPTION
            , SPVB_ITEM_MUSTNO = src.SPVB_ITEM_MUSTNO
            , SPVB_MAX = src.SPVB_MAX
            , SPVB_MIN = src.SPVB_MIN
            , SPVB_MUSTRETURN = src.SPVB_MUSTRETURN
            , SPVB_PLANT = src.SPVB_PLANT
            , [STATUS] = src.STATUS
            , STATUS_DESCRIPTION = src.STATUS_DESCRIPTION
            , ITEM_ID = src.ITEM_ID
            , SPVB_PRODUCTLINE = src.SPVB_PRODUCTLINE
            , SPVB_MACHINE = src.SPVB_MACHINE

			, W_DELETE_FLG = src.W_DELETE_FLG
			, W_DATASOURCE_NUM_ID = src.W_DATASOURCE_NUM_ID
			, W_INSERT_DT = src.W_INSERT_DT
			, W_BATCH_ID = src.W_BATCH_ID
			, W_INTEGRATION_ID = src.W_INTEGRATION_ID
			, W_UPDATE_DT = getdate()
        FROM [dbo].[W_CMMS_ITEM_D] tgt
        INNER JOIN #W_CMMS_ITEM_D_tmp src ON src.W_INTEGRATION_ID = tgt.W_INTEGRATION_ID


	    -- 4. Insert non-existed records to main table from temp table
        PRINT '4. Insert non-existed records to main table from temp table'

        INSERT INTO [dbo].[W_CMMS_ITEM_D] (
            [DESCRIPTION]
            , ISKIT
            , ISSUE_UNIT
            , ITEM_NUM
            , ITEM_TYPE
            , LOT_TYPE
            , LOT_TYPE_DESCRIPTION
            , ORDER_UNIT
            , SPP_CLASSIFICATION
            , SPP_CLASSIFICATION_DESCRIPTION
            , SPVB_ITEM_MUSTNO
            , SPVB_MAX
            , SPVB_MIN
            , SPVB_MUSTRETURN
            , SPVB_PLANT
            , [STATUS]
            , STATUS_DESCRIPTION
            , ITEM_ID
            , [SPVB_PRODUCTLINE]
            , [SPVB_MACHINE]

            , W_DELETE_FLG
            , W_DATASOURCE_NUM_ID
            , W_INSERT_DT
            , W_UPDATE_DT
            , W_BATCH_ID
            , W_INTEGRATION_ID
        )
        SELECT
            [DESCRIPTION]
            , ISKIT
            , ISSUE_UNIT
            , ITEM_NUM
            , ITEM_TYPE
            , LOT_TYPE
            , LOT_TYPE_DESCRIPTION
            , ORDER_UNIT
            , SPP_CLASSIFICATION
            , SPP_CLASSIFICATION_DESCRIPTION
            , SPVB_ITEM_MUSTNO
            , SPVB_MAX
            , SPVB_MIN
            , SPVB_MUSTRETURN
            , SPVB_PLANT
            , [STATUS]
            , STATUS_DESCRIPTION
            , ITEM_ID
            , [SPVB_PRODUCTLINE]
            , [SPVB_MACHINE]

            , W_DELETE_FLG
            , W_DATASOURCE_NUM_ID
            , W_INSERT_DT
            , W_UPDATE_DT
            , W_BATCH_ID
            , W_INTEGRATION_ID
        FROM #W_CMMS_ITEM_D_tmp
        WHERE W_UPDATE_FLG = 'N'

		

		/*delete & re-insert data refresh*/
		DELETE FROM [dbo].[SAP_ETL_DATAFRESH_CONF] WHERE UPPER(TABLE_NAME) = UPPER(@tgt_TableName)

		INSERT INTO [dbo].[SAP_ETL_DATAFRESH_CONF]
        (
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
        FROM W_CMMS_ITEM_D
            ) M
    WHERE 1=1
        AND W_BATCH_ID = @p_batch_id
        AND W_DELETE_FLG = 'N'

            SET @src_rownum = ( SELECT COUNT(1)
    FROM #W_CMMS_ITEM_D_tmp );
            SET @tgt_rownum = ( 
                SELECT
        COUNT(DISTINCT W_INTEGRATION_ID)
    FROM W_CMMS_ITEM_D
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