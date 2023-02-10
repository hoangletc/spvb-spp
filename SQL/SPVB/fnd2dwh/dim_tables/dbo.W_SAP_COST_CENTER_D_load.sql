SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

IF (OBJECT_ID('[dbo].[SAP_proc_load_w_sap_cost_center_d]') is not null)
BEGIN
    DROP PROCEDURE [dbo].[SAP_proc_load_w_sap_cost_center_d]
END;
GO

CREATE PROC [dbo].[SAP_proc_load_w_sap_cost_center_d]
    @p_batch_id [bigint]
AS 
BEGIN
    DECLARE	@tgt_TableName nvarchar(200) = N'dbo.W_SAP_COST_CENTER_D',
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
		-- 1. Check existence and remove of temp table
        PRINT '1. Check existence and remove of temp table'

        IF OBJECT_ID(N'tempdb..#W_SAP_COST_CENTER_D_tmp') IS NOT NULL 
        BEGIN
            PRINT N'DELETE temporary table #W_SAP_COST_CENTER_D_tmp'
            DROP Table #W_SAP_COST_CENTER_D_tmp
        END;


		-- 2. Select everything into temp table
        PRINT '2. Select everything into temp table'

		SELECT
            CONVERT(nvarchar(100), COST_CENTER.MANDT)       AS MANDT
            , CONVERT(nvarchar(100), COST_CENTER.KOKRS)     AS KOKRS
            , CONVERT(nvarchar(100), COST_CENTER.KOSTL)     AS KOSTL
            , CONVERT(nvarchar(100), COST_CENTER.DATBI)     AS DATBI
            , CONVERT(nvarchar(100), DATAB)                 AS DATAB
            , CONVERT(nvarchar(100), BKZKP)                 AS BKZKP
            , CONVERT(nvarchar(100), PKZKP)                 AS PKZKP
            , CONVERT(nvarchar(100), BUKRS)                 AS BUKRS
            , CONVERT(nvarchar(100), CSKT.KTEXT)            AS KTEXT
            
			, CONVERT(
                [varbinary](100), 
                CONCAT_WS(
                    '~', 
                    COST_CENTER.MANDT, 
                    COST_CENTER.KOKRS, 
                    COST_CENTER.KOSTL,
                    COST_CENTER.DATBI
                )
            )                                               AS W_INTEGRATION_ID
			, 'N'                                           AS W_DELETE_FLG
			, 1                                             AS W_DATASOURCE_NUM_ID
			, GETDATE()                                     AS W_INSERT_DT
			, GETDATE()                                     AS W_UPDATE_DT
			, @p_batch_id                                   AS W_BATCH_ID
            , 'N'                                           AS W_UPDATE_FLG
        INTO #W_SAP_COST_CENTER_D_tmp
        FROM [FND].[W_SAP_CSKS_D] COST_CENTER
            LEFT JOIN [FND].[W_SAP_CSKT_D] CSKT ON 1=1
                AND CSKT.MANDT = COST_CENTER.MANDT
                AND CSKT.KOKRS = COST_CENTER.KOKRS
                AND CSKT.KOSTL = COST_CENTER.KOSTL
                AND CSKT.DATBI = COST_CENTER.DATBI
        WHERE 1=1
            AND COST_CENTER.MANDT = 300
                 
        -- 3. Update main table using W_INTEGRATION_ID
        PRINT '3. Update main table using W_INTEGRATION_ID'

        -- 3.1. Mark existing records by flag 'Y'
        PRINT '3.1. Mark existing records by flag ''Y'''

        UPDATE #W_SAP_COST_CENTER_D_tmp
		SET W_UPDATE_FLG = 'Y'
		FROM #W_SAP_COST_CENTER_D_tmp tg
        INNER JOIN [dbo].[W_SAP_COST_CENTER_D] sc 
        ON sc.W_INTEGRATION_ID = tg.W_INTEGRATION_ID

        -- 3.2. Start updating
        PRINT '3.2. Start updating'

		UPDATE  [dbo].[W_SAP_COST_CENTER_D]
		SET 
           MANDT = src.MANDT
            , KOKRS = src.KOKRS
            , KOSTL = src.KOSTL
            , DATBI = src.DATBI
            , DATAB = src.DATAB
            , BKZKP = src.BKZKP
            , PKZKP = src.PKZKP
            , BUKRS = src.BUKRS
            , KTEXT = src.KTEXT

			, W_DELETE_FLG = src.W_DELETE_FLG
			, W_DATASOURCE_NUM_ID = src.W_DATASOURCE_NUM_ID
			, W_INSERT_DT = src.W_INSERT_DT
			, W_BATCH_ID = src.W_BATCH_ID
			, W_INTEGRATION_ID = src.W_INTEGRATION_ID
			, W_UPDATE_DT = getdate()

        FROM [dbo].[W_SAP_COST_CENTER_D] tgt
        INNER JOIN #W_SAP_COST_CENTER_D_tmp src ON src.W_INTEGRATION_ID = tgt.W_INTEGRATION_ID


	    -- 4. Insert non-existed records to main table from temp table
        PRINT '4. Insert non-existed records to main table from temp table'

        INSERT INTO [dbo].[W_SAP_COST_CENTER_D](
             MANDT
            , KOKRS
            , KOSTL
            , DATBI
            , DATAB
            , BKZKP
            , PKZKP
            , BUKRS
            , KTEXT

            , W_DELETE_FLG
			, W_DATASOURCE_NUM_ID
			, W_INSERT_DT
			, W_UPDATE_DT
			, W_BATCH_ID
			, W_INTEGRATION_ID
        )
        SELECT
            MANDT
            , KOKRS
            , KOSTL
            , DATBI
            , DATAB
            , BKZKP
            , PKZKP
            , BUKRS
            , KTEXT

            , W_DELETE_FLG
			, W_DATASOURCE_NUM_ID
			, W_INSERT_DT
			, W_UPDATE_DT
			, W_BATCH_ID
			, W_INTEGRATION_ID
        FROM #W_SAP_COST_CENTER_D_tmp
        where W_UPDATE_FLG = 'N'

		

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
            FROM W_SAP_COST_CENTER_D
        ) M
        WHERE 1=1
            AND W_BATCH_ID = @p_batch_id
            AND W_DELETE_FLG = 'N'

		SET @src_rownum = ( SELECT COUNT(1) FROM #W_SAP_COST_CENTER_D_tmp );
		SET @tgt_rownum = ( 
            SELECT 
                COUNT(DISTINCT W_INTEGRATION_ID)
            FROM W_SAP_COST_CENTER_D
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
