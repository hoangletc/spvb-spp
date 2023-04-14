SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
ALTER PROC [dbo].[SAP_proc_load_w_excel_spp_scrap_sales_f] @p_batch_id [bigint] AS
BEGIN
	DECLARE	@tgt_TableName nvarchar(200) = N'dbo.W_EXCEL_SPP_SCRAP_SALES_F',
			@sql nvarchar(max),
	        @column_name varchar(4000),
	        @no_row bigint	,
			@diff_row bigint, 

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

    set @v_job_id= (select top 1 JOB_ID from [dbo].[SAP_ETL_JOB] where 1=1 and ACTIVE_FLG='Y' and TGT_TABLE = @tgt_TableName)
	set @v_jobinstance_id = convert(bigint, convert(varchar,@v_batch_id)+convert(varchar,@v_job_id))
	set @v_src_tablename = (select top 1 SRC_TABLE from [dbo].[SAP_ETL_JOB] where 1=1 and ACTIVE_FLG='Y' and TGT_TABLE = @tgt_TableName /*+'T'*/) 

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

		print 'Check TMP table'
		IF OBJECT_ID(N'tempdb..#W_EXCEL_SPP_SCRAP_SALES_F_tmp') IS NOT NULL 
			BEGIN
                PRINT N'DELETE temporary table #W_EXCEL_SPP_SCRAP_SALES_F_tmp'
				DROP Table #W_EXCEL_SPP_SCRAP_SALES_F_tmp
            END
	
		TRUNCATE TABLE W_EXCEL_SPP_SCRAP_SALES_F
		print 'Insert into TMP'
			select 
			  CONVERT(VARCHAR, CONVERT(DATE, CONCAT(YEAR, '-', MONTH, '-01')), 112) AS DATE_WID
			, ISNULL(PL_X.PLANT_WID,0) AS PLANT_WID
			, F.MATERIAL_TYPE
			, F.UNIT
			, F.PLANT
			, F.PRICE
			, F.SCRAP_QTY
			,'N' AS [W_DELETE_FLG]
			,F.[W_DATASOURCE_NUM_ID]
			, CONCAT(F.MATERIAL_TYPE, '~', F.PLANT, '~', [YEAR], '~', [MONTH]) AS [W_INTEGRATION_ID]
			,DATEADD(HH, 7, GETDATE()) AS [W_INSERT_DT]
			,DATEADD(HH, 7, GETDATE()) AS [W_UPDATE_DT]
			,@v_batch_id AS [W_BATCH_ID]
			,'N' AS W_UPDATE_FLG
			, F.FILE_NAME
		INTO #W_EXCEL_SPP_SCRAP_SALES_F_tmp
		from FND.W_EXCEL_SPP_SCRAP_SALES_F F
		LEFT JOIN [dbo].[W_SAP_PLANT_EXTENDED_D] PL_X ON F.PLANT = PL_X.PLANT_NAME_2 AND PL_X.STo_LOC = ''
		WHERE F.W_DELETE_FLG = 'N'

		INSERT INTO W_EXCEL_SPP_SCRAP_SALES_F
			(
				  DATE_WID
				, PLANT_WID
				, MATERIAL_TYPE
				, UNIT
				, PLANT
				, PRICE
				, SCRAP_QTY
				, W_DELETE_FLG
				, W_DATASOURCE_NUM_ID
				, W_INTEGRATION_ID
				, W_INSERT_DT
				, W_UPDATE_DT
				, W_BATCH_ID
				, FILE_NAME
			)
		SELECT
				  DATE_WID
				, PLANT_WID
				, MATERIAL_TYPE
				, UNIT
				, PLANT
				, PRICE
				, SCRAP_QTY
				, W_DELETE_FLG
				, W_DATASOURCE_NUM_ID
				, W_INTEGRATION_ID
				, W_INSERT_DT
				, W_UPDATE_DT
				, W_BATCH_ID
				, FILE_NAME
		FROM #W_EXCEL_SPP_SCRAP_SALES_F_tmp 
		WHERE W_UPDATE_FLG='N'

		
		SET @src_rownum = (SELECT count(1) FROM #W_EXCEL_SPP_SCRAP_SALES_F_tmp)
		SET @tgt_rownum = (SELECT COUNT(DISTINCT W_INTEGRATION_ID) FROM W_EXCEL_SPP_SCRAP_SALES_F WHERE W_BATCH_ID = @p_batch_id);

		/*delete & re-insert data refresh*/
		DELETE FROM [dbo].[SAP_ETL_DATAFRESH_CONF] WHERE UPPER(TABLE_NAME) = UPPER(@tgt_TableName)

		INSERT INTO [dbo].[SAP_ETL_DATAFRESH_CONF] (TABLE_NAME, REFRESH_DATE, IS_FULLLOAD, IS_EXIST_SSAS, LAST_UPDATE_DATE)
		SELECT DISTINCT @tgt_TableName, NULL, 'Y', 'Y', DATEADD(HH, 7, GETDATE())
		FROM
			( 
			SELECT * FROM W_EXCEL_SPP_SCRAP_SALES_F
			) M
		WHERE W_BATCH_ID = @p_batch_id
			AND W_DELETE_FLG = 'N'

		print @src_rownum
		print @tgt_rownum

		print 'Check TMP table'
		IF OBJECT_ID(N'tempdb..#W_EXCEL_SPP_SCRAP_SALES_F_tmp') IS NOT NULL 
			BEGIN
                PRINT N'DELETE temporary table #W_EXCEL_SPP_SCRAP_SALES_F_tmp'
				DROP Table #W_EXCEL_SPP_SCRAP_SALES_F_tmp
            END

		set @diff_row= (select count(W_INTEGRATION_ID) FROM W_EXCEL_SPP_SCRAP_SALES_F GROUP BY W_INTEGRATION_ID HAVING COUNT(W_INTEGRATION_ID)>1)
		print @diff_row

		if @diff_row <>0
			throw 50000, 'Duplicate data', 1


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
