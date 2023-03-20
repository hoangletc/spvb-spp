SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

ALTER PROC [dbo].[CMMS_proc_load_fnd_w_spp_asset_status_f] @p_batch_id [bigint] AS
BEGIN
	DECLARE	@tgt_TableName nvarchar(200) = N'FND.W_CMMS_ASSET_STATUS_F',
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
			@p_job_status varchar(100) = 'SUCCESS',
			@n_split INT = 1
	;

    set @v_job_id= (select top 1 JOB_ID from [dbo].[SAP_ETL_JOB] where 1=1 and ACTIVE_FLG='Y' and TGT_TABLE = @tgt_TableName)
	set @v_jobinstance_id = convert(bigint, convert(varchar,@v_batch_id)+convert(varchar,@v_job_id))
	set @v_src_tablename = (select top 1 SRC_TABLE from [dbo].[SAP_ETL_JOB] where 1=1 and ACTIVE_FLG='Y' and TGT_TABLE = @tgt_TableName /*+'T' */) 

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
	
		/*Update soft delete flg for old data*/
		--NOTE [HoangLe - Mar17]: Do ASSET_STATUS (DOWNTIME) bị tách ra khi downtime dôi qua ngày mới, 
		-- nên cần cơ chế update hoàn toàn khác so với cái đang dùng mặc định, nên tạm thời đang disable đoạn code dưới

		-- UPDATE FND.W_CMMS_ASSET_STATUS_F SET 
		-- 	W_DELETE_FLG = 'Y'
		-- 	, W_UPDATE_DT = DATEADD(HH, 7, GETDATE())
		-- 	, W_BATCH_ID = @p_batch_id
		-- WHERE W_DELETE_FLG = 'N'
		-- 	AND FILE_PATH IN (SELECT DISTINCT FILE_PATH FROM STG.W_CMMS_ASSET_STATUS_F /*WHERE W_BATCH_ID = @p_batch_id*/)
		-- ;

		-- Step 0: Xoá các bảng tạm dùng trong quá trình tính toán
		IF OBJECT_ID(N'tempdb..#TMP_1') IS NOT NULL BEGIN
            PRINT N'DELETE temporary table #TMP_1'
            DROP Table #TMP_1
        END;
		IF OBJECT_ID(N'tempdb..#TMP_FINAL') IS NOT NULL BEGIN
            PRINT N'DELETE temporary table #TMP_FINAL'
            DROP Table #TMP_FINAL
        END;

		-- Step 1: Chọn ra tất cả các ASSET_STATUS có downtime bị dôi qua tháng hôm sau
		SELECT
			CONVERT(NVARCHAR(30), ASSETSTATUS_ID)       AS ASSETSTATUS_ID
			, CONVERT(DATETIME2, CHANGEDATE, 103)		AS CHANGEDATE
			, ROUND(DOWNTIME * 60, 0) 	                AS DOWNTIME
			, 'N' 										AS FLAG_DELETE
		INTO #TMP_1
		FROM STG.W_CMMS_ASSET_STATUS_FS
		WHERE 1=1
			-- AND ASSETSTATUS_ID IN (793046, 705020, 825789)
			AND DATEDIFF(MONTH,
					DATEADD(MINUTE, -ROUND(DOWNTIME * 60, 0), CONVERT(DATETIME2, CHANGEDATE, 103)),
					CONVERT(DATETIME2, CHANGEDATE, 103)) > 0
		;


		-- Step 2: Lặp để khấu trừ dần DOWNTIME ở mỗi ASSET_STATUS
		WHILE EXISTS(SELECT 1 FROM #TMP_1) BEGIN

			-- Step 2.1: Add vào bảng tạm các ASSET_STATUS có downtime bị dôi qua tháng mới. Cần chú ý vài field sau:
			-- 		DOWNTIME: Nếu current_CHANGEDATE - downtime bị lùi về 1 tháng thì set bằng DATEDIFF(<thời điểm bắt đầu tháng mới>, current_CHANGEDATE)
			-- 		IS_SPLIT: flag để xác định có bị split hay không
			-- 		N_SPLIT: split thứ mấy
			IF OBJECT_ID('tempdb..#TMP_FINAL') IS NOT NULL
				INSERT INTO #TMP_FINAL (
					ASSETSTATUS_ID
					, CHANGEDATE
					, DOWNTIME
					, IS_SPLIT
					, N_SPLIT
				) SELECT
					ASSETSTATUS_ID
					, CHANGEDATE
					, CASE WHEN DOWNTIME < DATEDIFF(MINUTE, 
								DATETIME2FROMPARTS(YEAR(CHANGEDATE), MONTH(CHANGEDATE), 1, 0, 0, 0, 0, 0),
								CHANGEDATE
							) THEN DOWNTIME
						ELSE DATEDIFF(MINUTE, 
								DATETIME2FROMPARTS(YEAR(CHANGEDATE), MONTH(CHANGEDATE), 1, 0, 0, 0, 0, 0),
								CHANGEDATE
							)		    
					END                             AS DOWNTIME
					, 1 							AS IS_SPLIT
					, @n_split						AS N_SPLIT
				FROM #TMP_1
			ELSE
				SELECT
					ASSETSTATUS_ID
					, CHANGEDATE
					, DATEDIFF(MINUTE, 
						DATETIME2FROMPARTS(YEAR(CHANGEDATE), MONTH(CHANGEDATE), 1, 0, 0, 0, 0, 0),
						CHANGEDATE
					)                               AS DOWNTIME
					, 1 							AS IS_SPLIT
					, @n_split						AS N_SPLIT
				INTO #TMP_FINAL
				FROM #TMP_1
			;

			-- Step 2.2: Cập nhật lại:
			-- 		CHANGEDATE: Set bằng thời điểm kết thúc tháng trước (ví dụ: 31/12/2022 23:59:59)
			-- 		DOWNTIME: set bằng downtime - DATEDIFF(<thời điểm bắt đầu tháng của cur_CHANGEDATE>, CHANGEDATE), 
			--				lưu ý là con số này có thể âm thời gian downtime còn lại ít hơn khoảng thời gian từ đầu tới cuối tháng của current_CHANGEDATE
			-- 		FLAG_DELETE: Nếu downtime bị âm thì sẽ set 'Y' để xoá dòng này khỏi bảng #TMP_1
			UPDATE #TMP_1 SET
				CHANGEDATE = DATEADD(SECOND, -1, DATETIME2FROMPARTS(YEAR(CHANGEDATE), MONTH(CHANGEDATE), 1, 0, 0, 0, 0, 0))
				, DOWNTIME = DOWNTIME - DATEDIFF(MINUTE, 
					DATETIME2FROMPARTS(YEAR(CHANGEDATE), MONTH(CHANGEDATE), 1, 0, 0, 0, 0, 0),
					CHANGEDATE
				)
				, FLAG_DELETE = CASE WHEN DOWNTIME <= DATEDIFF(MINUTE, 
											DATETIME2FROMPARTS(YEAR(CHANGEDATE), MONTH(CHANGEDATE), 1, 0, 0, 0, 0, 0),
											CHANGEDATE) THEN 'Y' ELSE 'N' END
			;

			-- Step 2.3: Xoá những dòng ASSET_STATUS mà downtime của nó không còn bị dôi ra nữa
			DELETE #TMP_1 WHERE FLAG_DELETE = 'Y';

			-- Step 2.4: Cập nhật lại biến đếm số SPLIT
			SET @n_split = @n_split + 1; 
		END;


		-- Step 3: INSERT toàn bộ bảng #TMP_FINAL union với asset_status không bị dôi
		INSERT INTO FND.W_CMMS_ASSET_STATUS_F (
			[ASSETSTATUS_ID]
			, [FROMDATE]
			, [TODATE]
			, [DOWNTIME]
				
			, [CODE]
			, [CODE_DESCRIPTION]
			, [IS_RUNNING]
			, [ASSET_NUM]
			, [ASSET_UID]
			, [LOCATION]

			, [WONUM]
			, [SPVB_ISSUE]
			, [SPVB_CA] 
			, [SPVB_PA] 
			, [REMARKS] 
			, [IS_SPLIT]
			, [N_SPLIT]

			, [W_INTEGRATION_ID]
			, [W_BATCH_ID]
			, [W_INSERT_DT]
			, [W_DELETE_FLG]
			, [W_DATASOURCE_NUM_ID]
			, [W_UPDATE_DT]
		)
		SELECT 
			F.[ASSETSTATUS_ID]
			, DATEADD(MINUTE, -F.[DOWNTIME], F.[CHANGEDATE])
			, F.[CHANGEDATE]
			, F.[DOWNTIME]
				
			, AS_ST.[CODE]
			, AS_ST.[CODE_DESCRIPTION]
			, AS_ST.[IS_RUNNING]
			, AS_ST.[ASSET_NUM]
			, AS_ST.[ASSET_UID]
			, AS_ST.[LOCATION]

			, AS_ST.[WONUM]
			, AS_ST.[SPVB_ISSUE]
			, AS_ST.[SPVB_CA] 
			, AS_ST.[SPVB_PA] 
			, AS_ST.[REMARKS] 
			, F.[IS_SPLIT]
			, F.[N_SPLIT]

			, CONCAT(F.[ASSETSTATUS_ID], '~', IS_SPLIT, '~', N_SPLIT)
			, @p_batch_id
			, DATEADD(HH, 7, GETDATE())
			, 'N'
			, 8
			, DATEADD(HH, 7, GETDATE())
		FROM #TMP_FINAL F
			LEFT JOIN [STG].[W_CMMS_ASSET_STATUS_FS] AS_ST ON 1=1
				AND AS_ST.ASSETSTATUS_ID = F.ASSETSTATUS_ID
		UNION ALL
		SELECT 
			-- TOP 10
			[ASSETSTATUS_ID]
			, DATEADD(MINUTE, -ROUND([DOWNTIME] * 60, 0), 
					CONVERT(DATETIME2, CHANGEDATE, 103))	AS [TODATE]
			, CONVERT(DATETIME2, CHANGEDATE, 103)			AS [FROMDATE]
			, ROUND([DOWNTIME] * 60, 0)						AS [DOWNTIME_ORG]

			, [CODE]
			, [CODE_DESCRIPTION]
			, [IS_RUNNING]
			, [ASSET_NUM]
			, [ASSET_UID]
			, [LOCATION]

			, [WONUM]
			, [SPVB_ISSUE]
			, [SPVB_CA] 
			, [SPVB_PA] 
			, [REMARKS] 
			, 0
			, NULL

			, CONCAT([ASSETSTATUS_ID], '~', 0, '~', NULL)
			, @p_batch_id
			, DATEADD(HH, 7, GETDATE())
			, 'N'
			, 8
			, DATEADD(HH, 7, GETDATE())
		FROM STG.W_CMMS_ASSET_STATUS_FS
		WHERE 1=1
			AND ASSETSTATUS_ID <> ''
			AND DOWNTIME <> 0
			AND DATEDIFF(MONTH,
					DATEADD(MINUTE, -ROUND(DOWNTIME * 60, 0), CONVERT(DATETIME2, CHANGEDATE, 103)),
					CONVERT(DATETIME2, CHANGEDATE, 103)
				) <= 0
		;


		/*delete & re-insert data refresh*/
		DELETE FROM [dbo].[SAP_ETL_DATAFRESH_CONF] WHERE UPPER(TABLE_NAME) = UPPER(@tgt_TableName)

		INSERT INTO [dbo].[SAP_ETL_DATAFRESH_CONF] (
			TABLE_NAME
			, REFRESH_DATE
			, IS_FULLLOAD
			, IS_EXIST_SSAS
			, LAST_UPDATE_DATE
		)
		SELECT DISTINCT @tgt_TableName, NULL, 'Y', 'Y', DATEADD(HH, 7, GETDATE())
		FROM
			( 
			SELECT * FROM W_PROGRAM_TARGET_RCS_F
			) M
		WHERE W_BATCH_ID = @p_batch_id
			AND W_DELETE_FLG = 'N'

		SET @src_rownum = (SELECT COUNT(1) FROM [STG].[W_CMMS_ASSET_STATUS_FS] WHERE W_BATCH_ID = @p_batch_id);
		SET @tgt_rownum = (SELECT COUNT(1) FROM FND.W_CMMS_ASSET_STATUS_F WHERE W_DELETE_FLG = 'N' AND  W_BATCH_ID = @p_batch_id);

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
