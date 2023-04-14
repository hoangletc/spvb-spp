SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

ALTER PROC [dbo].[CMMS_proc_load_fnd_w_spp_asset_status_f] @p_batch_id [bigint] AS
BEGIN
	DECLARE	@tgt_TableName nvarchar(200) = 'FND.W_CMMS_ASSET_STATUS_F',
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
		--NOTE [HoangLe - Mar17]: Do ASSET_STATUS (DOWNTIME) bị tách ra khi downtime dôi qua tháng mới, 
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
		IF OBJECT_ID(N'tempdb..#TMP_STATUS') IS NOT NULL BEGIN
			PRINT N'DELETE temporary table #TMP_STATUS'
			DROP Table #TMP_STATUS
		END;
		IF OBJECT_ID(N'tempdb..#TMP_PRE_DOWNTIME') IS NOT NULL BEGIN
			PRINT N'DELETE temporary table #TMP_PRE_DOWNTIME'
			DROP Table #TMP_PRE_DOWNTIME
		END;
		IF OBJECT_ID(N'tempdb..#TMP_RELATED_DOWNTIME') IS NOT NULL BEGIN
			PRINT N'DELETE temporary table #TMP_RELATED_DOWNTIME'
			DROP Table #TMP_RELATED_DOWNTIME
		END;

		IF OBJECT_ID(N'tempdb..#W_CMMS_AS_ST_F_tmp') IS NOT NULL 
        BEGIN
            PRINT N'DELETE temporary table #W_CMMS_AS_ST_F_tmp'
            DROP Table #W_CMMS_AS_ST_F_tmp
        END;

		-- Step 1: Chọn ra tất cả các ASSET_STATUS có downtime bị dôi qua tháng hôm sau
		WITH A AS (
			SELECT  
				CODE
				, ASSET_NUM
				, ASSETSTATUS_ID
				, SPVB_RELATEDDOWNTIME
			FROM STG.W_CMMS_ASSET_STATUS_FS F
			WHERE 1=1
				-- AND F.ASSET_NUM = '120280620000'
				-- AND F.CHANGEDATE LIKE '%11/2022%'
				-- AND CODE LIKE 'A1'
				AND CHANGEDATE <> ''
				AND IS_RUNNING = 0
		)
			SELECT
				CONVERT(BIGINT, A.ASSETSTATUS_ID)               AS ASSETSTATUS_ID
				, CONVERT(DATETIME2, CHANGEDATE, 103)		    AS CHANGEDATE
				, CONVERT(FLOAT, DOWNTIME) * 3600.0   			AS DOWNTIME
				, CONVERT(BIGINT, A.SPVB_RELATEDDOWNTIME)       AS SPVB_RELATEDDOWNTIME
				, 'N' 										    AS FLAG_DELETE
			INTO #TMP_STATUS
			FROM STG.W_CMMS_ASSET_STATUS_FS F
			JOIN A ON 1=1
				AND CONVERT(BIGINT, A.ASSETSTATUS_ID) + 1 = CONVERT(BIGINT, F.ASSETSTATUS_ID)
		;

		SELECT * INTO #TMP_1 FROM #TMP_STATUS
		WHERE 1=1
			-- AND ASSETSTATUS_ID IN (972831)
			AND DATEDIFF(MONTH, DATEADD(SECOND, -DOWNTIME, CHANGEDATE), CHANGEDATE) > 0.0
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
					, SPVB_RELATEDDOWNTIME
					, IS_SPLIT
					, N_SPLIT
				) SELECT
					ASSETSTATUS_ID
					, CHANGEDATE
					, CASE WHEN DOWNTIME < DATEDIFF(SECOND, 
								DATETIME2FROMPARTS(YEAR(CHANGEDATE), MONTH(CHANGEDATE), 1, 0, 0, 0, 0, 0),
								CHANGEDATE
							) THEN DOWNTIME / 60.0
						ELSE DATEDIFF(SECOND, 
								DATETIME2FROMPARTS(YEAR(CHANGEDATE), MONTH(CHANGEDATE), 1, 0, 0, 0, 0, 0),
								CHANGEDATE
							) / 60.0
					END                             AS DOWNTIME
					, SPVB_RELATEDDOWNTIME
					, 1 							AS IS_SPLIT
					, @n_split						AS N_SPLIT
				FROM #TMP_1
			ELSE
				SELECT
					ASSETSTATUS_ID
					, CHANGEDATE
					, DATEDIFF(SECOND, 
						DATETIME2FROMPARTS(YEAR(CHANGEDATE), MONTH(CHANGEDATE), 1, 0, 0, 0, 0, 0),
						CHANGEDATE
					) / 60.0                        AS DOWNTIME
					, SPVB_RELATEDDOWNTIME
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
				, DOWNTIME = DOWNTIME - 1.0 * DATEDIFF(SECOND, 
					DATETIME2FROMPARTS(YEAR(CHANGEDATE), MONTH(CHANGEDATE), 1, 0, 0, 0, 0, 0),
					CHANGEDATE
				)
				, FLAG_DELETE = CASE WHEN DOWNTIME <= DATEDIFF(SECOND, 
											DATETIME2FROMPARTS(YEAR(CHANGEDATE), MONTH(CHANGEDATE), 1, 0, 0, 0, 0, 0),
											CHANGEDATE) THEN 'Y' ELSE 'N' END
			;

			-- Step 2.3: Xoá những dòng ASSET_STATUS mà downtime của nó không còn bị dôi ra nữa
			DELETE #TMP_1 WHERE FLAG_DELETE = 'Y';

			-- Step 2.4: Cập nhật lại biến đếm số SPLIT
			SET @n_split = @n_split + 1; 
		END;


		-- Step 3: INSERT toàn bộ bảng #TMP_FINAL union với asset_status không bị dôi
		SELECT * INTO #TMP_PRE_DOWNTIME FROM (
			SELECT 
				[ASSETSTATUS_ID]
				, DATEADD(SECOND, -[DOWNTIME] * 60.0, [CHANGEDATE]) AS [FROMDATE]
				, [CHANGEDATE]                                      AS [TODATE]
				, [DOWNTIME]
				, [SPVB_RELATEDDOWNTIME]
					
				, [IS_SPLIT]
				, [N_SPLIT]
			FROM #TMP_FINAL
				
			UNION ALL
			SELECT 
				[ASSETSTATUS_ID]
				, DATEADD(SECOND, -[DOWNTIME], 
						CONVERT(DATETIME2, CHANGEDATE, 103))	    AS [FROMDATE]
				, CONVERT(DATETIME2, CHANGEDATE, 103)			    AS [TODATE]
				, [DOWNTIME] / 60.0
				, [SPVB_RELATEDDOWNTIME]

				, 0
				, NULL

			FROM #TMP_STATUS
			WHERE 1=1
				AND DATEDIFF(MONTH, 
						DATEADD(SECOND, -DOWNTIME, CHANGEDATE), 
						CHANGEDATE) <= 0
		) AAA
		;


		SELECT
			CONVERT(BIGINT, F.ASSETSTATUS_ID)           AS ASSETSTATUS_ID 
			, CONVERT(BIGINT, F.SPVB_RELATEDDOWNTIME)   AS SPVB_RELATEDDOWNTIME
			, CONVERT(NVARCHAR(30), F.ASSET_NUM)        AS ASSET_NUM
			, CONVERT(NVARCHAR(1000), T.[DESCRIPTION])  AS [DESCRIPTION]
		INTO #TMP_RELATED_DOWNTIME
		FROM STG.W_CMMS_ASSET_STATUS_FS F
		JOIN dbo.W_CMMS_ASSET_D T ON 1=1
			AND T.ASSET_UID = F.ASSET_UID
		;



		-- INSERT INTO FND.W_CMMS_ASSET_STATUS_F (
			--   [ASSETSTATUS_ID]
			-- , [FROMDATE]
			-- , [TODATE]
			-- , [DOWNTIME]
				
			-- , [CODE]
			-- , [CODE_DESCRIPTION]
			-- , [IS_RUNNING]
			-- , [ASSET_NUM]
			-- , [ASSET_UID]
			-- , [LOCATION]

			-- , [WONUM]
			-- , [SPVB_ISSUE]
			-- , [SPVB_CA] 
			-- , [SPVB_PA] 
			-- , [REMARKS] 
			-- , [IS_SPLIT]
			-- , [N_SPLIT]
			-- , [FILLER]
			-- , [FILLER_DOWNTIME]

			-- , [W_INTEGRATION_ID]
			-- , [W_BATCH_ID]
			-- , [W_INSERT_DT]
			-- , [W_DELETE_FLG]
			-- , [W_DATASOURCE_NUM_ID]
			-- , [W_UPDATE_DT]
		-- )
		SELECT
			  CONVERT(BIGINT, F.ASSETSTATUS_ID)  									AS [ASSETSTATUS_ID]
			, CONVERT(DATETIME2, F.[FROMDATE])										AS [FROMDATE]
			, CONVERT(DATETIME2, F.[TODATE])										AS [TODATE]
			, CONVERT(FLOAT, F.[DOWNTIME])											AS [DOWNTIME]

			, CONVERT(NVARCHAR(100), A.[CODE])										AS [CODE]
			, CONVERT(NVARCHAR(100), A.[CODE_DESCRIPTION])  						AS [CODE_DESCRIPTION]
			, CONVERT(INT, A.[IS_RUNNING])											AS [IS_RUNNING]
			, CONVERT(NVARCHAR(20), A.[ASSET_NUM])									AS [ASSET_NUM]
			, CONVERT(BIGINT, A.[ASSET_UID])										AS [ASSET_UID]
			, CONVERT(NVARCHAR(20), A.[LOCATION])									AS [LOCATION]

			, CONVERT(NVARCHAR(100), A.[WONUM])										AS [WONUM]
			, CONVERT(NVARCHAR(1000), A.[SPVB_ISSUE])								AS [SPVB_ISSUE]
			, CONVERT(NVARCHAR(200), A.[SPVB_CA])									AS [SPVB_CA] 
			, CONVERT(NVARCHAR(100), A.[SPVB_PA])									AS [SPVB_PA] 
			, CONVERT(NVARCHAR(100), A.[REMARKS])									AS [REMARKS] 
			, CONVERT(INT, F.[IS_SPLIT])											AS [IS_SPLIT]
			, CONVERT(INT, F.[N_SPLIT])												AS [N_SPLIT]

			, CONVERT(NVARCHAR(500), R.ASSET_NUM + ' - ' + R.DESCRIPTION) 			AS FILLER
			, CONVERT(FLOAT, D.DOWNTIME)                          					AS FILLER_DOWNTIME

			, CONCAT(F.[ASSETSTATUS_ID], '~', F.[IS_SPLIT], '~', F.[N_SPLIT])  		AS [W_INTEGRATION_ID]
			, CONVERT(INT, @p_batch_id)  											AS [W_BATCH_ID]
			, CONVERT(DATETIME2, DATEADD(HH, 7, GETDATE()))  						AS [W_INSERT_DT]
			, 'N'  																	AS [W_DELETE_FLG]
			, 8  																	AS [W_DATASOURCE_NUM_ID]
			, CONVERT(DATETIME2, DATEADD(HH, 7, GETDATE()))  						AS [W_UPDATE_DT]
			, 'N'																	AS [W_UPDATE_FLG]
		INTO #W_CMMS_AS_ST_F_tmp
		FROM #TMP_PRE_DOWNTIME F
			LEFT JOIN [STG].[W_CMMS_ASSET_STATUS_FS] A ON 1=1
				AND A.ASSETSTATUS_ID = F.ASSETSTATUS_ID
			LEFT JOIN #TMP_RELATED_DOWNTIME R ON 1=1
				AND R.SPVB_RELATEDDOWNTIME = F.ASSETSTATUS_ID
			LEFT JOIN #TMP_PRE_DOWNTIME D ON 1=1
				AND R.ASSETSTATUS_ID = D.ASSETSTATUS_ID
				AND F.FROMDATE = D.FROMDATE
		WHERE 1=1
			AND (
				F.SPVB_RELATEDDOWNTIME = 0
				OR F.SPVB_RELATEDDOWNTIME IS NULL
			)
		;


		-- 3. Update main table using W_INTEGRATION_ID
		PRINT '3. Update main table using W_INTEGRATION_ID'

		-- 3.1. Mark existing records by flag 'Y'
		PRINT '3.1. Mark existing records by flag ''Y'''

		UPDATE #W_CMMS_AS_ST_F_tmp
		SET W_UPDATE_FLG = 'Y'
		FROM #W_CMMS_AS_ST_F_tmp tg
		INNER JOIN FND.W_CMMS_ASSET_STATUS_F sc 
		ON sc.W_INTEGRATION_ID = tg.W_INTEGRATION_ID

		-- 3.2. Start updating
		PRINT '3.2. Start updating'

		UPDATE FND.W_CMMS_ASSET_STATUS_F
		SET 
			[ASSETSTATUS_ID] = src.[ASSETSTATUS_ID]
			, [FROMDATE] = src.[FROMDATE]
			, [TODATE] = src.[TODATE]
			, [DOWNTIME] = src.[DOWNTIME]

			, [CODE] = src.[CODE]
			, [CODE_DESCRIPTION] = src.[CODE_DESCRIPTION]
			, [IS_RUNNING] = src.[IS_RUNNING]
			, [ASSET_NUM] = src.[ASSET_NUM]
			, [ASSET_UID] = src.[ASSET_UID]
			, [LOCATION] = src.[LOCATION]

			, [WONUM] = src.[WONUM]
			, [SPVB_ISSUE] = src.[SPVB_ISSUE]
			, [SPVB_CA]  = src.[SPVB_CA] 
			, [SPVB_PA]  = src.[SPVB_PA] 
			, [REMARKS]  = src.[REMARKS] 
			, [IS_SPLIT] = src.[IS_SPLIT]
			, [N_SPLIT] = src.[N_SPLIT]
			, [FILLER] = src.[FILLER]
			, [FILLER_DOWNTIME] = src.[FILLER_DOWNTIME]

 			, [W_INTEGRATION_ID] = src.[W_INTEGRATION_ID]
			, [W_BATCH_ID] = src.[W_BATCH_ID]
			, [W_INSERT_DT] = src.[W_INSERT_DT]
			, [W_DELETE_FLG] = src.[W_DELETE_FLG]
			, [W_DATASOURCE_NUM_ID] = src.[W_DATASOURCE_NUM_ID]
			, [W_UPDATE_DT]  = src.[W_UPDATE_DT] 
		FROM FND.W_CMMS_ASSET_STATUS_F tgt
		INNER JOIN #W_CMMS_AS_ST_F_tmp src ON src.W_INTEGRATION_ID = tgt.W_INTEGRATION_ID


		-- 4. Insert non-existed records to main table from temp table
		PRINT '4. Insert non-existed records to main table from temp table'

		INSERT INTO FND.W_CMMS_ASSET_STATUS_F(
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
			, [FILLER]
			, [FILLER_DOWNTIME]

			, [W_INTEGRATION_ID]
			, [W_BATCH_ID]
			, [W_INSERT_DT]
			, [W_DELETE_FLG]
			, [W_DATASOURCE_NUM_ID]
			, [W_UPDATE_DT]
		)
		SELECT
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
			, [FILLER]
			, [FILLER_DOWNTIME]

			, [W_INTEGRATION_ID]
			, [W_BATCH_ID]
			, [W_INSERT_DT]
			, [W_DELETE_FLG]
			, [W_DATASOURCE_NUM_ID]
			, [W_UPDATE_DT]
		FROM #W_CMMS_AS_ST_F_tmp
		where W_UPDATE_FLG = 'N'


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
