SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROC [dbo].[SAP_proc_load_w_sap_anla_d]
    @p_batch_id [bigint]
AS

BEGIN
    --DECLARE @p_batch_id bigint = 2022122101
    DECLARE	@tgt_TableName nvarchar(200) = N'dbo.W_SAP_ANLA_D',
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

    set @v_job_id= (select top 1
        JOB_ID
    from [dbo].[SAP_ETL_JOB]
    where 1=1 and ACTIVE_FLG='Y' and TGT_TABLE = @tgt_TableName)
    set @v_jobinstance_id = convert(bigint, convert(varchar,@v_batch_id)+convert(varchar,@v_job_id))
    set @v_src_tablename = (select top 1
        SRC_TABLE
    from [dbo].[SAP_ETL_JOB]
    where 1=1 and ACTIVE_FLG='Y' and TGT_TABLE = @tgt_TableName /*+'T' */)

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
    PRINT 'TEST1'
    BEGIN TRY
	        /* check template table*/
        IF OBJECT_ID(N'tempdb..#W_SAP_ANLA_D_tmp') IS NOT NULL 
			BEGIN
        PRINT N'DELETE temporary table #W_SAP_ANLA_D_tmp'
        DROP Table #W_SAP_ANLA_D_tmp
    END;
        --PRINT 'END stored procedure'
		PRINT 'TEST2'
	select CONVERT(NVARCHAR(1000),[MANDT]) AS [CLIENT_CODE]
			  , CONVERT(NVARCHAR(1000),[BUKRS]) AS [COMPANY_CODE]
			  , CONVERT(NVARCHAR(1000),[ANLN1]) AS [MAIN_ASSET_NUMBER]
			  , CONVERT(NVARCHAR(1000),[ANLN2]) AS [ASSET_SUBNUMBER]
			  , CONVERT(NVARCHAR(1000),[ANLKL]) AS [ASSET_CLASS]
			  , 'N' AS W_UPDATE_FLG
			  , 'N' AS W_DELETE_FLG
			  , 1 AS W_DATASOURCE_NUM_ID
			  , CONVERT(NVARCHAR(1000),CONCAT_WS('~',BUKRS,ANLN1,ANLN2,ANLKL)) AS  W_INTEGRATION_ID
			  , GETDATE() AS W_INSERT_DT
			  , GETDATE() AS W_UPDATE_DT
			  , @p_batch_id W_BATCH_ID
    INTO #W_SAP_ANLA_D_tmp
    FROM [FND].[W_SAP_ANLA_D_TEMP]
		
		PRINT'TEST3'

		update #W_SAP_ANLA_D_tmp
		set W_UPDATE_FLG = 'Y'
		from #W_SAP_ANLA_D_tmp tg
        inner join dbo.W_SAP_ANLA_D sc on sc.W_INTEGRATION_ID = tg.W_INTEGRATION_ID
		PRINT'TEST4'
	        /* Delete old data */
		/* CLIENT_CODE + '~' + COMPANY_CODE +'~' + CUSTOMER_CODE+'~'+ SPECIAL_GL_TRANSACTION_TYPE + '~'+  SPECIAL_GL_INDICATOR + '~' + CLEARING_DOC + '~' + CONVERT(VARCHAR, CLEARING_DT)+ '~' + ASSIGMENT +'~'+DOC_NUM+'~'+LINE_NUM */
        PRINT 'Update data'
		update [dbo].[W_SAP_ANLA_D]
			SET [MANDT] = Src.[CLIENT_CODE]
			,[BUKRS] = Src.[COMPANY_CODE]
			,[ANLN1] = Src.[MAIN_ASSET_NUMBER]
			,[ANLN2] = Src.[ASSET_SUBNUMBER]
			,[ANLKL] = Src.[ASSET_CLASS]
			, W_UPDATE_DT = GETDATE()
        FROM [dbo].[W_SAP_ANLA_D] tgt
        INNER JOIN #W_SAP_ANLA_D_tmp src ON src.W_INTEGRATION_ID = tgt.W_INTEGRATION_ID

	    /* Insert data */
        PRINT 'INSERT new DATA'
        INSERT INTO [dbo].[W_SAP_ANLA_D]
        ( [MANDT]
        ,[BUKRS]
        ,[ANLN1]
        ,[ANLN2]
        ,[ANLKL]
        , W_DELETE_FLG
        , W_DATASOURCE_NUM_ID
        , W_INTEGRATION_ID
        , W_INSERT_DT
        , W_UPDATE_DT
        , W_BATCH_ID

        )
    SELECT
        [CLIENT_CODE]
			, [COMPANY_CODE]
			, [MAIN_ASSET_NUMBER]
			, [ASSET_SUBNUMBER]
			, [ASSET_CLASS]
			, W_DELETE_FLG
			, W_DATASOURCE_NUM_ID
			, W_INTEGRATION_ID
			, W_INSERT_DT
			, W_UPDATE_DT
			, W_BATCH_ID
    FROM #W_SAP_ANLA_D_tmp
    WHERE W_UPDATE_FLG = 'N'

		/*delete & re-insert data refresh*/
		DELETE FROM [dbo].[SAP_ETL_DATAFRESH_CONF] WHERE UPPER(TABLE_NAME) = UPPER(@tgt_TableName)

		INSERT INTO [dbo].[SAP_ETL_DATAFRESH_CONF]
        (TABLE_NAME, REFRESH_DATE, IS_FULLLOAD, IS_EXIST_SSAS, LAST_UPDATE_DATE)
    SELECT DISTINCT @tgt_TableName, NULL, 'Y', 'Y', DATEADD(HH, 7, GETDATE())
    FROM
        ( 
			SELECT *
        FROM W_SAP_ANLA_D
			) M
    WHERE W_BATCH_ID = @p_batch_id
        AND W_DELETE_FLG = 'N'

		SET @src_rownum = (SELECT COUNT(1)
    FROM #W_SAP_ANLA_D_tmp );
		SET @tgt_rownum = (SELECT COUNT(DISTINCT W_INTEGRATION_ID)
    FROM dbo.W_SAP_ANLA_D
    WHERE W_DELETE_FLG = 'N' AND W_BATCH_ID = @p_batch_id);

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
