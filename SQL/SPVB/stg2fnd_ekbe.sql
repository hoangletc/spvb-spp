SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROC [dbo].[SAP_proc_load_stg_to_fnd_ekbe]
	@p_batch_id [bigint]
AS
BEGIN
	DECLARE 
	@p_tgt_table nvarchar(100),
    @v_job_id bigint,
    @v_job_instance_id bigint,
    @p_src_chk_column varchar(32),
    @p_tgt_chk_column varchar(32),
    @p_return_code bigint,
	@p_return_msg varchar(4000),
    @p_job_status varchar(100) = 'SUCCESS',
    @src_rownum	BIGINT,
    @tgt_rownum BIGINT,
    @tgt_chk_value float,
    @src_chk_value float,	
    @p_error_message varchar(4000)

	--   @v_start_dt datetime,
	--@v_end_dt datetime,
	--   @v_openning_dt datetime,
	--   @v_balance_each_month bigint

	SET @p_tgt_table = 'FND.W_SAP_EKBE_F'
	SET @v_job_id = (
		SELECT TOP 1
		JOB_ID
	FROM dbo.SAP_ETL_JOB
	WHERE TGT_TABLE = @p_tgt_table and ACTIVE_FLG='Y'
	)
	SET @v_job_instance_id = convert(bigint, convert(varchar,@p_batch_id)+convert(varchar,@v_job_id))
	PRINT 'Job ID: ' + convert(varchar, @v_job_id)


	execute	 [dbo].[SAP_proc_etl_util_start_job_instance]
										@p_tgt_table_name 	= @p_tgt_table,
										@p_batch_id 		= @p_batch_id,
										@p_job_instance_id  = @v_job_instance_id OUTPUT,
										@p_src_chk_column 	= @p_src_chk_column OUTPUT,
										@p_tgt_chk_column 	= @p_tgt_chk_column OUTPUT,
										@p_return_code 		= @p_return_code OUTPUT,
										@p_return_msg 		= @p_return_msg OUTPUT

	BEGIN TRY
        PRINT 'START stored procedure'

		/**create temp table**/
		IF object_id('tempdb..#W_SAP_EKBE_F_tmp') IS NOT NULL
			drop table #W_SAP_EKBE_F_tmp;
		
		select * 
			, 'N' AS W_UPDATE_FLG
			, CONCAT_WS('~',MANDT,EBELN,EBELP,ZEKKN,VGABE,GJAHR,BELNR,BUZEI) as W_INTEGRATION_ID
	into #W_SAP_EKBE_F_tmp
	from STG.W_SAP_EKBE_FS sc
	
		update #W_SAP_EKBE_F_tmp
		set W_UPDATE_FLG = 'Y'
		from #W_SAP_EKBE_F_tmp tg
		inner join FND.W_SAP_EKBE_F sc on CONCAT_WS('~',sc.MANDT,sc.EBELN,sc.EBELP,sc.ZEKKN,sc.VGABE,sc.GJAHR,sc.BELNR,sc.BUZEI) = tg.W_INTEGRATION_ID

		/**delete old data**/
		delete FND.W_SAP_EKBE_F
		from FND.W_SAP_EKBE_F tg
		inner join #W_SAP_EKBE_F_tmp sc on sc.W_INTEGRATION_ID = CONCAT_WS('~',tg.MANDT,tg.EBELN,tg.EBELP,tg.ZEKKN,tg.VGABE,tg.GJAHR,tg.BELNR,tg.BUZEI) 
		where sc.W_UPDATE_FLG='Y'

		/**Insert new data**/
		insert into FND.W_SAP_EKBE_F
	select
		[MANDT]
			, [EBELN]
			, [EBELP]
			, [ZEKKN]
			, [VGABE]
			, [GJAHR]
			, [BELNR]
			, [BUZEI]
			, [BEWTP]
			, [BWART]
			, [BUDAT]
			, [MENGE]
			, [BPMNG]
			, [DMBTR]
			, [WRBTR]
			, [WAERS]
			, [AREWR]
			, [WESBS]
			, [BPWES]
			, [SHKZG]
			, [BWTAR]
			, [ELIKZ]
			, [XBLNR]
			, [LFGJA]
			, [LFBNR]
			, [LFPOS]
			, [GRUND]
			, [CPUDT]
			, [CPUTM]
			, [REEWR]
			, [EVERE]
			, [REFWR]
			, [MATNR]
			, [WERKS]
			, [XWSBR]
			, [ETENS]
			, [KNUMV]
			, [MWSKZ]
			, [LSMNG]
			, [LSMEH]
			, [EMATN]
			, [AREWW]
			, [HSWAE]
			, [BAMNG]
			, [CHARG]
			, [BLDAT]
			, [XWOFF]
			, [XUNPL]
			, [ERNAM]
			, [SRVPOS]
			, [PACKNO]
			, [INTROW]
			, [BEKKN]
			, [LEMIN]
			, [AREWB]
			, [REWRB]
			, [SAPRL]
			, [MENGE_POP]
			, [BPMNG_POP]
			, [DMBTR_POP]
			, [WRBTR_POP]
			, [WESBB]
			, [BPWEB]
			, [WEORA]
			, [AREWR_POP]
			, [KUDIF]
			, [RETAMT_FC]
			, [RETAMT_LC]
			, [RETAMTP_FC]
			, [RETAMTP_LC]
			, [XMACC]
			, [WKURS]
			, [INV_ITEM_ORIGIN]
			, [VBELN_ST]
			, [VBELP_ST]
			, [SGT_SCAT]
			, [_DATAAGING]
			, [SESUOM]
			, [LOGSY]
			, [ET_UPD]
			, [/CWM/BAMNG]
			, [/CWM/WESBS]
			, [/CWM/TY2TQ]
			, [/CWM/WESBB]
			, [J_SC_DIE_COMP_F]
			, [FSH_SEASON_YEAR]
			, [FSH_SEASON]
			, [FSH_COLLECTION]
			, [FSH_THEME]
			, [QTY_DIFF]
			, [WRF_CHARSTC1]
			, [WRF_CHARSTC2]
			, [WRF_CHARSTC3]
			, dateadd(hh,7,getdate()) as W_INSERT_DT
			, @p_batch_id as W_BATCH_ID
	from #W_SAP_EKBE_F_tmp sc;

		SET @src_rownum = (SELECT COUNT(DISTINCT CONCAT_WS('~',MANDT,EBELN,EBELP,ZEKKN,VGABE,GJAHR,BELNR,BUZEI))
	FROM FND.W_SAP_EKBE_F
	WHERE W_BATCH_ID = @p_batch_id);
		SET @tgt_rownum = (SELECT COUNT(CONCAT_WS('~',MANDT,EBELN,EBELP,ZEKKN,VGABE,GJAHR,BELNR,BUZEI))
	FROM FND.W_SAP_EKBE_F
	WHERE W_BATCH_ID = @p_batch_id);

		/*delete & re-insert data refresh*/
		DELETE FROM [dbo].[SAP_ETL_DATAFRESH_CONF] WHERE UPPER(TABLE_NAME) = UPPER(@p_tgt_table)

		/*load SSAS for partition based on registration date or program start date in batch*/
		INSERT INTO [dbo].[SAP_ETL_DATAFRESH_CONF]
		(TABLE_NAME, REFRESH_DATE, IS_FULLLOAD, IS_EXIST_SSAS, LAST_UPDATE_DATE)
	SELECT DISTINCT @p_tgt_table, CAST(LEFT([BUDAT], 6)+'01' AS DATE), 'N', 'Y', DATEADD(HH, 7, GETDATE())
	FROM FND.W_SAP_EKBE_F
	WHERE W_BATCH_ID = @p_batch_id

        END TRY
        BEGIN CATCH
            set @p_job_status = 'FAILED'
            set @p_error_message = ERROR_MESSAGE()
            print @p_error_message
        END CATCH

	execute	[dbo].[SAP_proc_etl_util_end_job_instance]
                @p_job_instance_id 	= @v_job_instance_id,
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
