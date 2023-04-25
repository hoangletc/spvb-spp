--------------------------------------------------
with MB51 AS (
	SELECT 
		RIGHT(F.MATNR, 8) 											AS MATNR
		, F.LGORT
		, F.WERKS
		, CONCAT(RIGHT(F.MATNR, 8), F.WERKS, F.LGORT) 				AS sap_code_mix
		, SUM(CASE WHEN ISNULL(MV.[Group], 0) = 'Gr' 
			THEN CONVERT(DECIMAL(38, 20), F.DMBTR_STOCK) * 100 
			else 0 end
		) 															AS GR_AMT
		, SUM(CASE WHEN ISNULL(MV.[Group], 0) = 'GR from Cost Center'
			THEN CONVERT(DECIMAL(38, 20), F.DMBTR_STOCK) * 100
			else 0 end
		) 															AS GI_AMT
		, SUM(CASE WHEN ISNULL(MV.[Group], 0) = 'GR' 
			THEN CONVERT(DECIMAL(38, 20), F.STOCK_QTY)
			ELSE 0 END
		) 															AS GR
		, SUM(CASE WHEN ISNULL(MV.[Group],0) = 'GI'
			THEN CONVERT(DECIMAL(38, 20), F.STOCK_QTY)
			ELSE 0 END
		) 															AS GI
		, SUM(CASE WHEN ISNULL(MV.[Group], 0) IN ('Transfer out plant', 'Transfer in plant')
			THEN CONVERT(DECIMAL(38, 20), F.STOCK_QTY)
			ELSE 0 END
		) 															AS Transfer
		, SUM(CASE WHEN ISNULL(MV.[Group], 0) = 'Other' 
			then CONVERT(DECIMAL(38, 20), F.STOCK_QTY)
			ELSE 0 END
		)															AS Other
	
	FROM fnd.w_sap_matdoc_f_TEMP F
	LEFT JOIN [FND].[W_SAP_T156T_D] T ON 1=1
		AND ISNULL(T.SOBKZ,'') = ISNULL(F.SOBKZ,'')
		AND ISNULL(T.KZBEW,'') = ISNULL(F.KZBEW,'')
		AND ISNULL(T.KZZUG,'') = ISNULL(F.KZZUG,'') 
		AND ISNULL(T.KZVBR,'') = ISNULL(F.KZVBR,'')
		AND ISNULL(T.BWART,'') = ISNULL(F.BWART,'')
	LEFT JOIN [STG].[W_EXCEL_SPP_MOVEMENT_TYPE_DS] MV ON 1=1
		AND MV.[Movement Type Name] = T.BTEXT 
	WHERE 1=1
	 	AND BUDAT >= '20230201' AND BUDAT < '20230301'
		AND RIGHT(F.MATNR, 8) like '6%'
		AND RECORD_TYPE <> 'MDOC_CP'

	GROUP BY
		RIGHT(F.MATNR, 8)
		, F.LGORT
		, F.WERKS
		, CONCAT(
			RIGHT(F.MATNR, 8)
			, F.WERKS
			, F.LGORT
		)
)
, OB AS (
	SELECT
		concat(Material,Plant, [Storage location]) 					AS sap_code_mix
		, Material
		, [Valuation Type]
		, Plant
		, [Storage location]

		, CONVERT(DECIMAL(38,20), CB)								AS CB
		, CONVERT(DECIMAL(38,20), Amount) 							AS Amount
		, PERIOD
	FROM FND.W_EXCEL_LM_BALANCE_F
	WHERE 1=1
		AND PERIOD = 202302

		-- and [MATERIAL] = '61174059'
		-- AND [PLANT] = '1020'
		-- AND [Storage location] = 'SP01'

)
, J3RF AS (
	select
		concat(MATERIAL, PLANT, STORAGE_LOCATION) 					AS sap_code_mix
		, MATERIAL
		, PLANT
		, STORAGE_LOCATION
		, STOCK_QUANTITY_ON_PERIOD_START
		, VALUE_ON_PERIOD_START
		, STOCK_QUANTITY_ON_PERIOD_END
		, STOCK_VALUE_ON_PERIOD_END
		, convert(deciMal(38,20), DEBIT_REVALUATION)
			+ convert(decimal(38,20), CREDIT_REVALUATION) 			AS [NET_REVALUATION]
	FROM [FND].[W_EXCEL_SPP_J3RFLVMOBVEDH_F] F
	WHERE 1=1
		AND PERIOD = 202302
)
, REVAL AS (
	select
		concat(MATERIAL, PLANT, 'SP01') 							AS sap_code_mix
		, MATERIAL
		, PLANT
		, STORAGE_LOCATION
		, convert(deciMal(38,20), DEBIT_REVALUATION)
			+ convert(decimal(38,20), CREDIT_REVALUATION) 			AS [NET_REVALUATION]
	FROM [FND].[W_EXCEL_SPP_J3RFLVMOBVEDH_F]
	WHERE 1=1
		AND STORAGE_LOCATION = 'OV01'
		AND CONVERT(DECIMAL(38,20), STOCK_QUANTITY_ON_PERIOD_END) = 0
		AND PERIOD = 202302
)
, MB51_RECAL as (
	SELECT
		MB51.MATNR
		, WERKS
		, CASE WHEN ISNULL(OB.CB,0) > 0 THEN LGORT 
			WHEN LGORT = 'OV01' AND ISNULL(J3RF.STOCK_QUANTITY_ON_PERIOD_END, 0) <= 0 THEN 'SP01'
			ELSE LGORT 
		END 														AS LGORT

		, SUM(GR_AMT) 												AS GR_AMT
		, SUM(GI_AMT) 												AS GI_AMT
		, SUM(GR) 													AS GR
		, SUM(GI) 													AS GI
		, SUM(Transfer) 											AS Transfer
		, SUM(Other) 												AS Other
		, concat(
			MB51.MATNR
			, WERKS
			, CASE WHEN ISNULL(OB.CB,0) > 0 THEN LGORT
				WHEN LGORT = 'OV01' AND ISNULL(J3RF.STOCK_QUANTITY_ON_PERIOD_END,0) <= 0 THEN 'SP01'
				ELSE LGORT
			END
		) 															AS sap_code_mix

        -- , OB.CB AS OB_CB
        -- , MB51.sap_code_mix AS MB51_SAP_CODE_MIX
        -- , GR_AMT
        -- , GI_AMT
        -- , GR
        -- , GI
        -- , Transfer
        -- , Other
	FROM MB51 
	LEFT JOIN OB ON 1=1
		AND OB.sap_code_mix = MB51.sap_code_mix
	LEFT JOIN J3RF ON 1=1
		AND MB51.sap_code_mix = J3RF.sap_code_mix

	-- WHERE 1=1
    --     AND MB51.MATNR = '61224499'

	GROUP BY 
		MB51.MATNR
		, WERKS
		, CASE WHEN ISNULL(OB.CB,0) > 0 THEN LGORT
			WHEN LGORT = 'OV01' AND ISNULL(J3RF.STOCK_QUANTITY_ON_PERIOD_END,0) <= 0 THEN 'SP01'
			ELSE LGORT
		END
		, concat(
			MB51.MATNR
			, WERKS
			, CASE WHEN ISNULL(OB.CB,0) > 0 THEN LGORT
				WHEN LGORT = 'OV01' AND ISNULL(J3RF.STOCK_QUANTITY_ON_PERIOD_END,0) <= 0 THEN 'SP01'
				ELSE LGORT END
			)
)
, J3RF_RECAL_QTY AS (
	select
		j.*
		, CONCAT(J.MATERIAL, J.PLANT, 'SP01') 						AS Sap_code_mix2
		, isnull(GR,0) 												AS GR
		, isnull(GI,0) 												AS GI


        -- M.sap_code_mix
        -- , j.STORAGE_LOCATION AS STORAGE_LOCATION
        -- , M.Transfer AS OLD_TRANSFER
        -- , STOCK_QUANTITY_ON_PERIOD_END AS OLD_STOCK_QUANTITY_ON_PERIOD_END

		, case when j.STORAGE_LOCATION is null
				and STOCK_QUANTITY_ON_PERIOD_END = 0
			then 0
			else isnull(Transfer,0)
		end 														AS Transfer
		, isnull(Other,0) 											AS Other
		, isnull(GR,0)
			+ isnull(GI,0) 
			+ case when j.STORAGE_LOCATION is null
					and STOCK_QUANTITY_ON_PERIOD_END = 0
				then 0 
				else isnull(Transfer, 0)
			end 
			+ isnull(Other,0)
			+ STOCK_QUANTITY_ON_PERIOD_START						AS CB
		, OB.AMOUNT 												AS Amount
		, isnull(GR_AMT,0) 											AS GR_AMT
		, isnull(GI_AMT,0) 											AS GI_AMT
	from J3RF j
	left join MB51_RECAL m ON 1=1
		AND M.sap_code_mix = J.sap_code_mix
	left join OB ON 1=1
		AND OB.sap_code_mix = J.sap_code_mix

	-- WHERE 1=1
	-- 	AND J.MATERIAL = '61224499'

	--GROUP BY J.sap_code_mix,J.MATERIAL, J.PLANT, J.STORAGE_LOCATION, STOCK_QUANTITY_ON_PERIOD_START, VALUE_ON_PERIOD_START, 
	--	STOCK_QUANTITY_ON_PERIOD_END, STOCK_VALUE_ON_PERIOD_END, [NET_REVALUATION]

)
, TRANSFER_OUT AS (
	SELECT
		MV.[Group]
		, RIGHT(F.MATNR, 8)											AS MATNR
		, F.LGORT
		, F.STOCK_QTY
		, F.WERKS
		, UMWRK_CID
		, LGORT_CID
		, VBELN_IM
		, VBELP_IM
		, row_number() over (
			partition by
				RIGHT(F.MATNR, 8)
				, VBELN_IM
				, VBELP_IM
			order by convert(decimal(38,20), F.STOCK_QTY) asc
		) 															as row_num
	FROM fnd.w_sap_matdoc_f_TEMP  F
		LEFT JOIN [FND].[W_SAP_T156T_D] T ON 1=1
			AND ISNULL(T.SOBKZ, '') = ISNULL(F.SOBKZ, '')
			AND ISNULL(T.KZBEW, '') = ISNULL(F.KZBEW, '')
			AND ISNULL(T.KZZUG, '') = ISNULL(F.KZZUG, '') 
			AND ISNULL(T.KZVBR, '') = ISNULL(F.KZVBR, '')
			and ISNULL(T.BWART, '') = ISNULL(F.BWART, '')
		LEFT JOIN [STG].[W_EXCEL_SPP_MOVEMENT_TYPE_DS] MV ON 1=1
			AND MV.[Movement Type Name] = T.BTEXT
	WHERE 1=1
	 	AND BUDAT >= '20230201' AND BUDAT < '20230301'
		AND ISNULL(MV.[Group], '') IN ('Transfer out plant')
		AND RECORD_TYPE <> 'MDOC_CP'
		AND LGORT <> '' 
)
, TRANS_DOC AS (
	select
		CONCAT(T1.MATNR, T1.UMWRK_CID,
				ISNULL(T2.LGORT, 'SP01')) 							AS receipt_sap_code_mix
		, T1.MATNR
		, T1.LGORT
		, T1.WERKS
		, T1.UMWRK_CID
		, ISNULL(T2.LGORT, 'SP01') 									AS LGORT_CID
	from TRANSFER_OUT t1
	left join TRANSFER_OUT t2 on 1=1
		AND t1.VBELN_IM = t2.VBELN_IM
		and t1.VBELP_IM = T2.VBELP_IM
		AND T2.row_num = 2
	where 1=1
		AND T1.row_num = 1

	UNION ALL

	SELECT
		CONCAT(RIGHT(F.MATNR, 8), UMWRK_CID, LGORT_CID) 			AS receipt_sap_code_mix
		, RIGHT(F.MATNR, 8) 										AS MATNR
		, F.LGORT
		, F.WERKS
		, UMWRK_CID
		, LGORT_CID
	FROM FND.W_SAP_MATDOC_F_TEMP  F
	LEFT JOIN [FND].[W_SAP_T156T_D] T ON 1=1
		AND ISNULL(T.SOBKZ, '') = ISNULL(F.SOBKZ, '')
		AND ISNULL(T.KZBEW, '') = ISNULL(F.KZBEW, '')
		AND ISNULL(T.KZZUG, '') = ISNULL(F.KZZUG, '') 
		AND ISNULL(T.KZVBR, '') = ISNULL(F.KZVBR, '')
		and ISNULL(T.BWART, '') = ISNULL(F.BWART, '')
	LEFT JOIN [STG].[W_EXCEL_SPP_MOVEMENT_TYPE_DS] MV ON 1=1
		AND MV.[Movement Type Name] = T.BTEXT 
	WHERE 1=1
		--AND RIGHT(MATNR,8) = '61025434' --AND WERKS IN ('1020')  
		AND BUDAT >= '20230201' AND BUDAT < '20230301'
		AND ISNULL(MV.[Group], '') IN ('Transfer in plant')
		AND RECORD_TYPE <> 'MDOC_CP'
		AND CONVERT(DECIMAL(38,20), STOCK_QTY) < 0
	GROUP BY MV.[Group], RIGHT(F.MATNR, 8), F.LGORT, F.WERKS, UMWRK_CID, LGORT_CID
)
, TRANSFER AS (
	SELECT
		J.sap_code_mix
		, J.MATERIAL
		, J.PLANT
		, J.STORAGE_LOCATION
		, J.STOCK_QUANTITY_ON_PERIOD_START
		, J.VALUE_ON_PERIOD_START
		, J.STOCK_QUANTITY_ON_PERIOD_END
		, J.STOCK_VALUE_ON_PERIOD_END
		, J.NET_REVALUATION
		, J.Sap_code_mix2
		, J.GR
		, J.GI
		, J.Transfer
		, J.Other
		, J.CB
		, J.Amount
		, J.GI_AMT
		, CASE WHEN ISNULL(J.STORAGE_LOCATION, '') = 'OV01' AND CB = 0
				THEN ISNULL(GR_AMT, 0)
			ELSE ISNULL(GR_AMT, 0) + J.[NET_REVALUATION]
					+ ISNULL(R.[NET_REVALUATION], 0)
		END 														AS GR_AMT
		, CASE WHEN J.Transfer <= 0 
			THEN (
				ISNULL(J.Amount, 0)
				+ CASE WHEN J.STORAGE_LOCATION = 'OV01' and CB = 0
					THEN GR_AMT
					ELSE GR_AMT + J.[NET_REVALUATION] + ISNULL(R.[NET_REVALUATION], 0)
				END)
				/ (CASE WHEN (STOCK_QUANTITY_ON_PERIOD_START + GR) = 0
					THEN NULL
					ELSE (STOCK_QUANTITY_ON_PERIOD_START + GR)
				END)
			ELSE 0 
		END PRICE
	FROM J3RF_RECAL_QTY J
		LEFT JOIN REVAL R ON 1=1
			AND R.sap_code_mix = J.Sap_code_mix2
			-- AND R.sap_code_mix = J.Sap_code_mix
)
, J_TEMP AS (
	SELECT
		J.*
		, CASE WHEN J.Transfer <= 0 THEN J.Price 
			else (
				ISNULL(J.AMOUNT, 0)
				+ ISNULL(J.GR_AMT, 0)
				+ isnull(J1.Price, 0) * isnull(J1.Transfer,0) * (-1)
			) / (
				case when (J.STOCK_QUANTITY_ON_PERIOD_START + j.GR + J.Transfer) = 0 then 1
					else J.STOCK_QUANTITY_ON_PERIOD_START + j.GR + J.Transfer
				end
			) 
		end 													as PRICE_STOCK
		, J1.TRANSFER											AS J1_TRANSFER
	FROM TRANSFER J
		LEFT JOIN TRANS_DOC T ON 1=1
			AND J.sap_code_mix = T.receipt_sap_code_mix
			AND J.Transfer > 0
		LEFT JOIN TRANSFER J1 ON 1=1
			AND J1.sap_code_mix = CONCAT(T.MATNR,T.WERKS, T.LGORT)
			and j1.Transfer < 0 
	WHERE 1=1
		AND ISNULL(J.STORAGE_LOCATION,'') IN ('SP01', 'SP05', 'sp04')
)
	SELECT
		J.sap_code_mix
		, J.MATERIAL
		, J.PLANT
		, J.STORAGE_LOCATION
		, J.PRICE_STOCK 										AS Price

		, J.STOCK_QUANTITY_ON_PERIOD_START 						AS OB
		, J.AMOUNT 												AS OB_AMT

		, j.GR													AS GR
		, J.GR_AMT												AS GR_AMNT

		, J.GI													AS GI
		, CASE WHEN J.PRICE_STOCK = 0 THEN J.GI_AMT
			ELSE J.GI * J.PRICE_STOCK
		END 													AS GI_AMT

		, J.Transfer											AS [TRANSFER]
		, CASE WHEN J.Transfer <= 0 
				THEN J.PRICE_STOCK * J.Transfer 
			else J.PRICE_STOCK * J1_TRANSFER * (-1) 
		END 													AS TRANSFER_AMT

		, J.Other												AS OTHER
		, J.other * J.PRICE_STOCK 								AS OTHER_AMT

		, j.CB													AS CB
		, (
			isnull(J.AMOUNT, 0)
			+ isnull(J.GR_AMT, 0)
			+ CASE WHEN J.PRICE_STOCK =  0 THEN J.GI_AMT 					ELSE J.GI * J.PRICE_STOCK END -- gi_amt
			+ CASE WHEN J.Transfer    <= 0 THEN J.PRICE_STOCK * J.Transfer 	ELSE J.PRICE_sTOCK * J1_TRANSFER * (-1) end
			+ J.other * J.PRICE_STOCK						
		)														AS CB_AMT
	-- INTO #TMP_HOANGLE_MARKET_PRICE
	FROM J_TEMP J
	-- WHERE 1=1
	-- 	AND J.MATERIAL = '61224499'
;



SELECT * FROM #TMP_HOANGLE_MARKET_PRICE
WHERE 1=1
	AND MATERIAL = '61174059'