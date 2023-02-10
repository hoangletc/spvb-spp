SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[W_SAP_SPP_TRANSACTION_F]
(
	DATE_WID                  		INT 				NULL
	, PRODUCT_WID               	INT 				NULL
	, PLANT_WID                 	INT 				NULL
	, COST_CENTER_WID           	INT 				NULL
	, ASSET_WID						INT 				NULL

	, [PLANT_CODE] 					[nvarchar](50) 		NULL
	, [VALUATION_TYPE] 				[nvarchar](50) 		NULL
	, [MATERIAL_NUMBER]				[nvarchar](50) 		NULL
	, [CLIENT_CODE] 				[nvarchar](50) 		NULL
	, [LEDGER_CODE] 				[nvarchar](50) 		NULL
	, [STORAGE_LOCATION] 			[nvarchar](50) 		NULL
	, [COMPANY_CODE] 				[nvarchar](50) 		NULL
	, [VALUATION_AREA] 				[nvarchar](50) 		NULL
	, [VALUATION_CLASS] 			[nvarchar](50) 		NULL
	, [MATERIAL_TYPE] 				[nvarchar](50) 		NULL
	, [MATERIAL_GROUP] 				[nvarchar](50) 		NULL
	, [PURCHASING_GROUP] 			[nvarchar](50) 		NULL
	, [BASE_UNIT_OF_MEASURE] 		[nvarchar](50) 		NULL
	, [PRICE_CONTROL] 				[nvarchar](50) 		NULL
	, [DOCUMENT_DATE] 				[nvarchar](50) 		NULL
	, [DOCUMENT_NUMBER] 			[nvarchar](50) 		NULL
	, [LINE_ITEM] 					[nvarchar](50) 		NULL
	, [UNIT] 						[nvarchar](50) 		NULL
	, [BASE_UNIT] 					[nvarchar](50) 		NULL
	, [CURRENCY] 					[nvarchar](50) 		NULL
	, [ACCOUNT_NUMBER] 				[nvarchar](50) 		NULL
	, [DEBIT_INDICATOR] 			[nvarchar](50) 		NULL

	, [COST_CENTER] 				[nvarchar](20) 		NULL
	, [COST_CENTER_DESC] 			[nvarchar](100) 	NULL

	, [QUANTITY] 					[decimal](38, 20) 	NULL
	, [MATERIAL_DOCUMENT]			[nvarchar](20) 		NULL
	, [MATERIAL_LINE]				[nvarchar](20) 		NULL
	, [STOCK_VALUE] 				[decimal](38, 20) 	NULL
	, [LOCAL_AMOUNT] 				[decimal](38, 20) 	NULL
	, [MOVEMENT_TYPE] 				[nvarchar](10) 		NULL
	, [UPLOADING_POINT] 			[nvarchar](100) 	NULL
	, [PURCHASE_DOCUMENT] 			[nvarchar](20) 		NULL
	, [PURCHASE_LINE_ITEM] 			[nvarchar](20) 		NULL

    , W_DELETE_FLG             		[varchar](1) 		NULL
	, W_DATASOURCE_NUM_ID       	[int] 				NULL
	, W_INSERT_DT               	[DATETIME2] 		NULL
	, W_UPDATE_DT               	[DATETIME2] 		NULL
	, W_BATCH_ID                	[nvarchar](50)  	NULL
	, W_INTEGRATION_ID          	[nvarchar](200) 	NULL
)
WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	HEAP
)
GO