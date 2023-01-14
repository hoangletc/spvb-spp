SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[W_SAP_TRANSACTION_F]
(
	ASSET_WID                   	INT NOT NULL IDENTITY(1, 1)
	, DATE_WID                  	INT NULL
	, PRODUCT_WID               	INT NULL
	, PLANT_WID                 	INT NULL
	, LOCATION_WID              	INT NULL
	, COST_CENTER_WID           	INT NULL

	, [PLANT_CODE] 					[nvarchar](200) NULL
	, [VALUATION_TYPE] 				[nvarchar](200) NULL
	, [MATERIAL_NUMBER]				[nvarchar](200) NULL
	, [CLIENT_CODE] 				[nvarchar](200) NULL
	, [LEDGER_CODE] 				[nvarchar](200) NULL
	, [STORAGE_LOCATION] 			[nvarchar](200) NULL
	, [COMPANY_CODE] 				[nvarchar](200) NULL
	, [VALUATION_AREA] 				[nvarchar](200) NULL
	, [VALUATION_CLASS] 			[nvarchar](200) NULL
	, [MATERIAL_TYPE] 				[nvarchar](200) NULL
	, [MATERIAL_GROUP] 				[nvarchar](200) NULL
	, [PURCHASING_GROUP] 			[nvarchar](200) NULL
	, [BASE_UNIT_OF_MEASURE] 		[nvarchar](200) NULL
	, [PRICE_CONTROL] 				[nvarchar](200) NULL
	, [DOCUMENT_DATE] 				[nvarchar](200) NULL
	, [DOCUMENT_NUMBER] 			[nvarchar](200) NULL
	, [LINE_ITEM] 					[nvarchar](200) NULL
	, [UNIT] 						[nvarchar](200) NULL
	, [BASE_UNIT] 					[nvarchar](200) NULL
	, [CURRENCY] 					[nvarchar](200) NULL
	, [ACCOUNT_NUMBER] 				[nvarchar](200) NULL
	, [DEBIT_INDICATOR] 			[nvarchar](200) NULL

	, [COST_CENTER] 				[decimal](38, 18) NULL
	, [COST_CENTER_DESCRIPTION] 	[decimal](38, 18) NULL

    , W_DELETE_FLG             		[varchar](1) NULL
	, W_DATASOURCE_NUM_ID       	[int] NULL
	, W_INSERT_DT               	[DATETIME] NULL
	, W_UPDATE_DT               	[nvarchar](200) NULL
	, W_BATCH_ID                	[nvarchar](200) NULL
	, W_INTEGRATION_ID          	[varbinary](500) NULL
)
WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	HEAP
)
GO
