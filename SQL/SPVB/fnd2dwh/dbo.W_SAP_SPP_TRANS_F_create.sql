SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[W_SAP_SPP_TRANS_F]
(
	[DATE_WID] BIGINT NULL,
	[PLANT_WID] BIGINT NULL,
	[MATERIAL_WID] BIGINT NULL,

	[POSTING_DATE] DATE NULL,
	[PLANT_CODE] [varchar](10) NULL,
	[STORAGE_LOCATION] [varchar](10) NULL,
	[VALUATION_TYPE] [varchar](20) NULL,
	[PRICE_CONTROL] [varchar](20) NULL,
	[MATERIAL_NUMBER] [varchar](20) NULL,
	[QUANTITY] [decimal](38, 20) NULL,
	[MATERIAL_DOCUMENT] [varchar](20) NULL,
	[MATERIAL_LINE] [varchar](20) NULL,
	[MOVEMENT_TYPE] [varchar](20) NULL,
	[ORGINAL_DOCUMENT] [varchar](20) NULL,
	[ORIGINAL_LINE_ITEM] [varchar](20) NULL,
	[ORG_POSTING_DATE] DATE NULL,
	[PURCHASE_DOCUMENT] [varchar](20) NULL,
	[PURCHASE_LINE_ITEM] [varchar](20) NULL,
	[OB_QUANTITY] [decimal](38, 20) NULL,
	[OB_VALUE] [decimal](38, 20) NULL,
	[STOCK_VALUE] [decimal](38, 20) NULL,
	[DEBIT_IND] [varchar](20) NULL,
	[LOCAL_AMT] [decimal](38, 20) NULL,
	[DELIVERY_COST] [decimal](38, 20) NULL,
	[REFERENCE_DOCUMENT] [varchar](20) NULL,
	[UPLOADING_POINT] [nvarchar](100) NULL,
	[MOVEMENT_IND] [nvarchar](20) NULL,
	[RECEIPT_IND] [nvarchar](20) NULL,
	[CONSUMPTION_POSTING] [nvarchar](20) NULL,
	[SPECIAL_IND] [nvarchar](20) NULL,

	[VENDOR_CODE] [varchar](20) NULL,
	[WORK_ORDER] [varchar](200) NULL,
	[COST_CENTER] [varchar](20) NULL, 

	[W_DELETE_FLG] [varchar](20) NULL,
	[W_DATASOURCE_NUM_ID] [varchar](20) NULL,
	[W_INTEGRATION_ID] [nvarchar](300) NULL,
	[W_INSERT_DT] DATETIME2 NULL,
	[W_UPDATE_DT] DATETIME2 NULL,
	[W_BATCH_ID] BIGINT NULL

	

	
)
WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	CLUSTERED COLUMNSTORE INDEX
)
GO
