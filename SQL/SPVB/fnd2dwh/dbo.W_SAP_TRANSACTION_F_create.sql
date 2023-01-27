SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[W_SAP_TRANSACTION_F]
(
	ASSET_WID                   INT NOT NULL IDENTITY(1, 1)
	, DATE_WID                  INT NULL
	, PRODUCT_WID               INT NULL
	, PLANT_WID                 INT NULL
	, WO_WID                    INT NULL

	, VENDOR_CODE               [nvarchar](500) NULL
	, PLANT_CODE                [nvarchar](500) NULL
	, COMPANY_CODE              [nvarchar](500) NULL
	, STOCK_STATUS              [nvarchar](500) NULL
	, CURRENCY_CODE             [nvarchar](500) NULL
	, STOCK_VALUE               [decimal](38, 18) NULL
	, BASE_UNIT                 [nvarchar](500) NULL
	, STOCK_QUANTITY_IN_BASE_UNIT [decimal](38, 18) NULL
	, ENTRY_UNIT                [nvarchar](500) NULL
	, STOCK_QUANTITY_IN_ENTRY_UNIT [decimal](38, 18) NULL

	, POSTING_DATE              [DATE] NULL
	, CREATE_DATE               [DATE] NULL
	, DOCUMENT_DATE             [DATE] NULL
	, UPDATE_DATE               [DATE] NULL

	, DOCUMENT_NUMBER           [nvarchar](500) NULL
	, DOCUMENT_LINE_ITEM        [nvarchar](500) NULL
	, LINE_ID                   [nvarchar](500) NULL
	, PURCHASE_DOCUMENT         [nvarchar](500) NULL
	, PURCHASE_LINE_ITEM        [nvarchar](500) NULL
	, ORIGINAL_DOCUMENT_NUM     [nvarchar](500) NULL
	, ORIGINAL_DOCUMENT_LINE    [nvarchar](500) NULL
	, REFERENCE_DOCUMENT        [nvarchar](500) NULL
	, DELIVERY_DOCUMENT         [nvarchar](500) NULL
	, DELIVER_LINE_ITEM         [nvarchar](500) NULL
	, IS_AUTO_FLG               [nvarchar](500) NULL
	, MATERIAL_NUMBER           [nvarchar](500) NULL
	, STORAGE_LOCATION          [nvarchar](500) NULL
	, BATCH_NUMBER              [nvarchar](500) NULL
	, CR_DR_FLG                 [nvarchar](500) NULL
	, [TEXT]                    [nvarchar](500) NULL
	, DOCUMENT_TYPE             [nvarchar](500) NULL
	, PROFIT_CENTER             [nvarchar](500) NULL
	, COST_CENTER               [nvarchar](500) NULL
	, UPLOADING_POINT           [nvarchar](500) NULL
	, MOVEMENT_TYPE             [nvarchar](500) NULL
	, PRICE_INDICATOR           [nvarchar](500) NULL
	, VALUATION_TYPE            [nvarchar](500) NULL

    , W_DELETE_FLG              [varchar](1) NULL
	, W_DATASOURCE_NUM_ID       [int] NULL
	, W_INSERT_DT               [DATETIME] NULL
	, W_UPDATE_DT               [nvarchar](500) NULL
	, W_BATCH_ID                [nvarchar](500) NULL
	, W_INTEGRATION_ID          [varbinary](500) NULL
)
WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	HEAP
)
GO