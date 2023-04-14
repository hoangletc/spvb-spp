SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [FND].[W_CMMS_MATU_F]
(
	  [MATU_ID] BIGINT NULL
	, [INVUSE_ID] BIGINT NULL
	, [INVUSELINE_ID] BIGINT NULL

	, [CURBAL] DECIMAL(38, 20) NULL
	, [LOCATION] [nvarchar](100) NULL
	, [LINE_TYPE] [nvarchar](100) NULL
	, [UNITCOST] DECIMAL(38, 20) NULL
	, [QTY_REQUESTED] DECIMAL(38, 20) NULL
	, [REFWO] [nvarchar](100) NULL
	, [STORELOC] [nvarchar](100) NULL
	, [DESCRIPTION] [nvarchar](1000) NULL
	, [LINECOST] DECIMAL(38, 20) NULL
	, [BINNUM] [nvarchar](100) NULL
	, [CURRENCY_CODE] [nvarchar](100) NULL
	, [PONUM] [nvarchar](100) NULL
	, [ISSUE_UNIT] [nvarchar](100) NULL
	, [ITEM_NUM] [nvarchar](100) NULL
	, [MRNUM] [nvarchar](100) NULL
	, [ACTUAL_COST] DECIMAL(38, 20) NULL
	, [EXCHANGERATE] DECIMAL(38, 20) NULL
	, [TRANSDATE] datetime2 NULL
	, [ACTUALDATE] datetime2 NULL
	, [ASSET_NUM] [nvarchar](100) NULL
	, [TO_SITEID] [nvarchar](100) NULL
	, [ISSUE_TYPE] [nvarchar](100) NULL
	, [ORG_ID] [nvarchar](100) NULL
	, [QUANTITY] DECIMAL(38, 20) NULL
	
	, [FILE_NAME] [nvarchar](100) NULL
	, [W_INTEGRATION_ID] [nvarchar](4000) NULL
	, [W_BATCH_ID] INT NULL
	, [W_INSERT_DT] DATETIME2 NULL
	, [W_DELETE_FLG] VARCHAR(1) NULL
	, [W_DATASOURCE_NUM_ID] INT NULL
	, [W_UPDATE_DT] DATETIME2 NULL
)
WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	HEAP
)
GO
