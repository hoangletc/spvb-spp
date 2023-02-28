SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [FND].[W_CMMS_MATU_F]
(
	[INVUSELINE] [nvarchar](100) NULL,
	[CURBAL] [real] NULL,
	[LOCATION] [nvarchar](100) NULL,
	[LINE_TYPE] [nvarchar](100) NULL,
	[UNITCOST] [real] NULL,
	[QTY_REQUESTED] [real] NULL,
	[REFWO] [nvarchar](100) NULL,
	[STORELOC] [nvarchar](100) NULL,
	[DESCRIPTION] [nvarchar](1000) NULL,
	[LINECOST] [real] NULL,
	[BINNUM] [nvarchar](100) NULL,
	[CURRENCY_CODE] [nvarchar](100) NULL,
	[PONUM] [nvarchar](100) NULL,
	[ISSUE_UNIT] [nvarchar](100) NULL,
	[ITEM_NUM] [nvarchar](100) NULL,
	[INVUSE_ID] [bigint] NULL,
	[MRNUM] [nvarchar](100) NULL,
	[ACTUAL_COST] [real] NULL,
	[EXCHANGERATE] [real] NULL,
	[TRANSDATE] datetime2 NULL,
	[ACTUALDATE] datetime2 NULL,
	[ASSET_NUM] [nvarchar](100) NULL,
	[TO_SITEID] [nvarchar](100) NULL,
	[ISSUE_TYPE] [nvarchar](100) NULL,
	[ORG_ID] [nvarchar](100) NULL,
	[QUANTITY] [real] NULL,
	[INVUSELINE_ID] [nvarchar](100) NULL,
	[MATU_ID] [bigint] NULL,
	[FILE_NAME] [nvarchar](100) NULL,
	[W_BATCH_ID] [bigint] NULL,
	[W_INSERT_DT] [datetime2] NULL
)
WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	HEAP
)
GO
