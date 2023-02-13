SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[W_CMMS_ITEM_D]
(
    [ITEM_WID] INT NOT NULL IDENTITY(1, 1),

	[DESCRIPTION] [nvarchar](1000) NULL,
	[ISKIT] [nvarchar](100) NULL,
	[ISSUE_UNIT] [nvarchar](100) NULL,
	[ITEM_NUM] [nvarchar](100) NULL,
	[ITEM_TYPE] [nvarchar](100) NULL,
	[LOT_TYPE] [nvarchar](100) NULL,
	[LOT_TYPE_DESCRIPTION] [nvarchar](100) NULL,
	[ORDER_UNIT] [nvarchar](100) NULL,
	[SPP_CLASSIFICATION] [nvarchar](100) NULL,
	[SPP_CLASSIFICATION_DESCRIPTION] [nvarchar](100) NULL,
	[SPVB_ITEM_MUSTNO] [nvarchar](100) NULL,
	[SPVB_MAX] [nvarchar](100) NULL,
	[SPVB_MIN] [nvarchar](100) NULL,
	[SPVB_MUSTRETURN] [nvarchar](100) NULL,
	[SPVB_PLANT] [nvarchar](100) NULL,
	[STATUS] [nvarchar](100) NULL,
	[STATUS_DESCRIPTION] [nvarchar](100) NULL,
	[ITEM_ID] [bigint] NULL,
	[SPVB_PRODUCTLINE] [nvarchar](300) NULL,
	[SPVB_MACHINE] [nvarchar](300) NULL,

    [W_DELETE_FLG] VARCHAR(1) NULL,
    [W_DATASOURCE_NUM_ID] INT NULL,
    [W_INTEGRATION_ID] [nvarchar](300) NULL,
    [W_INSERT_DT] [datetime] NULL,
    [W_UPDATE_DT] [datetime] NULL,
    [W_BATCH_ID] [bigint] NULL
)
WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	HEAP
)
GO
