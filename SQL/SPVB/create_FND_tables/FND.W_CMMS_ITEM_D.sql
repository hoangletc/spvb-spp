SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [FND].[W_CMMS_ITEM_D]
(
	[DESCRIPTION] [nvarchar](100) NULL,
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
	[W_BATCH_ID] BIGINT NULL,
	[W_INSERT_DT] DATETIME NULL
)
WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	HEAP
)
GO
