SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [FND].[W_CMMS_ITEM_D]
(
	[ROTATING] [nvarchar](max) NULL,
	[SPVB_PLANT] [nvarchar](max) NULL,
	[STATUS_DESCRIPTION] [nvarchar](max) NULL,
	[CONDITION_ENABLED] [nvarchar](max) NULL,
	[DESCRIPTION] [nvarchar](max) NULL,
	[ORDER_UNIT] [nvarchar](max) NULL,
	[ISSUE_UNIT] [nvarchar](max) NULL,
	[IS_KIT] [nvarchar](max) NULL,
	[ITEM_NUM] [nvarchar](max) NULL,
	[ITEM_TYPE] [nvarchar](max) NULL,
	[SPVB_OEM] [nvarchar](max) NULL,
	[SPVB_OEMPARTNO] [nvarchar](max) NULL,
	[SPVB_VENDOR] [nvarchar](max) NULL,
	[SPVB_MACHINE] [nvarchar](max) NULL,
	[SPVB_PRODUCTLINE] [nvarchar](max) NULL,
	[SPVB_PARTNO] [nvarchar](max) NULL,
	[SPVB_MUSTRETURN] [nvarchar](max) NULL,
	[STATUS] [nvarchar](max) NULL,
	[W_BATCH_ID] [nvarchar](max) NULL,
	[W_INSERT_DT] [nvarchar](max) NULL
)
WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	HEAP
)
GO
