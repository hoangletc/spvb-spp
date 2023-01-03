SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [FND].[W_CMMS_ASSET_D]
(
	[STATUS_DES] [nvarchar](max) NULL,
	[ASSET_NUM] [nvarchar](max) NULL,
	[DESCRIPTION] [nvarchar](max) NULL,
	[STATUS] [nvarchar](max) NULL,
	[ASSET_STATUS_CHANGEDATE] [nvarchar](max) NULL,
	[ASSET_STATUS_SPVB_CA] [nvarchar](max) NULL,
	[ASSET_STATUS_SPVB_T] [nvarchar](max) NULL,
	[ASSET_STATUS_SPVB_C] [nvarchar](max) NULL,
	[ASSET_STATUS_IS_RUNNING] [nvarchar](max) NULL,
	[ASSET_STATUS_SPVB_i] [nvarchar](max) NULL,
	[ASSET_STATUS_SPVB_l] [nvarchar](max) NULL,
	[W_BATCH_ID] [nvarchar](max) NULL,
	[W_INSERT_DT] [nvarchar](max) NULL
)
WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	HEAP
)
GO
