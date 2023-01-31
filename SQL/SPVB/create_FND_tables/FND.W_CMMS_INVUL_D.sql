SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [FND].[W_CMMS_INVUL_D]
(
	[INVUSELINE_ID] [bigint] NULL,
	[UNITCOST] [real] NULL,
	[SPVB_EXTREASONCODE] [nvarchar](max) NULL,
	[SPVB_EXTREASONCODE_DESCRIPTION] [nvarchar](max) NULL,
	[SPVB_MUSTRETURN] [nvarchar](max) NULL,
	[SPVB_RETURNFROMISSUE] [nvarchar](max) NULL,
	[SPVB_MUSTRETURN_ORG] [nvarchar](max) NULL,
	[RETURNED_QTY] [real] NULL,
	[REMARK] [nvarchar](max) NULL,
	[LINE_TYPE] [nvarchar](max) NULL,
	[DESCRIPTION] [nvarchar](max) NULL,
	[TO_SITEID] [nvarchar](max) NULL,
	[INVUSE_NUM] [nvarchar](max) NULL,
	[ACTUALDATE] [nvarchar](max) NULL,
	[RECEIVED_QTY] [real] NULL,
	[ASSET_NUM] [nvarchar](max) NULL,
	[COSTCENTER] [nvarchar](max) NULL,
	[FROM_STORELOC] [nvarchar](max) NULL,
	[REFWO] [nvarchar](max) NULL,
	[LINECOST] [real] NULL,
	[QUANTITY] [real] NULL,
	[COSTCENTER_DESCRIPTION] [nvarchar](max) NULL,
	[INVUSELINE_NUM] [bigint] NULL,
	[ITEM_NUM] [nvarchar](max) NULL,
	[ITEMSETID] [nvarchar](max) NULL,
	[LOCATION] [nvarchar](max) NULL,
	[SPVB_SAPPO] [nvarchar](max) NULL,
	[USE_TYPE] [nvarchar](max) NULL,
	[ENTER_BY] [nvarchar](max) NULL,
	[SPVB_WONUMREF] [nvarchar](max) NULL,
	[SPVB_REASON] [nvarchar](max) NULL,
	[MATU_ID] [bigint] NULL,
	[W_BATCH_ID] [bigint] NULL,
	[W_INSERT_DT] DATETIME NULL
)
WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	HEAP
)
GO
