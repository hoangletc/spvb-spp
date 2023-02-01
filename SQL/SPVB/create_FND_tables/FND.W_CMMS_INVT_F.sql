SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [FND].[W_CMMS_INVT_F]
(
	[ACTUALDATE] [nvarchar](50) NULL,
	[BIN_NUM] [nvarchar](50) NULL,
	[CURBAL] [real] NULL,
	[EXTERNAL_REFID] [nvarchar](50) NULL,
	[INVTRANS_ID] [bigint] NULL,
	[INVUSELINE] [nvarchar](50) NULL,
	[ITEM_NUM] [nvarchar](50) NULL,
	[LINECOST] [real] NULL,
	[NEWCOST] [real] NULL,
	[QUANTITY] [real] NULL,
	[SITE_ID] [nvarchar](50) NULL,
	[STORELOC] [nvarchar](50) NULL,
	[TRANSDATE] [nvarchar](50) NULL,
	[TRANSTYPE] [nvarchar](50) NULL,
	[TRANSTYPE_DESCRIPTION] [nvarchar](50) NULL,

	[W_BATCH_ID] [bigint] NULL,
	[W_INSERT_DT] [nvarchar](50) NULL
)
WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	HEAP
)
GO
