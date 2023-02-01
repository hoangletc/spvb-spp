SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [FND].[W_CMMS_MATR_F]
(
	[CURBAL] [real] NULL,
	[LINETYPE] [nvarchar](50) NULL,
	[RECEIVED_UNIT] [nvarchar](50) NULL,
	[TOTAL_CURBAL] [real] NULL,
	[TOBIN] [nvarchar](50) NULL,
	[UNIT_COST] [real] NULL,
	[PO_LINE_NUM] [bigint] NULL,
	[FROM_SITEID] [nvarchar](50) NULL,
	[REFWO] [nvarchar](50) NULL,
	[SPVB_SAPPO] [nvarchar](50) NULL,
	[FINANCIAL_PERIOD] [nvarchar](50) NULL,
	[DESCRIPTION] [nvarchar](200) NULL,
	[ACTUALDATE] [nvarchar](50) NULL,
	[SPVB_SAPRECEIPT] [nvarchar](50) NULL,
	[QTYOVERRECEIVED] [real] NULL,
	[BINNUM] [nvarchar](50) NULL,
	[FROM_STORELOC] [nvarchar](50) NULL,
	[CURRENCY_CODE] [nvarchar](50) NULL,
	[PONUM] [nvarchar](50) NULL,
	[ISSUE_UNIT] [nvarchar](50) NULL,
	[PO_SITEID] [nvarchar](50) NULL,
	[ITEM_NUM] [nvarchar](50) NULL,
	[MRNUM] [nvarchar](50) NULL,
	[TO_STORELOC] [nvarchar](50) NULL,
	[LINECOST] [real] NULL,
	[ACTUALCOST] [real] NULL,
	[MATR_ID] [bigint] NULL,
	[EXCHANGERATE] [real] NULL,
	[SPVB_DND] [nvarchar](50) NULL,
	[TRANSDATE] [nvarchar](50) NULL,
	[ASSET_NUM] [nvarchar](50) NULL,
	[ISSUE_TYPE] [nvarchar](50) NULL,
	[CURRENCYLINECOST] [real] NULL,
	[QUANTITY] [real] NULL,
	[SPVB_SAPREMARK] [nvarchar](50) NULL,
	[INVUSELINE_ID] [nvarchar](50) NULL,
	[LINETYPE_DESCRIPTION] [nvarchar](200) NULL,
	[SITE_ID] [nvarchar](50) NULL,
	[ISSUE] [nvarchar](50) NULL,
	[SHIPMENT_NUM] [nvarchar](100) NULL,
	[W_BATCH_ID] [bigint] NULL,
	[W_INSERT_DT] DATETIME NULL
)
WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	HEAP
)
GO


drop table [FND].[W_CMMS_MATR_F]
