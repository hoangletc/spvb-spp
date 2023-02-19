SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [FND].[W_CMMS_MATR_F]
(
	[QUANTITY] [float] NULL,
	[SITEID] [nvarchar](200) NULL,
	[FINANCIALPERIOD] [nvarchar](200) NULL,
	[PONUM] [nvarchar](200) NULL,
	[ACTUALCOST] [float] NULL,
	[ISSUE] [bit] NULL,
	[MRNUM] [nvarchar](200) NULL,
	[POSITEID] [nvarchar](200) NULL,
	[ITEMNUM] [nvarchar](200) NULL,
	[CURRENCYCODE] [nvarchar](200) NULL,
	[REFWO] [nvarchar](200) NULL,
	[ISSUETYPE] [nvarchar](200) NULL,
	[SPVB_SAPRECEIPT] [nvarchar](200) NULL,
	[TOTALCURBAL] [float] NULL,
	[FROMSITEID] [nvarchar](200) NULL,
	[RECEIVEDUNIT] [nvarchar](200) NULL,
	[DESCRIPTION] [nvarchar](1000) NULL,
	[BINNUM] [nvarchar](200) NULL,
	[ASSETNUM] [nvarchar](200) NULL,
	[SPVB_DND] [nvarchar](200) NULL,
	[SHIPMENTNUM] [nvarchar](200) NULL,
	[MATRECTRANSID] [bigint] NULL,
	[TOSTORELOC] [nvarchar](200) NULL,
	[TRANSDATE] [nvarchar](200) NULL,
	[INVUSELINEID] [nvarchar](200) NULL,
	[LINETYPE] [nvarchar](200) NULL,
	[UNITCOST] [float] NULL,
	[POLINENUM] [bigint] NULL,
	[CURRENCYLINECOST] [float] NULL,
	[ACTUALDATE] [nvarchar](200) NULL,
	[ISSUEUNIT] [nvarchar](200) NULL,
	[SPVB_SAPREMARK] [nvarchar](200) NULL,
	[CURBAL] [float] NULL,
	[SPVB_SAPPO] [nvarchar](200) NULL,
	[FROMSTORELOC] [nvarchar](200) NULL,
	[TOBIN] [nvarchar](200) NULL,
	[EXCHANGERATE] [float] NULL,
	[LINETYPE_DESCRIPTION] [nvarchar](200) NULL,
	[QTYOVERRECEIVED] [nvarchar](200) NULL,

	[W_BATCH_ID] [bigint] NULL,
	[W_INSERT_DT] DATETIME2 NULL
)
WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	HEAP
)
GO
