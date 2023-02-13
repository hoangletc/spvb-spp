SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [FND].[W_SAP_ACDOCA_SPP_F]
(
	[RCLNT] [nvarchar](500) NULL,
	[RLDNR] [nvarchar](500) NULL,
	[RBUKRS] [nvarchar](500) NULL,
	[GJAHR] [nvarchar](500) NULL,
	[BELNR] [nvarchar](500) NULL,
	[DOCLN] [nvarchar](500) NULL,
	[RYEAR] [nvarchar](500) NULL,
	[BUDAT] [nvarchar](500) NULL,
	[RTCUR] [nvarchar](500) NULL,
	[RWCUR] [nvarchar](500) NULL,
	[RHCUR] [nvarchar](500) NULL,
	[RACCT] [nvarchar](500) NULL,
	[RCNTR] [nvarchar](500) NULL,
	[PRCTR] [nvarchar](500) NULL,
	[RFAREA] [nvarchar](500) NULL,
	[EBELN] [nvarchar](500) NULL,
	[RBUSA] [nvarchar](500) NULL,
	[KOKRS] [nvarchar](500) NULL,
	[WERKS] [nvarchar](500) NULL,
	[MATNR] [nvarchar](500) NULL,
	[MATNR_COPA] [nvarchar](500) NULL,
	[VKBUR_PA] [nvarchar](500) NULL,
	[VTWEG] [nvarchar](500) NULL,
	[AUGDT] [nvarchar](500) NULL,
	[KTOPL] [nvarchar](500) NULL,
	[AUART_PA] [nvarchar](500) NULL,
	[KDGRP] [nvarchar](500) NULL,
	[BWTAR] [nvarchar](500) NULL,
	[EBELP] [nvarchar](500) NULL,
	[TSL] [decimal](38, 18) NULL,
	[WSL] [decimal](38, 18) NULL,
	[HSL] [decimal](38, 18) NULL,
	[MSL] [decimal](38, 18) NULL,
	[VMSL] [decimal](38, 18) NULL,
	[ANLN1] [nvarchar](500) NULL,
	[SGTXT] [nvarchar](500) NULL,
	[AWREF] [nvarchar](500) NULL,
	[AWITEM] [nvarchar](500) NULL,
	[BWKEY] [nvarchar](500) NULL,
	[RRUNIT] [nvarchar](500) NULL,
	[VPRSV] [nvarchar](500) NULL,
	[BLDAT] [nvarchar](500) NULL,
	[RUNIT] [nvarchar](500) NULL,
	[RVUNIT] [nvarchar](500) NULL,
	[DRCRK] [nvarchar](500) NULL,
	
	[XREVERSING] [nvarchar](500) NULL,
	[XREVERSED] [nvarchar](500) NULL,
	[XTRUEREV] [nvarchar](500) NULL,
	[AWTYP_REV] [nvarchar](500) NULL,
	[AWORG_REV] [nvarchar](500) NULL,
	[AWREF_REV] [nvarchar](500) NULL,
	[SUBTA_REV] [nvarchar](500) NULL,
	[W_BATCH_ID] BIGINT NULL,
	[W_INSERT_DT] DATETIME2 NULL
)
WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	HEAP
)
GO

drop table [dbo].[W_SAP_SPP_TRANSACTION_F]