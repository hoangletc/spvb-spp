SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[W_CMMS_TRANSACTION_F]
(
    [TRANSACTION_WID] INT NOT NULL IDENTITY(1, 1),

    [LOC_WID] INT NULL,
    [ASSET_WID] INT NULL,
    [INVU_WID] INT NULL,
    [INVUL_WID] INT NULL,
    [ITEM_WID] INT NULL,
    [WO_WID] INT NULL,
    [DATE_WID] INT NULL,

    [USAGE] [nvarchar](1000) NULL,
    [WAREHOUSE] [nvarchar](1000) NULL,
    [WAREHOUSE_NAME] [nvarchar](1000) NULL,
    [TRANSACTION_TYPE] [varchar](1000) NULL,
    [TRANSACTION_DATE] DATETIMEOFFSET NULL,
    [ACTUAL_DATE] DATETIMEOFFSET NULL,
    [ITEM_NO] [nvarchar](1000) NULL,
    [DESCRIPTION] [nvarchar](1000) NULL,
    [TRANSACTION_QUANT] [nvarchar](1000) NULL,
    [TRANSACTION_UOM] [nvarchar](1000) NULL,
    [BINNUM] [nvarchar](1000) NULL,
    [OVERHAUL] [nvarchar](1) NULL,
    [MUST_RETURN_ORIGINAL] [nvarchar](1) NULL,
    [MUST_RETURN_USER_INPUT] [nvarchar](1) NULL,
    [MUST_RETURN_REMARK] [nvarchar](1000) NULL,
    [PRICE] [real] NULL,
    [AMOUNT] [real] NULL,
    [MRNUM] [nvarchar](1000) NULL,
    [WORK_ORDER] [nvarchar](1000) NULL,
    [ASSET] [nvarchar](1000) NULL,
    [LINE] [nvarchar](1000) NULL,
    [WORK_TYPE] [nvarchar](1000) NULL,
    [WORKORDER_STATUS] [nvarchar](1000) NULL,
    [ACTUAL_FINISH] [nvarchar](1000) NULL,
    [WO_LAST_STATUSDATE] DATETIMEOFFSET NULL,
    [WO_DONE_BY] [nvarchar](1000) NULL,
    [USER_ID] [nvarchar](1000) NULL,
    [REASON_CODE] [nvarchar](1000) NULL,
    [JOURNAL_CMT_HEADER] [nvarchar](1000) NULL,
    [PONUM] [nvarchar](1000) NULL,
    [SAP_DND] [nvarchar](1000) NULL,
    [RET_WONUM] [nvarchar](1000) NULL,


    [W_DELETE_FLG] VARCHAR(1) NULL,
    [W_DATASOURCE_NUM_ID] INT NULL,
    [W_INTEGRATION_ID] [nvarchar](500) NULL,
    [W_INSERT_DT] [datetime2] NULL,
    [W_UPDATE_DT] [datetime2] NULL,
    [W_BATCH_ID] [bigint] NULL
)
WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	HEAP
)
GO
