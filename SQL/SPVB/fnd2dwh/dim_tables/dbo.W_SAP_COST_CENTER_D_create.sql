SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[W_SAP_COST_CENTER_D]
(
    [COST_CENTER_WID] INT IDENTITY(1, 1),

    [CLIENT]            [nvarchar](100) NULL,
    [CO_AREA]           [nvarchar](100) NULL,
    [COST_CENTER]       [nvarchar](100) NULL,
    [PAYMENT_CARD]      [nvarchar](100) NULL,
    [EFFECTIVE_DATE]    [nvarchar](100) NULL,
    [COMPANY_CODE]      [nvarchar](100) NULL,
    [COST_CENTER_DESC]  [nvarchar](100) NULL,

    [W_DELETE_FLG] VARCHAR(1) NULL,
    [W_DATASOURCE_NUM_ID] INT NULL,
    [W_INTEGRATION_ID] [nvarchar](300) NULL,
    [W_INSERT_DT] [datetime2] NULL,
    [W_UPDATE_DT] [datetime2] NULL,
    [W_BATCH_ID] [bigint] NULL
)
WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	CLUSTERED COLUMNSTORE INDEX
)
GO