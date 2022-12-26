
EXEC [dbo].[HL_utils_rmTabl] 'dbo.HL_STG1'
CREATE TABLE dbo.HL_STG1
(
    MappingAssetSizeCode NVARCHAR(90),
    MappedBIGrouping NVARCHAR(80)
)

EXEC [dbo].[HL_utils_rmTabl] 'dbo.HL_STG2'
CREATE TABLE dbo.HL_STG2
(
    MappingAssetSizeCode NVARCHAR(90),
    MappedBIGrouping NVARCHAR(80)
)

EXEC [dbo].[HL_utils_rmTabl] 'dbo.HL_FND1'
CREATE TABLE dbo.HL_FND1
(
    MappingAssetSizeCode NVARCHAR(90),
    MappedBIGrouping NVARCHAR(80)
)

EXEC [dbo].[HL_utils_rmTabl] 'dbo.HL_FND2'
CREATE TABLE dbo.HL_FND2
(
    MappingAssetSizeCode NVARCHAR(90),
    MappedBIGrouping NVARCHAR(80)
)

EXEC [dbo].[HL_utils_rmTabl] 'dbo.HL_ETL_JOB'
CREATE TABLE dbo.HL_ETL_JOB
(
    JOB_NAME NVARCHAR(100) NOT NULL,
    ETL_TYPE NVARCHAR(5),
    SRC_TABLE NVARCHAR(30),
    TRG_TABLE NVARCHAR(30),
    SRC_FOLDER_NAME NVARCHAR(100),
    TRG_FOLDER_NAME NVARCHAR(100)
)


-- CREATE TABLE dbo.HL_ETL_SCHEMA_MAPPING
-- (
--     TGT_TABLE NVARCHAR(100) NOT NULL,
--     SCHEMA_MAPPING NVARCHAR(MAX)
-- )

