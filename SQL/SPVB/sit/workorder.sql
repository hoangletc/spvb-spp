
--------------------------------------------------------------------------------------
SELECT *
FROM dbo.W_CMMS_WO_F
WHERE 1=1
    AND WORK_ORDERS = 'WO7000004377';

SELECT *
FROM FND.W_CMMS_WO_STATUS_D
WHERE 1=1
    AND WONUM = 'WO7000004377'
    AND GLACCOUNT = '530301'
;

-- Result: issue with WOSTATUS changedate
--------------------------------------------------------------------------------------

SELECT top 100 *
FROM dbo.W_CMMS_WO_F
where asset_wid = 0
WHERE 1=1
    AND WORK_ORDERS = 'WO7000016163';

SELECT *
FROM STG.W_CMMS_WO_STATUS_DS
WHERE 1=1
    AND WONUM = 'WO7000016163'
;

-- Result: issue with WOSTATUS changedate
--------------------------------------------------------------------------------------


select top 10 * From dbo.W_CMMS_ASSET_D;