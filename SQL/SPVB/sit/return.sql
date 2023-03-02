SELECT *
FROM dbo.W_CMMS_TRANSACTION_F
WHERE 1=1
    AND ITEM_NO = '60005743'
    AND RET_WONUM = 'WO7000543495'
;

SELECT *
FROM FND.W_CMMS_MATU_F
WHERE ITEM_NUM = '60005743';

-- Result:
--------------------------------------------------------------------------------------

SELECT *
FROM dbo.W_CMMS_TRANSACTION_F
WHERE 1=1
    AND ITEM_NO = '60001007'
    AND RET_WONUM = 'WO7000543492'
;

SELECT *
FROM FND.W_CMMS_MATU_F
WHERE ITEM_NUM = '60001007';

-- Result:
--------------------------------------------------------------------------------------