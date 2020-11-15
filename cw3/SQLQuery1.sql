DECLARE @YearsAgo INT;  
SET @YearsAgo = 8;

SELECT * FROM DimCurrency as DC JOIN FactCurrencyRate as FCR
ON DC.CurrencyKey=FCR.CurrencyKey
WHERE (DC.CurrencyAlternateKey='EUR' OR  DC.CurrencyAlternateKey='GBP') 
AND (DATEADD(Year, -@YearsAgo, GETUTCDATE()) > FCR.Date)