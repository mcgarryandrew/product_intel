WITH x AS 
(
  SELECT AVG(Pop),ID FROM `YOUR_PROJECT_ID.DATASET.CLIENT_DO_SELL`  
  WHERE Pop IN (
    SELECT MIN(Pop) 
    FROM `YOUR_PROJECT_ID.DATASET.CLIENT_DO_SELL` 
    GROUP BY ID,MonthNum,YearNum
  )
  GROUP BY ID
)
SELECT
    `YOUR_PROJECT_ID.DATASET.CLIENT_DO_SELL` .ID as ID, 
    AVG(Impressions) AS Impressions,
    AVG(newCost) AS newCost,
    ROUND(AVG(Clicks),0) AS Clicks,
    AVG(ConversionValue) AS ConversionValue,
    AVG(Clicks) / AVG(Impressions) AS CTR,
    MAX(IMG) AS IMG, 
    BRAND,
    AVG(`YOUR_PROJECT_ID.DATASET.CLIENT_DO_SELL` .Pop) AS Pop,
    AVG(CAST(itemRev as float64)) as itemRev,
    AVG(CAST(buyToDetailRate AS float64) / 100) as BTDR,
    AVG(CAST(newROAS AS float64)) as ROAS,
    productName,
    country,
    `YOUR_PROJECT_ID.DATASET.CLIENT_DO_SELL` .MonthNum as Monthum,
    `YOUR_PROJECT_ID.DATASET.CLIENT_DO_SELL` .YearNum
FROM
  `YOUR_PROJECT_ID.DATASET.CLIENT_DO_SELL` 
INNER JOIN x 
ON `YOUR_PROJECT_ID.DATASET.CLIENT_DO_SELL` .ID = x.ID
GROUP BY
  `YOUR_PROJECT_ID.DATASET.CLIENT_DO_SELL`.MonthNum,YearNum,ID,BRAND,productName,country