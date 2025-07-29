-- 1. From the two most commonly appearing regions, which is the latest datasource?
WITH top_regions AS (
  SELECT region
  FROM trips
  GROUP BY region
  ORDER BY COUNT(*) DESC
  LIMIT 2
)
SELECT region, datasource, MAX(datetime) AS latest_time
FROM trips
WHERE region IN (SELECT region FROM top_regions)
GROUP BY region, datasource
ORDER BY region, latest_time DESC;


-- 2. What regions has the "cheap_mobile" datasource appeared in?
SELECT DISTINCT region
FROM trips
WHERE datasource = 'cheap_mobile';
