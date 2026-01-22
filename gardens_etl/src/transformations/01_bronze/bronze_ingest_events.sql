-- Raw (bronze) streaming table using Auto Loader
CREATE OR REFRESH STREAMING TABLE ${catalog}.1_bronze.bronze_events
AS
SELECT
  Name, Eventjson, Timestampmillis
FROM STREAM read_files(
  '/Volumes/${catalog}/1_bronze/telemetry_data', 
  format => "json",
  inferColumnTypes => true
);
