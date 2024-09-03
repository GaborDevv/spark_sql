# Databricks notebook source
import pyspark.sql
import delta

# COMMAND ----------

# MAGIC %md
# MAGIC Configure SPARK to create connection to datalake storage

# COMMAND ----------

storage_account_name = "stgabordevvwesteurope"
storage_account_access_key = "ia4K+6SaKozCtW+KD1mpRbVn+tplzwYGO8otsIFSn/APZV3XbUtGH0HW7CIZRuCsE600Ta5xZFOE+ASt3UM9YA=="
file_location = f"abfss://data@{storage_account_name}.dfs.core.windows.net/"
spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
    storage_account_access_key,
)

# COMMAND ----------

# MAGIC %md
# MAGIC read expedia data

# COMMAND ----------

expedia_df = spark.read.format("avro").load(file_location + "/m07_raw/expedia")

# COMMAND ----------

# MAGIC %md
# MAGIC Create database for delta tables

# COMMAND ----------

db = "Hotel_data"

spark.sql(f"CREATE DATABASE IF NOT EXISTS {db}")
spark.sql(f"USE {db}")

# COMMAND ----------

# MAGIC %md
# MAGIC CREATING DELTA TABLE FROM EXPEDIA DATA

# COMMAND ----------

expedia_df.write.mode("overwrite").format("delta").option(
    "overwriteSchema", "true"
).saveAsTable("EXPEDIA_RAW")

# COMMAND ----------

# MAGIC %md
# MAGIC READING WEATHER DATA

# COMMAND ----------

hotel_df = spark.read.format("parquet").load(file_location + "/m07_raw/hotel-weather")

# COMMAND ----------

# MAGIC %md CREATING DELTA TABLE FROM HOTEL DATA

# COMMAND ----------

hotel_df.write.mode("overwrite").format("delta").option(
    "overwriteSchema", "true"
).saveAsTable("HOTEL_RAW")

# COMMAND ----------

# MAGIC %md
# MAGIC Top 10 hotels with max absolute temperature difference by month

# COMMAND ----------

# MAGIC %sql
# MAGIC EXPLAIN EXTENDED
# MAGIC SELECT
# MAGIC   id,
# MAGIC   month,
# MAGIC   year,
# MAGIC   MAX(avg_tmpr_c) - MIN(avg_tmpr_c) AS max_temp_diff
# MAGIC FROM
# MAGIC   hotel_raw
# MAGIC GROUP BY
# MAGIC   id,
# MAGIC   month,
# MAGIC   year
# MAGIC ORDER BY
# MAGIC   max_temp_diff DESC
# MAGIC LIMIT
# MAGIC   10;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   id,
# MAGIC   month,
# MAGIC   year,
# MAGIC   MAX(avg_tmpr_c) - MIN(avg_tmpr_c) AS max_temp_diff
# MAGIC FROM
# MAGIC   hotel_raw
# MAGIC GROUP BY
# MAGIC   id,
# MAGIC   month,
# MAGIC   year
# MAGIC ORDER BY
# MAGIC   max_temp_diff DESC
# MAGIC LIMIT
# MAGIC   10;

# COMMAND ----------

# MAGIC %md
# MAGIC SAVING THE RESULTS OF THE FIRST TASK

# COMMAND ----------

_sqldf.write.format("parquet").mode("overwrite").save(
    file_location + "/results/sqltask1"
)

# COMMAND ----------

# MAGIC %md
# MAGIC Top 10 busy (e.g., with the biggest visits count) hotels for each month

# COMMAND ----------

# MAGIC %sql
# MAGIC EXPLAIN EXTENDED
# MAGIC /*creating CTE for those data that has different month for checkout and checkin*/
# MAGIC WITH diff_checkdate AS(
# MAGIC   SELECT
# MAGIC     hotel_id,
# MAGIC     YEAR(srch_ci) AS checkin_year,
# MAGIC     MONTH(srch_ci) AS checkin_month,
# MAGIC     YEAR(srch_co) AS checkout_year,
# MAGIC     MONTH(srch_co) AS checkout_month
# MAGIC   FROM
# MAGIC     expedia_raw
# MAGIC   WHERE
# MAGIC     YEAR(srch_ci) != YEAR(srch_co)
# MAGIC     OR MONTH(srch_ci) != MONTH(srch_co)
# MAGIC ),
# MAGIC /*creating CTE for the number of checkins and diff_checkdate data*/
# MAGIC alldata AS(
# MAGIC   SELECT
# MAGIC     hotel_id,
# MAGIC     YEAR(srch_ci) as years,
# MAGIC     MONTH(srch_ci) as months
# MAGIC   FROM
# MAGIC     expedia_raw
# MAGIC   UNION ALL
# MAGIC   SELECT
# MAGIC     hotel_id,
# MAGIC     checkout_year as years,
# MAGIC     checkout_month as months
# MAGIC   FROM
# MAGIC     diff_checkdate
# MAGIC ),
# MAGIC /*creating CTE for grouped data by hotels, years and months*/
# MAGIC grouping AS(
# MAGIC   SELECT
# MAGIC     count(*) as visits,
# MAGIC     *
# MAGIC   FROM
# MAGIC     alldata
# MAGIC   group by
# MAGIC     hotel_id,
# MAGIC     years,
# MAGIC     months
# MAGIC ),
# MAGIC /*creating CTE for partitioning data by months to rank visit counts*/
# MAGIC rankings AS (
# MAGIC   SELECT
# MAGIC     visits,
# MAGIC     RANK() OVER(
# MAGIC       PARTITION BY years,
# MAGIC       months
# MAGIC       order by
# MAGIC         visits DESC
# MAGIC     ) as rankings,
# MAGIC     hotel_id,
# MAGIC     years,
# MAGIC     months
# MAGIC   FROM
# MAGIC     grouping
# MAGIC )
# MAGIC /*limiting selection to only top 10 of each month and dropping null values*/
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   rankings
# MAGIC WHERE
# MAGIC   rankings <= 10
# MAGIC   AND years IS NOT NULL

# COMMAND ----------

# MAGIC %sql
# MAGIC /*creating CTE for those data that has different month for checkout and checkin*/
# MAGIC WITH diff_checkdate AS(
# MAGIC   SELECT
# MAGIC     hotel_id,
# MAGIC     YEAR(srch_ci) AS checkin_year,
# MAGIC     MONTH(srch_ci) AS checkin_month,
# MAGIC     YEAR(srch_co) AS checkout_year,
# MAGIC     MONTH(srch_co) AS checkout_month
# MAGIC   FROM
# MAGIC     expedia_raw
# MAGIC   WHERE
# MAGIC     YEAR(srch_ci) != YEAR(srch_co)
# MAGIC     OR MONTH(srch_ci) != MONTH(srch_co)
# MAGIC ),
# MAGIC /*creating CTE for the number of checkins and diff_checkdate data*/
# MAGIC alldata AS(
# MAGIC   SELECT
# MAGIC     hotel_id,
# MAGIC     YEAR(srch_ci) as years,
# MAGIC     MONTH(srch_ci) as months
# MAGIC   FROM
# MAGIC     expedia_raw
# MAGIC   UNION ALL
# MAGIC   SELECT
# MAGIC     hotel_id,
# MAGIC     checkout_year as years,
# MAGIC     checkout_month as months
# MAGIC   FROM
# MAGIC     diff_checkdate
# MAGIC ),
# MAGIC /*creating CTE for grouped data by hotels, years and months*/
# MAGIC grouping AS(
# MAGIC   SELECT
# MAGIC     count(*) as visits,
# MAGIC     *
# MAGIC   FROM
# MAGIC     alldata
# MAGIC   group by
# MAGIC     hotel_id,
# MAGIC     years,
# MAGIC     months
# MAGIC ),
# MAGIC /*creating CTE for partitioning data by months to rank visit counts*/
# MAGIC rankings AS (
# MAGIC   SELECT
# MAGIC     visits,
# MAGIC     RANK() OVER(
# MAGIC       PARTITION BY years,
# MAGIC       months
# MAGIC       order by
# MAGIC         visits DESC
# MAGIC     ) as rankings,
# MAGIC     hotel_id,
# MAGIC     years,
# MAGIC     months
# MAGIC   FROM
# MAGIC     grouping
# MAGIC )
# MAGIC /*limiting selection to only top 10 of each month and dropping null values*/
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   rankings
# MAGIC WHERE
# MAGIC   rankings <= 10
# MAGIC   AND years IS NOT NULL

# COMMAND ----------

# MAGIC %md
# MAGIC SAVING THE RESULTS OF SECOND TASK

# COMMAND ----------

_sqldf.write.format("parquet").mode("overwrite").save(
    file_location + "/results/sqltask2"
)

# COMMAND ----------

# MAGIC %md 
# MAGIC Weather trend for visits with extended stay (more than 7 days)(the day temperature difference between last and first day of stay and average temperature during stay)

# COMMAND ----------

# MAGIC %sql
# MAGIC EXPLAIN EXTENDED
# MAGIC /*creating CTE to filted data where there is an extended stay (more than 7 days)*/
# MAGIC WITH extended_stays AS (
# MAGIC   SELECT
# MAGIC     *
# MAGIC   FROM
# MAGIC     (
# MAGIC       SELECT
# MAGIC         e.id AS b_id,
# MAGIC         e.srch_ci AS checkin,
# MAGIC         e.srch_co AS checkout,
# MAGIC         e.hotel_id AS hotelId,
# MAGIC         e.user_id
# MAGIC       FROM
# MAGIC         expedia_raw e
# MAGIC       WHERE
# MAGIC         DATEDIFF(e.srch_co, e.srch_ci) > 7
# MAGIC     )
# MAGIC     JOIN hotel_raw h ON hotelId = h.id
# MAGIC   WHERE
# MAGIC     cast(checkin AS DATE) = cast(h.wthr_date AS DATE)
# MAGIC     or cast(checkout AS DATE) = cast(h.wthr_date AS DATE)
# MAGIC ),
# MAGIC /*creating CTE where I have weather data for both checkin and checkout dates*/
# MAGIC extended_stays_with_available_weather AS(
# MAGIC   SELECT
# MAGIC     *
# MAGIC   FROM
# MAGIC     extended_stays
# MAGIC   WHERE
# MAGIC     b_id IN (
# MAGIC       SELECT
# MAGIC         *
# MAGIC       FROM
# MAGIC         (
# MAGIC           SELECT
# MAGIC             b_id
# MAGIC           FROM
# MAGIC             extended_stays
# MAGIC           GROUP BY
# MAGIC             b_id
# MAGIC           HAVING
# MAGIC             count(b_id) > 1
# MAGIC         )
# MAGIC     )
# MAGIC   ORDER BY
# MAGIC     b_id,
# MAGIC     wthr_date
# MAGIC ),
# MAGIC /*creating CTE for calculating temperature difference between checkin date and checkout date+ average of the two*/
# MAGIC added_cols AS(
# MAGIC   SELECT
# MAGIC     *,
# MAGIC     - avg_tmpr_c + LEAD(avg_tmpr_c) OVER (
# MAGIC       PARTITION BY b_id
# MAGIC       ORDER BY
# MAGIC         avg_tmpr_c
# MAGIC     ) AS temperature_diff,
# MAGIC     AVG(avg_tmpr_c) OVER (PARTITION BY b_id) as average_temperature
# MAGIC   FROM
# MAGIC     extended_stays_with_available_weather
# MAGIC )
# MAGIC SELECT
# MAGIC   b_id,
# MAGIC   checkin,
# MAGIC   checkout,
# MAGIC   address,
# MAGIC   temperature_diff,
# MAGIC   average_temperature
# MAGIC FROM
# MAGIC   added_cols
# MAGIC where
# MAGIC   temperature_diff IS NOT NULL

# COMMAND ----------

# MAGIC %sql
# MAGIC /*creating CTE to filted data where there is an extended stay (more than 7 days)*/
# MAGIC WITH extended_stays AS (
# MAGIC   SELECT
# MAGIC     *
# MAGIC   FROM
# MAGIC     (
# MAGIC       SELECT
# MAGIC         e.id AS b_id,
# MAGIC         e.srch_ci AS checkin,
# MAGIC         e.srch_co AS checkout,
# MAGIC         e.hotel_id AS hotelId,
# MAGIC         e.user_id
# MAGIC       FROM
# MAGIC         expedia_raw e
# MAGIC       WHERE
# MAGIC         DATEDIFF(e.srch_co, e.srch_ci) > 7
# MAGIC     )
# MAGIC     JOIN hotel_raw h ON hotelId = h.id
# MAGIC   WHERE
# MAGIC     cast(checkin AS DATE) = cast(h.wthr_date AS DATE)
# MAGIC     or cast(checkout AS DATE) = cast(h.wthr_date AS DATE)
# MAGIC ),
# MAGIC /*creating CTE where I have weather data for both checkin and checkout dates*/
# MAGIC extended_stays_with_available_weather AS(
# MAGIC   SELECT
# MAGIC     *
# MAGIC   FROM
# MAGIC     extended_stays
# MAGIC   WHERE
# MAGIC     b_id IN (
# MAGIC       SELECT
# MAGIC         *
# MAGIC       FROM
# MAGIC         (
# MAGIC           SELECT
# MAGIC             b_id
# MAGIC           FROM
# MAGIC             extended_stays
# MAGIC           GROUP BY
# MAGIC             b_id
# MAGIC           HAVING
# MAGIC             count(b_id) > 1
# MAGIC         )
# MAGIC     )
# MAGIC   ORDER BY
# MAGIC     b_id,
# MAGIC     wthr_date
# MAGIC ),
# MAGIC /*creating CTE for calculating temperature difference between checkin date and checkout date+ average of the two*/
# MAGIC added_cols AS(
# MAGIC   SELECT
# MAGIC     *,
# MAGIC     - avg_tmpr_c + LEAD(avg_tmpr_c) OVER (
# MAGIC       PARTITION BY b_id
# MAGIC       ORDER BY
# MAGIC         avg_tmpr_c
# MAGIC     ) AS temperature_diff,
# MAGIC     AVG(avg_tmpr_c) OVER (PARTITION BY b_id) as average_temperature
# MAGIC   FROM
# MAGIC     extended_stays_with_available_weather
# MAGIC )
# MAGIC SELECT
# MAGIC   b_id,
# MAGIC   checkin,
# MAGIC   checkout,
# MAGIC   address,
# MAGIC   temperature_diff,
# MAGIC   average_temperature
# MAGIC FROM
# MAGIC   added_cols
# MAGIC where
# MAGIC   temperature_diff IS NOT NULL

# COMMAND ----------

# MAGIC %md
# MAGIC SAVING THE RESULTS OF THIRD TASK

# COMMAND ----------

_sqldf.write.format("parquet").mode("overwrite").save(
    file_location + "/results/sqltask3"
)

# COMMAND ----------

# MAGIC %md
# MAGIC SAVING INTERMEDIATE DATA (joined data with all columns from both datasource)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS joined_data AS
# MAGIC SELECT
# MAGIC   e.id as e_id,
# MAGIC   e.date_time,
# MAGIC   e.site_name,
# MAGIC   e.posa_continent,
# MAGIC   e.user_location_country,
# MAGIC   e.user_location_region,
# MAGIC   e.user_location_city,
# MAGIC   e.orig_destination_distance,
# MAGIC   e.user_id,
# MAGIC   e.is_mobile,
# MAGIC   e.is_package,
# MAGIC   e.channel,
# MAGIC   e.srch_ci,
# MAGIC   e.srch_co,
# MAGIC   e.srch_adults_cnt,
# MAGIC   e.srch_children_cnt,
# MAGIC   e.srch_rm_cnt,
# MAGIC   e.srch_destination_id,
# MAGIC   e.srch_destination_type_id,
# MAGIC   e.hotel_id,
# MAGIC   h.address,
# MAGIC   h.avg_tmpr_c,
# MAGIC   h.avg_tmpr_f,
# MAGIC   h.city,
# MAGIC   h.country,
# MAGIC   h.geoHash,
# MAGIC   h.id,
# MAGIC   h.latitude,
# MAGIC   h.longitude,
# MAGIC   h.name,
# MAGIC   h.wthr_date,
# MAGIC   h.year,
# MAGIC   h.month,
# MAGIC   h.day
# MAGIC FROM
# MAGIC   hotel_raw h FULL
# MAGIC   OUTER JOIN expedia_raw e ON e.hotel_id = h.id

# COMMAND ----------

_sqldf.write.format("parquet").mode("overwrite").save(
    file_location + "/results/sql_task_intermediate_data"
)
