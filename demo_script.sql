/******************************
*          Athena           *
******************************/
-- Ad-Hoc/Exploratory
SELECT COUNT(*) FROM store_sales;

SELECT * FROM store_sales LIMIT 100;

SELECT DISTINCT cd_education_status FROM customer_demographics;

SELECT min(d_date_sk), max(d_date_sk) FROM date_dim WHERE d_year = 2001

-- 40 seconds
SELECT
ss_item_sk,
avg(ss_quantity) agg1,
avg(ss_list_price) agg2,
avg(ss_coupon_amt) agg3,
avg(ss_sales_price) agg4
FROM store_sales ss

INNER JOIN (SELECT cd_demo_sk FROM customer_demographics WHERE cd_education_status = '4 yr Degree') cd
  ON ss.ss_cdemo_sk = cd.cd_demo_sk

WHERE ss_sold_date_sk between 2451911 and 2452275

GROUP BY ss_item_sk;

-- -- 24 minutes
-- SELECT
-- ss_item_sk,
-- avg(ss_quantity) agg1,
-- avg(ss_list_price) agg2,
-- avg(ss_coupon_amt) agg3,
-- avg(ss_sales_price) agg4
-- FROM store_sales ss

-- INNER JOIN date_dim dd
--   ON ss.ss_sold_date_sk = dd.d_date_sk

-- INNER JOIN customer_demographics cd
--   ON ss.ss_cdemo_sk = cd.cd_demo_sk

-- WHERE dd.d_year = 2001
--   AND cd.cd_education_status = '4 yr Degree'

-- GROUP BY ss_item_sk


-- Athena TPCDS Database - Runtime 27m22s
select /* TPC-DS query7.tpl 0.2 */  i_item_id,
        avg(ss_quantity) agg1,
        avg(ss_list_price) agg2,
        avg(ss_coupon_amt) agg3,
        avg(ss_sales_price) agg4
 from store_sales, customer_demographics, date_dim, item, promotion
 where ss_sold_date_sk = d_date_sk and
       ss_item_sk = i_item_sk and
       ss_cdemo_sk = cd_demo_sk and
       ss_promo_sk = p_promo_sk and
       cd_gender = 'M' and
       cd_marital_status = 'M' and
       cd_education_status = '4 yr Degree' and
       (p_channel_email = 'N' or p_channel_event = 'N') and
       d_year = 2001
 group by i_item_id
 order by i_item_id
 limit 100;

/******************************
*          Redshift           *
******************************/

-- Disable Caching
set enable_result_cache_for_session to off;

-- All in Redshift - 19.536s
select /* TPC-DS query7.tpl 0.2 */  i_item_id,
        avg(ss_quantity) agg1,
        avg(ss_list_price) agg2,
        avg(ss_coupon_amt) agg3,
        avg(ss_sales_price) agg4
 from tpcds.store_sales, tpcds.customer_demographics, tpcds.date_dim, tpcds.item, tpcds.promotion
 where ss_sold_date_sk = d_date_sk and
       ss_item_sk = i_item_sk and
       ss_cdemo_sk = cd_demo_sk and
       ss_promo_sk = p_promo_sk and
       cd_gender = 'M' and
       cd_marital_status = 'M' and
       cd_education_status = '4 yr Degree' and
       (p_channel_email = 'N' or p_channel_event = 'N') and
       d_year = 2001
 group by i_item_id
 order by i_item_id
 limit 100;

 -- Large Fact Table (store_sales) in Spectrum, Dimensional Tables in Redshift - 25.332
select /* TPC-DS query7.tpl 0.2 */  i_item_id,
        avg(ss_quantity) agg1,
        avg(ss_list_price) agg2,
        avg(ss_coupon_amt) agg3,
        avg(ss_sales_price) agg4 
 from spectrum_tpcds.store_sales, tpcds.customer_demographics, tpcds.date_dim, tpcds.item, tpcds.promotion
 where ss_sold_date_sk = d_date_sk and
       ss_item_sk = i_item_sk and
       ss_cdemo_sk = cd_demo_sk and
       ss_promo_sk = p_promo_sk and
       cd_gender = 'M' and 
       cd_marital_status = 'M' and
       cd_education_status = '4 yr Degree' and
       (p_channel_email = 'N' or p_channel_event = 'N') and
       d_year = 2001 
 group by i_item_id
 order by i_item_id
 limit 100; 

-- All In Spectrum - 26.123s
select /* TPC-DS query7.tpl 0.2 */  i_item_id, 
        avg(ss_quantity) agg1,
        avg(ss_list_price) agg2,
        avg(ss_coupon_amt) agg3,
        avg(ss_sales_price) agg4 
 from spectrum_tpcds.store_sales, spectrum_tpcds.customer_demographics, spectrum_tpcds.date_dim, spectrum_tpcds.item, spectrum_tpcds.promotion
 where ss_sold_date_sk = d_date_sk and
       ss_item_sk = i_item_sk and
       ss_cdemo_sk = cd_demo_sk and
       ss_promo_sk = p_promo_sk and
       cd_gender = 'M' and 
       cd_marital_status = 'M' and
       cd_education_status = '4 yr Degree' and
       (p_channel_email = 'N' or p_channel_event = 'N') and
       d_year = 2001 
 group by i_item_id
 order by i_item_id
 limit 100;

 -- All In Spectrum - Many Small Files - 27.100s
select /* TPC-DS query7.tpl 0.2 */  i_item_id,
        avg(ss_quantity) agg1,
        avg(ss_list_price) agg2,
        avg(ss_coupon_amt) agg3,
        avg(ss_sales_price) agg4 
 from spectrum_tpcds_many_small_files.store_sales, spectrum_tpcds_many_small_files.customer_demographics, spectrum_tpcds_many_small_files.date_dim, spectrum_tpcds_many_small_files.item, spectrum_tpcds_many_small_files.promotion
 where ss_sold_date_sk = d_date_sk and
       ss_item_sk = i_item_sk and
       ss_cdemo_sk = cd_demo_sk and
       ss_promo_sk = p_promo_sk and
       cd_gender = 'M' and 
       cd_marital_status = 'M' and
       cd_education_status = '4 yr Degree' and
       (p_channel_email = 'N' or p_channel_event = 'N') and
       d_year = 2001 
 group by i_item_id
 order by i_item_id
 limit 100;

-- Get Spectrum Metrics
SELECT
a.*,
(cast(a.sum_s3_scanned_bytes as float))/1024/1024/1024/1024*5.00 as cost
FROM (
	SELECT 
	user,
	query,
	sum(s3_scanned_rows) as sum_s3_scanned_rows,
	sum(s3_scanned_bytes) as sum_s3_scanned_bytes,
	sum(s3query_returned_rows) as sum_s3query_returned_rows,
	sum(s3query_returned_bytes) as sum_s3query_returned_bytes

	FROM SVL_S3QUERY
	GROUP BY user, query
	WHERE starttime::date = CURRENT_DATE
) a
;
