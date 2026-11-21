-- =============================================================================
-- BigQuery ML — Dự báo rủi ro trễ hạn pre-shipment (bản Cloud của Bước 7, tùy chọn)
-- Chạy trong BigQuery SQL Editor. Thay `PROJECT` bằng Project ID của nhóm.
-- Nguồn: bảng Gold đã load ở Bước 4 (không phụ thuộc định nghĩa view — tránh lệch giữa 2 bản view)
-- =============================================================================

-- 0. Bảng feature pre-shipment (giống ml/data_prep.py), loại đơn hủy, gán split theo thời gian
CREATE OR REPLACE TABLE `PROJECT.dataco_logistics_gold.ml_features` AS
SELECT
  f.order_item_id,
  f.order_id,
  f.late_delivery_risk,
  IF(f.order_date_ts >= TIMESTAMP('2017-07-01'), 'test', 'train') AS split,
  s.shipping_mode, s.payment_type,
  g.market, g.order_region,
  c.customer_segment,
  p.department_name, p.category_name,
  EXTRACT(HOUR FROM f.order_date_ts)                     AS order_hour,
  MOD(EXTRACT(DAYOFWEEK FROM f.order_date_ts) + 5, 7) + 1 AS order_dayofweek,  -- 1=Mon..7=Sun
  EXTRACT(MONTH FROM f.order_date_ts)                    AS order_month,
  IF(MOD(EXTRACT(DAYOFWEEK FROM f.order_date_ts) + 5, 7) + 1 >= 6, 1, 0) AS is_weekend,
  IF(EXTRACT(MONTH FROM f.order_date_ts) IN (10, 11, 12), 1, 0)          AS is_peak_season,
  f.order_item_quantity, f.product_price, f.discount_rate, f.net_sales,
  COUNT(*) OVER (PARTITION BY f.order_id)                AS items_in_order
FROM `PROJECT.dataco_logistics_gold.fact_order_shipments` f
JOIN `PROJECT.dataco_logistics_gold.dim_shipping_ops` s USING (shipping_ops_key)
JOIN `PROJECT.dataco_logistics_gold.dim_geography`    g USING (destination_geo_key)
JOIN `PROJECT.dataco_logistics_gold.dim_product`      p USING (product_key)
JOIN `PROJECT.dataco_logistics_gold.dim_customer`     c USING (customer_key)
WHERE s.delivery_status != 'Shipping canceled';

-- 1a. Logistic Regression
CREATE OR REPLACE MODEL `PROJECT.dataco_logistics_gold.bqml_late_logreg`
OPTIONS (model_type = 'LOGISTIC_REG', input_label_cols = ['late_delivery_risk'],
         data_split_method = 'NO_SPLIT') AS
SELECT * EXCEPT (order_item_id, order_id, split)
FROM `PROJECT.dataco_logistics_gold.ml_features` WHERE split = 'train';

-- 1b. Boosted Trees (XGBoost trong BigQuery)
CREATE OR REPLACE MODEL `PROJECT.dataco_logistics_gold.bqml_late_xgb`
OPTIONS (model_type = 'BOOSTED_TREE_CLASSIFIER', input_label_cols = ['late_delivery_risk'],
         max_iterations = 100, learn_rate = 0.1, max_tree_depth = 6,
         data_split_method = 'NO_SPLIT', enable_global_explain = TRUE) AS
SELECT * EXCEPT (order_item_id, order_id, split)
FROM `PROJECT.dataco_logistics_gold.ml_features` WHERE split = 'train';

-- 2. Đánh giá trên tập test out-of-time
SELECT 'LOGISTIC_REG' AS model, * FROM ML.EVALUATE(MODEL `PROJECT.dataco_logistics_gold.bqml_late_logreg`,
  (SELECT * FROM `PROJECT.dataco_logistics_gold.ml_features` WHERE split = 'test'))
UNION ALL
SELECT 'BOOSTED_TREE', * FROM ML.EVALUATE(MODEL `PROJECT.dataco_logistics_gold.bqml_late_xgb`,
  (SELECT * FROM `PROJECT.dataco_logistics_gold.ml_features` WHERE split = 'test'));

-- 3. Feature importance toàn cục
SELECT * FROM ML.GLOBAL_EXPLAIN(MODEL `PROJECT.dataco_logistics_gold.bqml_late_xgb`);

-- 4. Bảng chấm điểm cho Power BI
CREATE OR REPLACE TABLE `PROJECT.dataco_logistics_gold.bqml_predictions` AS
SELECT order_item_id, split, late_delivery_risk AS actual_late,
       predicted_late_delivery_risk AS pred_label,
       (SELECT prob FROM UNNEST(predicted_late_delivery_risk_probs) WHERE label = 1) AS pred_prob_late
FROM ML.PREDICT(MODEL `PROJECT.dataco_logistics_gold.bqml_late_xgb`,
                (SELECT * FROM `PROJECT.dataco_logistics_gold.ml_features`));
