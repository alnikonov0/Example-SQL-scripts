/* ВСЕ ПРИВЕДЕННЫЕ НИЖЕ СХЕМЫ, ТАБЛИЦЫ, СТОЛБЦЫ, ФИЛЬТРЫ ЯВЛЯЮТСЯ ПРИМЕРАМИ И ОТЛИЧАЮТСЯ ОТ ОРИГИНАЛЬНОЙ РЕАЛИЗАЦИИ */

/* На вход скрипт получает прогноз модели в формате JSON и преобразовывает его в табличную форму */

DELETE FROM TEST_SCHEMA.CP_ML_CALCULATE
WHERE date = CURRENT_DATE();

INSERT INTO TEST_SCHEMA.CP_ML_CALCULATE (
    pred_date,
    date,
    horizon,
    is_holiday,
    pred_cashin,
    pred_cashout,
    pred_saldo,
    description_id
)
WITH PARSE_JOIN AS (
    SELECT
        MAPITEMS(
            MAPJSONEXTRACTOR(value),
            prediction_key,
            model_name,
            model_version_id,
            calculation_time
        )
            OVER (
                PARTITION BY
                    prediction_key,
                    model_name,
                    model_version_id,
                    calculation_time
            )
    FROM (
        SELECT
            prediction_key,
            model_name,
            model_version_id,
            calculation_time,
            value
        FROM ODS_MLFW.MODEL_PREDICT
        LIMIT 1 OVER (PARTITION BY prediction_name, prediction_key ORDER BY tech_load_ts DESC)
    ) AS t
    WHERE model_name = 'liquidity_forecasting'
),
DESCRIPTION AS (
    SELECT
        id,
        forecast_flow_alias AS alias
    FROM (
        SELECT
            id,
            forecast_flow_alias,
            stage,
            tech_is_deleted
        FROM ODS_SHAREPOINT_ENF.CP_WORKFLOW_DESCRIPTION
        LIMIT 1 OVER (PARTITION BY id ORDER BY tech_load_ts DESC)
    ) AS t
    WHERE tech_is_deleted is false
        and stage = 'prod'
),
PREP AS (
    SELECT
        MAX(DECODE(keys, 'date', values::date)) OVER (w) AS date,
        MAX(DECODE(keys, 'partner', values::varchar)) OVER (w) AS partner,
        COALESCE(NULLIF(SPLIT_PART(keys, '.', 2), ''), keys) AS prediction_num,
        COALESCE(NULLIF(SPLIT_PART(keys, '.', 3), ''), keys) AS keys,
        values,
        prediction_key,
        model_name,
        model_version_id,
        calculation_time
    FROM PARSE_JOIN
    WINDOW
        w AS (
            PARTITION BY
                prediction_key,
                model_name,
                model_version_id,
                calculation_time
        )
),
FINAL AS (
    SELECT
        prediction_key,
        model_name,
        model_version_id,
        calculation_time,
        date,
        partner,
        MAX(DECODE(keys, 'horizon', values::integer)) OVER (w) AS horizon,
        MAX(DECODE(keys, 'is_holiday', values::integer)) OVER (w) AS is_holiday,
        MAX(DECODE(keys, 'pred_cashin', values::integer)) OVER (w) * 1000000 AS pred_cashin,
        MAX(DECODE(keys, 'pred_cashout', values::integer)) OVER (w) * 1000000 AS pred_cashout,
        MAX(DECODE(keys, 'pred_date', values::date)) OVER (w) AS pred_date
    FROM PREP
    WHERE prediction_num NOT IN ('partner', 'date')
    WINDOW w AS (
        PARTITION BY
            prediction_num,
            prediction_key,
            model_name,
            model_version_id,
            calculation_time,
            date,
            partner
    )
)
SELECT
    f.pred_date,
    f.date,
    f.horizon,
    f.is_holiday,
    f.pred_cashin,
    f.pred_cashout,
    f.pred_cashin + f.pred_cashout AS pred_saldo,
    d.id AS description_id
FROM FINAL AS f
    INNER JOIN DESCRIPTION AS d ON f.partner = d.alias
WHERE f.date = CURRENT_DATE()
GROUP BY
    f.pred_date,
    f.date,
    f.horizon,
    f.is_holiday,
    f.pred_cashin,
    f.pred_cashout,
    f.pred_cashin + f.pred_cashout,
    d.id
