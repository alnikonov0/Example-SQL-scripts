/* ВСЕ ПРИВЕДЕННЫЕ НИЖЕ СХЕМЫ, ТАБЛИЦЫ, СТОЛБЦЫ, ФИЛЬТРЫ ЯВЛЯЮТСЯ ПРИМЕРАМИ И ОТЛИЧАЮТСЯ ОТ ОРИГИНАЛЬНОЙ РЕАЛИЗАЦИИ */

/* 
Скрипт формирует таблицу интервалов доверия для ошибок прогноза по каждому партнёру, дню недели и неделе горизонта прогноза.

Это помогает понять, насколько прогноз отличается от факта, и какие диапазоны ошибок типичны.

Можно использовать для контроля качества модели, планирования денежных потоков и анализа рисков.
*/

INSERT INTO TEST_SCHEMA.CP_ML_INTERVALS
(
    horizon_week,
    weekday,
    partner,
    saldo_percentile_lower,
    saldo_percentile_upper,
    cashout_percentile_lower,
    cashout_percentile_upper,
    cashin_percentile_lower,
    cashin_percentile_upper
)
WITH FACT_CASHFLOW AS (
    /*Таблица с подневным фактом*/
    SELECT
        payment_base_date AS pred_date,
        business_partner_group AS partner,
        SUM(DECODE(flow_type, 'Поступления', payment_amount_rub, 0)) AS fact_cashin,
        SUM(DECODE(flow_type, 'Выбытия', payment_amount_rub, 0)) AS fact_cashout
    FROM (
        SELECT
            payment_base_date,
            flow_type,
            DECODE(
                LTRIM(business_partner_id, '0'),
                '100001', 'partner_1',
                '100002', 'partner_2',
                '100003', 'partner_3',
                '100004', 'partner_4',
                '100005', 'partner_5',
                '100006', 'partner_6',
                '100007', 'partner_7',
                '100008', 'partner_8',
                '100009', 'partner_9',
                '100010', 'partner_10',
                'bulk'
            ) AS business_partner_group,
            payment_amount_rub
        FROM TEST_SCHEMA.CP_CASHFLOW
        WHERE markup = 'модель'
            AND LTRIM(business_partner_id, '0') != 100010
    ) AS subquery
    GROUP BY
        payment_base_date,
        business_partner_group
),
PARTNER_DECODE AS (
    SELECT
        pred_date,
        date,
        horizon,
        pred_cashin,
        pred_cashout,
        pred_saldo,
        DECODE(
            DESCRIPTION_ID,
            1, 'bulk',
            2, 'partner_1',
            3, 'partner_2',
            4, 'partner_3',
            5, 'partner_4',
            6, 'partner_5',
            7, 'partner_6',
            8, 'partner_7',
            9, 'partner_8',
            10, 'partner_9'
        ) AS partner
    FROM TEST_SCHEMA.CP_ML_CALCULATE
    WHERE is_holiday = 0
        AND DESCRIPTION_ID IN (
            SELECT id
            FROM TEST_SCHEMA.CP_ML_WORKFLOW_DESCRIPTION AS mwdf
            WHERE stage = 'prod'
                AND forecast_flow != 'ЗСНХ'
        )
        AND horizon >= 1
),
PRED_CASHFLOW AS (
    SELECT
        pred_date,
        date,
        horizon,
        AVG(pred_cashin) AS pred_cashin,
        AVG(pred_cashout) AS pred_cashout,
        AVG(pred_saldo) AS pred_saldo,
        partner
    FROM PARTNER_DECODE
    GROUP BY
        partner,
        pred_date,
        date,
        horizon
),
ERROR_TABLE AS (
    /*Таблица с подневным план-фактом*/
    (
        SELECT
            pc.pred_date,
            pc.partner,
            pc.horizon,
            fc.fact_cashin - pc.pred_cashin AS cashin_error,
            fc.fact_cashout - pc.pred_cashout AS cashout_error,
            fc.fact_cashin + fc.fact_cashout - pc.pred_saldo AS saldo_error
        FROM
            PRED_CASHFLOW AS pc
            LEFT JOIN FACT_CASHFLOW AS fc
                ON
                    pc.pred_date = fc.pred_date
                    AND pc.partner = fc.partner
        WHERE pc.pred_date <= fc.pred_date
    )

    UNION

    (
        SELECT
            pred_date,
            'total' as partner,
            horizon,
            SUM(fact_cashin) - SUM(pred_cashin) AS cashin_error,
            SUM(fact_cashout) - SUM(pred_cashout) AS cashout_error,
            SUM(fact_saldo) - SUM(pred_saldo) AS saldo_error
        FROM (
            SELECT
                pc.pred_date,
                pc.date,
                pc.partner,
                pc.horizon,
                fc.fact_cashin,
                pc.pred_cashin,
                fc.fact_cashout,
                pc.pred_cashout,
                pc.pred_saldo,
                fc.fact_cashin + fc.fact_cashout AS fact_saldo
            FROM
                PRED_CASHFLOW AS pc
                LEFT JOIN FACT_CASHFLOW AS fc
                    ON
                        pc.pred_date = fc.pred_date
                        AND pc.partner = fc.partner
            WHERE pc.pred_date <= fc.pred_date
        ) AS subquery
        GROUP BY
            pred_date,
            date,
            horizon
    )
)
SELECT DISTINCT
    (horizon - 1) // 5 + 1 AS horizon_week,
    CASE
        WHEN DAYOFWEEK(pred_date) = 2 THEN 'Пн'
        WHEN DAYOFWEEK(pred_date) = 3 THEN 'Вт'
        WHEN DAYOFWEEK(pred_date) = 4 THEN 'Ср'
        WHEN DAYOFWEEK(pred_date) = 5 THEN 'Чт'
        WHEN DAYOFWEEK(pred_date) = 6 THEN 'Пт'
        WHEN DAYOFWEEK(pred_date) = 7 THEN 'Сб'
    END AS weekday,
    partner,
    ROUND(PERCENTILE_CONT(0.025) WITHIN GROUP (ORDER BY saldo_error)
        OVER w, 2) AS saldo_percentile_lower,
    ROUND(PERCENTILE_CONT(0.975) WITHIN GROUP (ORDER BY saldo_error)
        OVER w, 2) AS saldo_percentile_upper,
    ROUND(PERCENTILE_CONT(0.025) WITHIN GROUP (ORDER BY cashout_error)
        OVER w, 2) AS cashout_percentile_lower,
    ROUND(PERCENTILE_CONT(0.975) WITHIN GROUP (ORDER BY cashout_error)
        OVER w, 2) AS cashout_percentile_upper,
    ROUND(PERCENTILE_CONT(0.025) WITHIN GROUP (ORDER BY cashin_error)
        OVER w, 2) AS cashin_percentile_lower,
    ROUND(PERCENTILE_CONT(0.975) WITHIN GROUP (ORDER BY cashin_error)
        OVER w, 2) AS cashin_percentile_upper
FROM ERROR_TABLE
WINDOW w AS (PARTITION BY horizon_week, weekday, partner);
