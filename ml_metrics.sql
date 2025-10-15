/* ВСЕ ПРИВЕДЕННЫЕ НИЖЕ СХЕМЫ, ТАБЛИЦЫ, СТОЛБЦЫ, ФИЛЬТРЫ ЯВЛЯЮТСЯ ПРИМЕРАМИ И ОТЛИЧАЮТСЯ ОТ ОРИГИНАЛЬНОЙ РЕАЛИЗАЦИИ */

/*Таблица содержит точность оценки модели по движению денежных средств относительно факта*/
TRUNCATE TABLE TEST_SCHEMA.CP_ML_METRICS;

INSERT INTO TEST_SCHEMA.CP_ML_METRICS (
    pred_date,
    date,
    horizon,
    partner,
    ae_cashin,
    ae_cashout,
    ae_saldo,
    weekly_wape_cashin,
    weekly_wape_cashout,
    weekly_wape_saldo
)

WITH PARTNER_MAPPING AS (
    SELECT
        id,
        forecast_flow_alias AS partner
    FROM TEST_SCHEMA.CP_ML_WORKFLOW_DESCRIPTION
    WHERE stage = 'prod'
        AND id NOT IN (24, 25, 26)
),

CASHFLOW_PRED_CTE AS (
    /* Таблица с подневным прогнозом */
    SELECT
        cme.pred_date,
        cme.date,
        cme.horizon,
        COALESCE(pm.partner, 'total') AS partner,
        SUM(cme.pred_cashin) AS pred_cashin,
        SUM(cme.pred_cashout) AS pred_cashout,
        SUM(cme.pred_saldo) AS pred_saldo
    FROM TEST_SCHEMA.CP_MAIN_EVENTS AS cme
        INNER JOIN PARTNER_MAPPING AS pm
            ON cme.id = pm.id
    WHERE cme.horizon >= 1
        AND cme.is_holiday = 0
    GROUP BY
        cme.pred_date,
        cme.date,
        cme.horizon,
        ROLLUP(pm.partner)
),

DAILY_CASHFLOW_FACT AS (
    /* Таблица с фактическим движением денежных средств */
    SELECT
        payment_base_date AS pred_date,
        COALESCE(business_partner_group, 'total') AS partner,
        COALESCE(SUM(CASE WHEN flow_type = 'Поступления' THEN payment_amount_rub END), 0) AS fact_cashin,
        COALESCE(SUM(CASE WHEN flow_type = 'Выбытия' THEN payment_amount_rub END), 0) AS fact_cashout,
        COALESCE(SUM(payment_amount_rub), 0) AS fact_saldo
    FROM (
        SELECT
            payment_base_date,
            flow_type,
            CASE
                WHEN new_markup = 'Агентский факторинг' THEN 'partner_a'
                WHEN new_markup = 'Выбытия b' THEN 'partner_b'
                WHEN new_markup IN ('Поступления Partner_1', 'Выбытия Partner_1') THEN 'partner_c'
                WHEN new_markup IN ('Поступления Partner_2', 'Выбытия Partner_2') THEN 'partner_d'
                WHEN new_markup IN ('Поступления Partner_3', 'Выбытия Partner_3') THEN 'partner_e'
                WHEN new_markup IN ('Поступления Partner_4', 'Выбытия Partner_4') THEN 'partner_f'
                WHEN new_markup IN ('Поступления Partner_5', 'Выбытия Partner_5') THEN 'partner_g'
                WHEN new_markup IN ('Поступления Partner_6', 'Выбытия Partner_6') THEN 'partner_h'
                WHEN new_markup IN ('Поступления Partner_7', 'Выбытия Partner_7') THEN 'partner_i'
                WHEN new_markup IN ('Поступления Partner_8', 'Выбытия Partner_8') THEN 'partner_j'
                WHEN new_markup IN ('Поступления Partner_9', 'Выбытия Partner_9') THEN 'partner_k'
                WHEN new_markup IN ('Поступления Partner_10', 'Выбытия Partner_10') THEN 'partner_l'
                WHEN new_markup IN ('Поступления Partner_11', 'Выбытия Partner_11') THEN 'partner_m'
                WHEN new_markup IN ('Поступления Partner_12', 'Выбытия Partner_12') THEN 'partner_n'
            END AS business_partner_group,
            payment_amount_rub
        FROM TEST_SCHEMA.CP_CASHFLOW
        WHERE markup = 'модель'
            AND BE = '9000'
            AND new_markup NOT IN ('Возврат акциза', 'Возврат НДС', 'Налог на прибыль')
    ) AS subquery
    GROUP BY
        payment_base_date,
        ROLLUP(business_partner_group)
),

p AS (SELECT DISTINCT partner FROM DAILY_CASHFLOW_FACT),

WEEK_GRID AS (
    /* Сетка со всеми датами по всем прогнозируемым контрагентам */
    SELECT DISTINCT
        DATE_TRUNC('week', s.pred_date) as week_start,
        s.pred_date,
        p.partner
    FROM (SELECT DISTINCT pred_date FROM DAILY_CASHFLOW_FACT) AS s
        CROSS JOIN p
),

CASHFLOW_FACT_CTE AS (
    /* Таблица с подневным фактом */
    SELECT
        wg.week_start,
        wg.partner,
        wg.pred_date,
        COALESCE(cfc.fact_cashin, 0) AS fact_cashin,
        COALESCE(cfc.fact_cashout, 0) AS fact_cashout,
        COALESCE(cfc.fact_saldo, 0) AS fact_saldo,
        SUM(ABS(COALESCE(cfc.fact_cashin, 0))) OVER w AS weekly_abs_fact_cashin,
        SUM(ABS(COALESCE(cfc.fact_cashout, 0))) OVER w AS weekly_abs_fact_cashout,
        SUM(ABS(COALESCE(cfc.fact_saldo, 0))) OVER w AS weekly_abs_fact_saldo
    FROM WEEK_GRID AS wg
        LEFT JOIN DAILY_CASHFLOW_FACT AS cfc
            ON wg.partner = cfc.partner
               AND cfc.pred_date = wg.pred_date
    WINDOW w AS (PARTITION BY wg.week_start, wg.partner)
),

ERROR_TABLE AS (
    /* Матрица ошибок */
    SELECT
        cpc.pred_date,
        cpc.date,
        cpc.horizon,
        cpc.partner,
        ABS(COALESCE(cfc.fact_cashin, 0) - cpc.pred_cashin) / 1000000 AS ae_cashin,
        ABS(COALESCE(cfc.fact_cashout, 0) - cpc.pred_cashout) / 1000000 AS ae_cashout,
        ABS(COALESCE(cfc.fact_saldo, 0) - cpc.pred_saldo) / 1000000 AS ae_saldo,
        LEAST(
            ABS(COALESCE(cfc.fact_cashin, 0) - cpc.pred_cashin)
            / (COALESCE(cfc.weekly_abs_fact_cashin, 0) + 0.01) * 100, 100
        ) AS weekly_wape_cashin,
        LEAST(
            ABS(COALESCE(cfc.fact_cashout, 0) - cpc.pred_cashout)
            / (COALESCE(cfc.weekly_abs_fact_cashout, 0) + 0.01) * 100, 100
        ) AS weekly_wape_cashout,
        LEAST(
            ABS(COALESCE(cfc.fact_saldo, 0) - cpc.pred_saldo)
            / (COALESCE(cfc.weekly_abs_fact_saldo, 0) + 0.01) * 100, 100
        ) AS weekly_wape_saldo
    FROM CASHFLOW_PRED_CTE AS cpc
        LEFT JOIN CASHFLOW_FACT_CTE AS cfc
            ON cfc.pred_date = cpc.pred_date
               AND cfc.partner = cpc.partner
    WHERE DATE_TRUNC('week', cpc.pred_date) < DATE_TRUNC('week', CURRENT_DATE())
)

SELECT
    pred_date,
    date,
    horizon,
    partner,
    ae_cashin,
    ae_cashout,
    ae_saldo,
    weekly_wape_cashin,
    weekly_wape_cashout,
    weekly_wape_saldo
FROM ERROR_TABLE;
