/* ВСЕ ПРИВЕДЕННЫЕ НИЖЕ СХЕМЫ, ТАБЛИЦЫ, СТОЛБЦЫ, ФИЛЬТРЫ ЯВЛЯЮТСЯ ПРИМЕРАМИ И ОТЛИЧАЮТСЯ ОТ ОРИГИНАЛЬНОЙ РЕАЛИЗАЦИИ */

/*  
    Скрипт формирует итоговую витрину для дашборда, в котором отображается корректность соблюдения сроков заведения финансовых документов.
    В рамках процесса расходные документы должны заводиться за 15 дней до проводки. 
    В случае несоблюдения сроков на дашборде отображаются отделы, сотрудники, ответственные и суммы.
*/

/*
Схематичное описание data-flow скрипта с комментариями:
@ссылка на внутренний ресурс@
*/
CREATE LOCAL TEMPORARY TABLE IF NOT EXISTS CV_FM_00013_COPY_AGG (
    create_date date,
    change_date date,
    payment_date date,
    payment_amount_rub numeric (17, 2),
    cashflow_number integer,
    finance_position varchar (14),
    finance_position_text varchar (96),
    bpartner varchar (20),
    employee int,
    currency varchar (10),
    zzsatr_kurat_ext varchar (46),
    kurator varchar (242),
    author varchar (24),
    be integer,
    be_name varchar (80),
    ac_doc_typ varchar (4),
    zdognr integer,
    status varchar (60),
    direction varchar (80),
    link_cashflow_number integer
)
ON COMMIT PRESERVE ROWS;


TRUNCATE TABLE CV_FM_00013_COPY_AGG;
INSERT INTO CV_FM_00013_COPY_AGG
(
    create_date,
    change_date,
    payment_date,
    payment_amount_rub,
    cashflow_number,
    finance_position,
    finance_position_text,
    bpartner,
    employee,
    currency,
    zzsatr_kurat_ext,
    kurator,
    author,
    be,
    be_name,
    ac_doc_typ,
    zdognr,
    status,
    direction,
    link_cashflow_number
)

WITH COMP_CODE AS (
    SELECT
        comp_code,
        comp_code_text
    FROM (
        SELECT
            comp_code,
            txtmd AS comp_code_text,
            is_deleted
        FROM ODS_SAPBW._BI0_TCOMP_CODE
        LIMIT 1 OVER (PARTITION BY comp_code ORDER BY load_ts DESC)
    ) AS t
    WHERE is_deleted = 0
),
CMMT_ITEM AS (
    SELECT
        fm_area,
        cmmt_item,
        dateto,
        txtmd
    FROM (
        SELECT
            fm_area,
            cmmt_item,
            dateto,
            txtmd,
            tech_is_deleted
        FROM ODS_SAPBW._BI0_TCMMT_ITEM
        WHERE langu = 'R'
        LIMIT 1 OVER (PARTITION BY fm_area, cmmt_item, langu, dateto ORDER BY tech_load_ts DESC)
    ) AS t
    WHERE tech_is_deleted is false
        AND LENGTH(txtmd) > 0
        AND dateto = '9999-12-31'
),
MAIN AS (
    SELECT
        NULLIF(t.bpartner, '') AS bpartner,
        t.cmmt_item,
        ci.txtmd AS cmmt_item_text,
        t.comp_code::int,
        cc.comp_code_text,
        t.currency,
        t.date,
        t.employee::int,
        t.wbs_elemt,
        t.kblk_zz_base_type,
        t.kblk_zz_off_plan,
        t.kblk_zz_status,
        t.kblk_zz_type_paym,
        t.kblp_aedat,
        t.kblp_aende,
        t.kblp_erdat,
        t.kblp_erfas,
        t.kblp_fdatk,
        t.kblp_wtges,
        t.kurat1 || ' ' || t.kurat2 || ' ' || t.kurat3 AS kurator,
        t.prdg0,
        NULLIF(t.resp, '') AS resp,
        t.responsible,
        t.responsible_ext,
        t.rub,
        t.text_kblk_base_type,
        t.text_kblk_zz_status,
        t.text_kblp_lifnr,
        t.zbelnr::int,
        NULLIF(t.zdognr, '')::int AS zdognr,
        NULLIF(t.zfebep_xblnr, '')::int AS zfebep_xblnr,
        t.zfich243,
        t.zgnch100,
        t.zprotfipos,
        t.zzkblpos,
        t.zzsatr_card_dog_vnnum,
        t.zzsatr_kurat_ext,
        t.tech_load_ts,
        t.ac_doc_typ
    FROM (
        SELECT
            bpartner,
            cmmt_item,
            comp_code,
            currency,
            date,
            employee,
            wbs_elemt,
            kblk_zz_base_type,
            kblk_zz_off_plan,
            kblk_zz_status,
            kblk_zz_type_paym,
            kblp_aedat,
            kblp_aende,
            kblp_erdat,
            kblp_erfas,
            kblp_fdatk,
            kblp_wtges,
            kurat1,
            kurat2,
            kurat3,
            prdg0,
            resp,
            responsible,
            responsible_ext,
            rub,
            text_kblk_base_type,
            text_kblk_zz_status,
            text_kblp_lifnr,
            zbelnr,
            zdognr,
            zfebep_xblnr,
            zfich243,
            zgnch100,
            zprotfipos,
            zzkblpos,
            zzsatr_card_dog_vnnum,
            zzsatr_kurat_ext,
            tech_load_ts,
            tech_is_deleted,
            ac_doc_typ
        FROM ODS_SAPBW.CV_FM_00013 AS cv
        LIMIT 1 OVER (PARTITION BY
            cmmt_item,
            date,
            kblp_erdat,
            kblp_fdatk,
            zbelnr,
            zzkblpos
        ORDER BY tech_load_ts DESC)
    ) AS t
        LEFT JOIN COMP_CODE AS cc ON t.comp_code = cc.comp_code
        LEFT JOIN CMMT_ITEM AS ci ON t.comp_code = ci.fm_area AND t.cmmt_item = ci.cmmt_item
    WHERE t.tech_is_deleted is false
)
SELECT
    create_date,
    change_date,
    payment_date,
    SUM(payment_amount_rub) as payment_amount_rub,
    cashflow_number,
    finance_position,
    finance_position_text,
    bpartner,
    employee,
    currency,
    zzsatr_kurat_ext,
    kurator,
    author,
    be,
    be_name,
    ac_doc_typ,
    zdognr,
    status,
    direction,
    link_cashflow_number
FROM (
    SELECT
        MIN(kblp_erdat) OVER (PARTITION BY zbelnr, cmmt_item) AS create_date,
        date AS change_date,
        kblp_fdatk AS payment_date,
        rub AS payment_amount_rub,
        zbelnr AS cashflow_number,
        cmmt_item AS finance_position,
        cmmt_item_text AS finance_position_text,
        bpartner,
        employee,
        currency,
        zzsatr_kurat_ext,
        kurator,
        kblp_erfas AS author,
        comp_code AS be,
        comp_code_text AS be_name,
        ac_doc_typ,
        zdognr,
        text_kblk_zz_status AS status,
        resp AS direction,
        zfebep_xblnr AS link_cashflow_number
    FROM MAIN
) AS t
GROUP BY
    create_date,
    change_date,
    payment_date,
    cashflow_number,
    finance_position,
    finance_position_text,
    bpartner,
    employee,
    currency,
    zzsatr_kurat_ext,
    kurator,
    author,
    be,
    be_name,
    ac_doc_typ,
    zdognr,
    status,
    direction,
    link_cashflow_number;

CREATE LOCAL TEMPORARY TABLE IF NOT EXISTS ORGUNIT_TEMP
(
    calmonth date,
    orgunit int,
    employee int,
    teamlead varchar (80),
    orgunit_1 varchar (1000),
    orgunit_2 varchar (1000),
    orgunit_3 varchar (1000),
    orgunit_4 varchar (1000),
    orgunit_5 varchar (1000),
    orgunit_6 varchar (1000)
)
ON COMMIT PRESERVE ROWS;


TRUNCATE TABLE ORGUNIT_TEMP;
INSERT INTO ORGUNIT_TEMP (
    calmonth,
    orgunit,
    employee,
    teamlead,
    orgunit_1,
    orgunit_2,
    orgunit_3,
    orgunit_4,
    orgunit_5,
    orgunit_6
)

with ORGUNIT AS (
    SELECT
        orgunit,
        txtlg,
        datefrom,
        dateto
    FROM (
        SELECT
            orgunit AS orgunit,
            txtlg,
            datefrom,
            dateto,
            tech_is_deleted
        FROM ODS_SAPBW._BI0_TORGUNIT
        WHERE langu = 'R'
        LIMIT
            1 OVER (PARTITION BY orgunit, langu, dateto ORDER BY tech_load_ts DESC)
    ) AS t
    WHERE tech_is_deleted IS FALSE
        AND LENGTH(txtlg) > 0
),
EMPLOYEE_ZNRDS AS (
    SELECT DISTINCT
        employee,
        DATE_TRUNC('month', change_date)::date AS calmonth
    FROM CV_FM_00013_COPY_AGG
),
LEADER AS (
    SELECT
        employee::int,
        txtmd,
        datefrom,
        dateto
    FROM (
        SELECT
            employee,
            txtmd,
            datefrom,
            dateto,
            is_deleted
        FROM ODS_SAPBW._BI0_TEMPLOYEE
        LIMIT 1 OVER (PARTITION BY employee, dateto ORDER BY load_ts DESC)
    ) AS t
    WHERE is_deleted = 0
        AND LENGTH(TXTMD) > 0
),
HR AS (
    SELECT
        t.calmonth,
        t.orgunit::int,
        t.employee::int,
        l.TXTMD AS teamlead,
        t.zgnch060,
        t.zgnch061,
        t.zgnch062,
        t.zgnch063,
        t.zgnch064,
        t.zgnch047
    FROM (
        SELECT
            (calmonth || '01')::date AS calmonth,
            orgunit,
            employee,
            zhrch005,
            ZGNCH060,
            ZGNCH061,
            ZGNCH062,
            ZGNCH063,
            ZGNCH064,
            ZGNCH047
        FROM ODS_SAPBW.CV_HR_00025
        WHERE employee != 00000000
    ) AS t
        INNER JOIN EMPLOYEE_ZNRDS AS ez ON ez.employee = t.employee AND ez.calmonth = t.calmonth
        LEFT JOIN LEADER AS l ON t.zhrch005 = l.employee AND t.calmonth between l.datefrom and l.dateto
    WHERE NOT (
            t.ZGNCH060 = '00000000'
            AND t.ZGNCH061 = '00000000'
            AND t.ZGNCH062 = '00000000'
            AND t.ZGNCH063 = '00000000'
            AND t.ZGNCH064 = '00000000'
            AND t.ZGNCH047 = '00000000'
        )
    GROUP BY
        t.calmonth,
        t.orgunit,
        t.employee,
        l.TXTMD,
        t.zgnch060,
        t.zgnch061,
        t.zgnch062,
        t.zgnch063,
        t.zgnch064,
        t.zgnch047
),
TMP as (
    select
        EXPLODE(
            calmonth,
            orgunit,
            employee,
            teamlead,
            array [
                zgnch060,
                zgnch061,
                zgnch062,
                zgnch063,
                zgnch064,
                zgnch047
            ]
        )
            over (
                partition best
            )
        AS (calmonth, orgunit, employee, teamlead, position, value)
    from HR
),
FINAL as (
    select
        TMP.calmonth,
        TMP.orgunit,
        TMP.employee,
        TMP.teamlead,
        MAX(DECODE(TMP.position, 0, og.txtlg)) AS orgunit_1,
        MAX(DECODE(TMP.position, 1, og.txtlg)) AS orgunit_2,
        MAX(DECODE(TMP.position, 2, og.txtlg)) AS orgunit_3,
        MAX(DECODE(TMP.position, 3, og.txtlg)) AS orgunit_4,
        MAX(DECODE(TMP.position, 4, og.txtlg)) AS orgunit_5,
        MAX(DECODE(TMP.position, 5, og.txtlg)) AS orgunit_6
    from TMP
        INNER JOIN ORGUNIT AS og
            ON
                og.orgunit = TMP.value
                AND TMP.calmonth BETWEEN og.datefrom AND og.dateto
    group by
        TMP.calmonth,
        TMP.orgunit,
        TMP.employee,
        TMP.teamlead
)
SELECT
    co.calmonth,
    co.orgunit,
    co.employee,
    co.teamlead,
    co.orgunit_1,
    co.orgunit_2,
    co.orgunit_3,
    co.orgunit_4,
    co.orgunit_5,
    co.orgunit_6
FROM FINAL AS co;

CREATE LOCAL TEMPORARY TABLE IF NOT EXISTS CV_FM_00013_COPY_DATASET
(
    create_date date,
    calmonth date,
    change_date date,
    payment_date date,
    payment_amount_rub numeric (17, 2),
    cashflow_number integer,
    finance_position varchar (14),
    finance_position_text varchar (96),
    kurator varchar (242),
    direction varchar (80),
    link_cashflow_number integer,
    bpartner varchar (20),
    employee int,
    currency varchar (10),
    author varchar (24),
    be integer,
    be_name varchar (80),
    zdognr integer,
    ac_doc_typ varchar (4),
    status varchar (60),
    doc_text varchar (85),
    teamlead varchar (80),
    orgunit_1 varchar (1000),
    orgunit_2 varchar (1000),
    orgunit_3 varchar (1000),
    orgunit_4 varchar (1000),
    orgunit_5 varchar (1000),
    orgunit_6 varchar (1000),
    link_cashflow_create_date date,
    link_direction varchar (80),
    link_rub numeric (17, 2),
    link_flag varchar (6)
)
ON COMMIT PRESERVE ROWS;

TRUNCATE TABLE CV_FM_00013_COPY_DATASET;
INSERT INTO CV_FM_00013_COPY_DATASET (
    create_date,
    calmonth,
    change_date,
    payment_date,
    payment_amount_rub,
    cashflow_number,
    finance_position,
    finance_position_text,
    kurator,
    direction,
    link_cashflow_number,
    bpartner,
    employee,
    currency,
    author,
    be,
    be_name,
    zdognr,
    ac_doc_typ,
    status,
    doc_text,
    teamlead,
    orgunit_1,
    orgunit_2,
    orgunit_3,
    orgunit_4,
    orgunit_5,
    orgunit_6,
    link_cashflow_create_date,
    link_direction,
    link_rub,
    link_flag
)
with LINK_CASHFLOWS AS (
    SELECT
        create_date,
        change_date,
        payment_date,
        payment_amount_rub,
        cashflow_number,
        finance_position,
        finance_position_text,
        bpartner,
        employee,
        currency,
        kurator,
        author,
        be,
        be_name,
        direction,
        ac_doc_typ,
        zdognr,
        status,
        link_cashflow_number,
        'Да' AS link_flag
    FROM CV_FM_00013_COPY_AGG AS main
    WHERE status = 'Удалена'
        AND EXISTS (
            SELECT 1
            FROM CV_FM_00013_COPY_AGG AS agg
            WHERE agg.link_cashflow_number = main.cashflow_number
                AND agg.link_cashflow_number IS NOT NULL
        )
    GROUP BY
        create_date,
        change_date,
        payment_date,
        payment_amount_rub,
        cashflow_number,
        finance_position,
        finance_position_text,
        bpartner,
        employee,
        currency,
        author,
        be,
        be_name,
        kurator,
        direction,
        ac_doc_typ,
        zdognr,
        status,
        link_cashflow_number
),
LINK_DIRECTIONS AS (
    SELECT
        cashflow_number,
        direction
    FROM CV_FM_00013_COPY_AGG AS main
    WHERE EXISTS (
            SELECT 1
            FROM CV_FM_00013_COPY_AGG AS agg
            WHERE agg.link_cashflow_number = main.cashflow_number
                AND agg.link_cashflow_number IS NOT NULL
        )
    LIMIT 1 OVER (PARTITION BY cashflow_number ORDER BY create_date ASC)
),
DOC_TYPE AS (
    SELECT
        ac_doc_typ,
        doc_text
    FROM (
        SELECT
            ac_doc_typ,
            txtmd AS doc_text,
            tech_is_deleted
        FROM ODS_SAPBW._BI0_TAC_DOC_TYP
        WHERE langu = 'R'
        LIMIT 1 OVER (PARTITION BY ac_doc_typ, langu ORDER BY tech_load_ts DESC)
    ) AS t
    WHERE tech_is_deleted is false
),
DATASET AS (
    SELECT
        m.create_date,
        co.calmonth,
        m.change_date,
        m.payment_date,
        m.payment_amount_rub,
        m.cashflow_number,
        m.finance_position,
        m.finance_position_text,
        m.kurator,
        m.direction,
        m.link_cashflow_number,
        m.bpartner,
        m.employee,
        m.currency,
        m.author,
        m.be,
        m.be_name,
        m.zdognr,
        m.ac_doc_typ,
        m.status,
        m.ac_doc_typ || ' ' || doc_type.doc_text AS doc_text,
        co.teamlead,
        co.orgunit_1,
        co.orgunit_2,
        co.orgunit_3,
        co.orgunit_4,
        co.orgunit_5,
        co.orgunit_6,
        lc.create_date AS link_cashflow_create_date,
        lc.payment_amount_rub AS link_rub,
        link_dir.direction as link_direction,
        lc.link_flag
    FROM CV_FM_00013_COPY_AGG AS m
        LEFT JOIN LINK_CASHFLOWS AS lc
            ON m.cashflow_number = lc.cashflow_number AND m.finance_position = lc.finance_position
        LEFT JOIN LINK_DIRECTIONS AS link_dir
            ON m.link_cashflow_number = link_dir.cashflow_number
        LEFT JOIN ORGUNIT_TEMP AS co
            ON
                m.employee = co.employee
                AND co.calmonth <= m.change_date
        LEFT JOIN DOC_TYPE ON m.ac_doc_typ = doc_type.ac_doc_typ
    LIMIT 1 OVER (
        PARTITION BY
            m.create_date, m.change_date, m.payment_date, m.payment_amount_rub, m.cashflow_number,
            m.finance_position,
            m.finance_position_text,
            m.kurator,
            m.direction,
            m.link_cashflow_number,
            m.bpartner,
            m.employee,
            m.currency,
            m.author,
            m.be,
            m.zdognr,
            m.ac_doc_typ,
            m.status,
            lc.create_date, lc.payment_amount_rub, lc.link_flag, link_dir.direction
        ORDER BY co.calmonth DESC, m.create_date, m.change_date DESC
    )
)
SELECT
    create_date,
    calmonth,
    change_date,
    payment_date,
    payment_amount_rub,
    cashflow_number,
    finance_position,
    finance_position_text,
    kurator,
    direction,
    link_cashflow_number,
    bpartner,
    employee,
    currency,
    author,
    be,
    be_name,
    zdognr,
    ac_doc_typ,
    status,
    doc_text,
    teamlead,
    orgunit_1,
    orgunit_2,
    orgunit_3,
    orgunit_4,
    orgunit_5,
    orgunit_6,
    link_cashflow_create_date,
    link_direction,
    link_rub,
    link_flag
FROM DATASET;


TRUNCATE TABLE TEST_SCHEMA.CP_EMPLOYEE_ERRORS;
INSERT INTO TEST_SCHEMA.CP_EMPLOYEE_ERRORS (
    financial_position,
    financial_position_text,
    business_partner_id,
    business_partner_name,
    payment_amount_rub,
    transaction_amount_currency,
    transaction_currency,
    payment_base_date,
    payment_base_date_1,
    payment_base_date_7,
    payment_base_date_14,
    expense_application_id,
    vgo,
    application_sum,
    application_sum_1,
    application_sum_7,
    application_sum_14,
    create_date,
    change_date,
    curator,
    author,
    be,
    be_text,
    status,
    document_type,
    contract_number,
    responsible_department,
    manager,
    orgunit_4_level,
    orgunit_5_level,
    orgunit_6_level,
    orgunit_7_level,
    orgunit_8_level,
    orgunit_9_level,
    flag_link_application,
    link_application_number,
    link_application_create_date,
    link_application_sum,
    application_create_date_calculate,
    link_application_department,
    days_delta,
    coeff


)
WITH ACTUAL_DATASET as (
    SELECT
        create_date,
        change_date,
        payment_date,
        payment_amount_rub,
        cashflow_number,
        finance_position,
        finance_position_text,
        kurator,
        direction,
        link_cashflow_number,
        bpartner,
        employee,
        currency,
        author,
        be,
        be_name,
        zdognr,
        status,
        doc_text,
        teamlead,
        orgunit_1,
        orgunit_2,
        orgunit_3,
        orgunit_4,
        orgunit_5,
        orgunit_6,
        link_cashflow_create_date,
        link_direction,
        link_rub,
        link_flag
    FROM CV_FM_00013_COPY_DATASET
    WHERE link_flag IS NULL or (link_flag IS NOT NULL AND status = 'Удалена')
    LIMIT 1 OVER (PARTITION BY cashflow_number, finance_position, bpartner ORDER BY change_date DESC)
),
CMMT_ITEM AS (
    SELECT
        fm_area,
        cmmt_item,
        langu,
        dateto,
        txtmd
    FROM (
        SELECT
            fm_area,
            cmmt_item,
            langu,
            dateto,
            txtmd,
            tech_is_deleted
        FROM ODS_SAPBW._BI0_TCMMT_ITEM
        WHERE langu = 'R'
        LIMIT 1 OVER (PARTITION BY fm_area, cmmt_item, langu, dateto ORDER BY tech_load_ts DESC)
    ) AS t
    WHERE tech_is_deleted is false
        AND dateto = '9999-12-31'
        AND LENGTH(txtmd) > 0
),
CASHFLOW AS (
    SELECT
        cc.financial_position,
        ci.txtmd AS financial_position_text,
        cc.business_partner_id AS business_partner,
        cc.business_partner_name AS business_partner_name,
        SUM(cc.payment_amount_rub) AS payment_amount_rub,
        SUM(cc.transaction_amount_currency) AS transaction_amount,
        cc.transaction_currency AS transaction_amount_currency,
        cc.payment_base_date,
        (cc.payment_base_date - interval '1 day')::date AS payment_base_date_1,
        (cc.payment_base_date - interval '7 day')::date AS payment_base_date_7,
        (cc.payment_base_date - interval '14 day')::date AS payment_base_date_14,
        NULLIF(cc.expense_application_id, '')::int AS funds_allocation,
        cc.be AS business_sector,
        cc.be_text AS business_sector_text
    FROM TEST_SCHEMA.CP_CASHFLOW AS cc
        LEFT JOIN CMMT_ITEM AS ci ON cc.be = ci.fm_area AND cc.financial_position = ci.cmmt_item
    WHERE SUBSTR(cc.cashflow_item, 2, 1) != 1
        AND cc.cashflow_item NOT ILIKE '%T%'
        AND cc.payment_base_date >= '2024-10-01'
        AND NULLIF(cc.expense_application_id, '') IS NOT NULL
    GROUP BY
        cc.financial_position,
        ci.txtmd,
        cc.business_partner_id,
        cc.business_partner_name,
        cc.transaction_currency,
        cc.payment_base_date,
        cc.expense_application_id,
        cc.be,
        cc.be_text
    HAVING SUM(payment_amount_rub) != 0
),
APP_EXPENSE_FUNDS AS (
    SELECT
        m.finance_position,
        m.finance_position_text,
        m.cashflow_number,
        MAX(m.create_date) AS create_date,
        MAX(m.change_date) AS change_date,
        m.payment_date AS payment_date,
        m.payment_amount_rub AS payment_amount,
        m.bpartner,
        m.employee,
        m.kurator,
        m.author,
        m.be,
        m.be_name,
        m.zdognr,
        m.status,
        m.doc_text,
        m.direction,
        m.teamlead,
        m.orgunit_1,
        m.orgunit_2,
        m.orgunit_3,
        m.orgunit_4,
        m.orgunit_5,
        m.orgunit_6,
        m.link_cashflow_number,
        m.link_cashflow_create_date,
        m.link_direction,
        m.link_rub,
        m.link_flag
    FROM CASHFLOW AS CF
        INNER JOIN ACTUAL_DATASET AS m ON
            m.cashflow_number = CF.funds_allocation
            AND CF.financial_position = m.finance_position
            AND CF.business_partner = m.bpartner
    GROUP BY
        m.finance_position,
        m.finance_position_text,
        m.cashflow_number,
        m.bpartner,
        m.employee,
        m.payment_date,
        m.payment_amount_rub,
        m.kurator,
        m.author,
        m.be,
        m.be_name,
        m.zdognr,
        m.status,
        m.doc_text,
        m.direction,
        m.teamlead,
        m.orgunit_1,
        m.orgunit_2,
        m.orgunit_3,
        m.orgunit_4,
        m.orgunit_5,
        m.orgunit_6,
        m.link_cashflow_number,
        m.link_cashflow_create_date,
        m.link_direction,
        m.link_rub,
        m.link_flag
),
AGG_COPY_DATASET AS (
    SELECT
        m.finance_position,
        m.cashflow_number,
        m.bpartner,
        m.create_date,
        m.change_date AS change_date,
        m.payment_date AS payment_date,
        SUM(m.payment_amount_rub) AS payment_amount
    FROM CV_FM_00013_COPY_DATASET AS m
    GROUP BY
        m.finance_position,
        m.cashflow_number,
        m.bpartner,
        m.create_date,
        m.change_date,
        m.payment_date
),
DATES_CALCULATE AS (
    SELECT
        m.finance_position,
        m.cashflow_number,
        m.bpartner,
        m.payment_date,
        m.change_date,
        m.payment_amount,
        CASE WHEN ROW_NUMBER() OVER (
                    WIN
                    ORDER BY DECODE(TRUE, CF.payment_base_date_1 - m.change_date < 0, 99999, CF.payment_base_date_1 - m.change_date) asc
                ) = 1
                AND CF.payment_base_date_1 - m.change_date >= 0 then m.payment_amount
        end as payment_amount_1,
        CASE WHEN ROW_NUMBER() OVER (
                    WIN
                    ORDER BY DECODE(TRUE, CF.payment_base_date_7 - m.change_date < 0, 99999, CF.payment_base_date_7 - m.change_date) asc
                ) = 1
                AND CF.payment_base_date_7 - m.change_date >= 0 then m.payment_amount
        end as payment_amount_7,
        CASE WHEN ROW_NUMBER() OVER (
                    WIN
                    ORDER BY DECODE(TRUE, CF.payment_base_date_14 - m.change_date < 0, 99999, CF.payment_base_date_14 - m.change_date) asc
                ) = 1
                AND CF.payment_base_date_14 - m.change_date >= 0 then m.payment_amount
        end as payment_amount_14
    FROM AGG_COPY_DATASET AS m
        INNER JOIN CASHFLOW AS CF ON
            m.cashflow_number = CF.funds_allocation
            AND m.finance_position = CF.financial_position
            AND m.change_date <= CF.payment_base_date
            AND m.bpartner = CF.business_partner
    WINDOW WIN AS (PARTITION BY m.finance_position, m.cashflow_number, m.bpartner)
),
GUIDE_DATES_PAYMENT AS (
    SELECT
        finance_position,
        cashflow_number,
        bpartner,
        MAX(payment_date) as payment_date,
        MAX(payment_amount_1) as payment_amount_1,
        MAX(payment_amount_7) as payment_amount_7,
        MAX(payment_amount_14) as payment_amount_14
    FROM DATES_CALCULATE
    GROUP BY
        finance_position,
        cashflow_number,
        bpartner
),
VGO AS (
    SELECT
        bpartner,
        vgo,
        datefrom,
        dateto
    FROM (
        SELECT
            bpartner,
            datefrom,
            dateto,
            _bic_zgnch034 AS vgo,
            tech_is_deleted
        FROM ODS_SAPBW._BI0_MBPARTNER
        LIMIT 1 OVER (PARTITION BY bpartner, objvers, dateto ORDER BY tech_load_ts DESC)
    ) AS t
    WHERE tech_is_deleted is false
        AND LENGTH(vgo) > 0
),
CASHFLOW_APP_EXPENSE AS (
    SELECT
        cf.financial_position,
        cf.financial_position_text,
        cf.business_partner,
        cf.business_partner_name,
        cf.payment_amount_rub,
        cf.transaction_amount,
        cf.transaction_amount_currency,
        cf.payment_base_date,
        cf.payment_base_date_1,
        cf.payment_base_date_7,
        cf.payment_base_date_14,
        cf.funds_allocation,
        cf.business_sector,
        cf.business_sector_text,
        vgo.vgo,
        GDP.payment_amount_1,
        GDP.payment_amount_7,
        GDP.payment_amount_14,
        aef.create_date,
        aef.change_date,
        aef.payment_amount,
        aef.bpartner,
        aef.employee,
        aef.author,
        aef.be,
        aef.be_name,
        aef.zdognr,
        aef.status,
        aef.doc_text,
        aef.kurator,
        aef.direction,
        aef.teamlead,
        aef.orgunit_1,
        aef.orgunit_2,
        aef.orgunit_3,
        aef.orgunit_4,
        aef.orgunit_5,
        aef.orgunit_6,
        aef.link_cashflow_number,
        aef.link_cashflow_create_date,
        aef.link_rub,
        aef.cashflow_number,
        aef.finance_position,
        aef.link_direction,
        ABS(DATEDIFF('day', cf.payment_base_date, COALESCE(aef.link_cashflow_create_date, aef.create_date))) as days_deltas
    FROM CASHFLOW AS cf
        LEFT JOIN APP_EXPENSE_FUNDS AS aef
            ON
                cf.funds_allocation = aef.cashflow_number
                AND cf.financial_position = aef.finance_position
                AND cf.business_partner = aef.bpartner
        LEFT JOIN GUIDE_DATES_PAYMENT AS GDP
            ON
                cf.funds_allocation = GDP.cashflow_number
                AND cf.financial_position = GDP.finance_position
                AND cf.business_partner = GDP.bpartner
        LEFT JOIN VGO ON cf.business_partner = vgo.bpartner and cf.payment_base_date between vgo.datefrom and vgo.dateto
),
BASE_DATES_CALCULATE as (
    SELECT
        m.finance_position,
        m.cashflow_number,
        m.bpartner,
        m.create_date,
        m.change_date,
        m.payment_date,
        m.payment_amount,
        CASE WHEN ROW_NUMBER()
                    OVER (WIN ORDER BY DECODE(TRUE, m.change_date - CAE.payment_base_date_1 <= 0, 99999, m.change_date - CAE.payment_base_date_1) asc)
                = 1
                AND CAE.payment_base_date_1 < m.change_date
                AND CAE.payment_base_date_1 >= m.create_date then m.payment_amount
        end as payment_amount_1,
        CASE WHEN ROW_NUMBER()
                    OVER (WIN ORDER BY DECODE(TRUE, m.change_date - CAE.payment_base_date_7 <= 0, 99999, m.change_date - CAE.payment_base_date_1) asc)
                = 1
                AND CAE.payment_base_date_7 < m.change_date
                AND CAE.payment_base_date_7 >= m.create_date then m.payment_amount
        end as payment_amount_7,
        CASE WHEN ROW_NUMBER()
                    OVER (
                        WIN ORDER BY DECODE(TRUE, m.change_date - CAE.payment_base_date_14 <= 0, 99999, m.change_date - CAE.payment_base_date_1) asc
                    )
                = 1
                AND CAE.payment_base_date_14 < m.change_date
                AND CAE.payment_base_date_14 >= m.create_date then m.payment_amount
        end as payment_amount_14
    FROM CASHFLOW_APP_EXPENSE AS CAE
        INNER JOIN AGG_COPY_DATASET AS m
            ON
                CAE.funds_allocation = m.cashflow_number
                AND CAE.financial_position = m.finance_position
                AND CAE.business_partner = m.bpartner
    WINDOW WIN AS (PARTITION BY m.finance_position, m.cashflow_number, m.bpartner)
),
GUIDE_DATES_PAYMENT_BASE AS (
    SELECT
        finance_position,
        cashflow_number,
        bpartner,
        MAX(payment_date) as payment_date,
        MAX(payment_amount_1) as payment_amount_1,
        MAX(payment_amount_7) as payment_amount_7,
        MAX(payment_amount_14) as payment_amount_14
    FROM BASE_DATES_CALCULATE
    GROUP BY
        finance_position,
        cashflow_number,
        bpartner
),
ONLY_LINK AS (
    SELECT
        cashflow_number,
        link_cashflow_number,
        CREATE_DATE
    FROM CASHFLOW_APP_EXPENSE AS cf_all
    WHERE link_cashflow_number IS NOT NULL
    LIMIT 1 OVER (PARTITION BY link_cashflow_number ORDER BY create_date asc)
),
APP_EXPENSE_NOT_NULL AS (
    SELECT
        app.finance_position AS financial_position,
        app.finance_position_text AS financial_position_text,
        app.currency,
        app.payment_date,
        app.cashflow_number,
        app.create_date AS create_date,
        app.change_date AS change_date,
        app.bpartner,
        app.employee,
        app.author,
        app.be,
        app.be_name,
        app.zdognr,
        app.status,
        app.doc_text,
        app.kurator,
        app.direction,
        app.teamlead,
        app.orgunit_1,
        app.orgunit_2,
        app.orgunit_3,
        app.orgunit_4,
        app.orgunit_5,
        app.orgunit_6,
        app.cashflow_number AS link_cashflow_number,
        app.link_cashflow_create_date,
        SUM(app.payment_amount_rub) AS link_rub,
        app.link_direction,
        app.create_date AS date_input,
        only_link.create_date AS sub_create_date
    FROM ACTUAL_DATASET AS app
        INNER JOIN ONLY_LINK
            ON app.cashflow_number = only_link.link_cashflow_number
    GROUP BY
        app.finance_position,
        app.finance_position_text,
        app.payment_date,
        app.create_date,
        app.change_date,
        app.bpartner,
        app.employee,
        app.currency,
        app.author,
        app.be,
        app.be_name,
        app.zdognr,
        app.status,
        app.doc_text,
        app.kurator,
        app.direction,
        app.teamlead,
        app.orgunit_1,
        app.orgunit_2,
        app.orgunit_3,
        app.orgunit_4,
        app.orgunit_5,
        app.orgunit_6,
        app.link_flag,
        app.cashflow_number,
        app.link_cashflow_create_date,
        app.link_direction,
        app.create_date,
        only_link.create_date
)
SELECT
    financial_position,
    financial_position_text,
    business_partner,
    business_partner_name,
    payment_amount_rub,
    transaction_amount,
    transaction_amount_currency,
    payment_base_date,
    payment_base_date_1,
    payment_base_date_7,
    payment_base_date_14,
    funds_allocation,
    vgo,
    payment_amount,
    payment_amount_1,
    payment_amount_7,
    payment_amount_14,
    create_date,
    change_date,
    kurator,
    author,
    be,
    be_name,
    status,
    doc_text,
    zdognr,
    direction,
    teamlead,
    orgunit_1,
    orgunit_2,
    orgunit_3,
    orgunit_4,
    orgunit_5,
    orgunit_6,
    link_flag,
    link_cashflow_number,
    link_cashflow_create_date,
    link_rub,
    application_create_date_calculate,
    link_direction,
    days_deltas,
    coeff
FROM (
    SELECT distinct
        cae.financial_position,
        cae.financial_position_text,
        cae.business_partner,
        cae.business_partner_name,
        cae.payment_amount_rub,
        cae.transaction_amount,
        cae.transaction_amount_currency,
        cae.payment_base_date,
        cae.payment_base_date_1,
        cae.payment_base_date_7,
        cae.payment_base_date_14,
        cae.funds_allocation,
        cae.vgo,
        cae.payment_amount,
        COALESCE(cae.payment_amount_1, ba.payment_amount_1) AS payment_amount_1,
        COALESCE(cae.payment_amount_7, ba.payment_amount_7) AS payment_amount_7,
        COALESCE(cae.payment_amount_14, ba.payment_amount_14) AS payment_amount_14,
        cae.create_date,
        cae.change_date,
        cae.kurator,
        cae.author,
        COALESCE(cae.business_sector, cae.be) AS be,
        COALESCE(cae.business_sector_text, cae.be_name) AS be_name,
        cae.status,
        cae.doc_text,
        cae.zdognr,
        cae.direction,
        cae.teamlead,
        cae.orgunit_1,
        cae.orgunit_2,
        cae.orgunit_3,
        cae.orgunit_4,
        cae.orgunit_5,
        cae.orgunit_6,
        'нет' AS link_flag,
        cae.link_cashflow_number,
        cae.link_cashflow_create_date,
        cae.link_rub,
        COALESCE(cae.link_cashflow_create_date, cae.CREATE_DATE) AS application_create_date_calculate,
        cae.link_direction,
        cae.days_deltas,
        DECODE(
            cae.days_deltas,
            0, 0,
            1, 0.07143,
            2, 0.14286,
            3, 0.21429,
            4, 0.28571,
            5, 0.35714,
            6, 0.42857,
            7, 0.50000,
            8, 0.57143,
            9, 0.64286,
            10, 0.71429,
            11, 0.78571,
            12, 0.85714,
            13, 0.92857,
            1
        ) AS coeff
    FROM CASHFLOW_APP_EXPENSE AS cae
        LEFT JOIN GUIDE_DATES_PAYMENT_BASE AS ba
            ON
                cae.finance_position = ba.finance_position
                AND cae.bpartner = ba.bpartner
                AND cae.cashflow_number = ba.cashflow_number

    UNION ALL

    SELECT
        financial_position,
        financial_position_text,
        '00000000' AS business_partner,
        null AS business_partner_name,
        null AS payment_amount_rub,
        null AS transaction_amount,
        currency AS transaction_amount_currency,
        payment_date AS payment_base_date,
        null AS payment_base_date_1,
        null AS payment_base_date_7,
        null AS payment_base_date_14,
        cashflow_number AS funds_allocation,
        null AS vgo,
        null AS payment_amount,
        CASE
            WHEN
                status = 'Удалена'
                AND payment_date <= change_date
                AND (payment_date - interval '1 day' BETWEEN date_input AND LEAST(
                    (sub_create_date - interval '1 day'),
                    change_date - interval '1 day'
                )
                OR payment_date - interval '1 day' BETWEEN sub_create_date - interval '1 day' AND LEAST(
                    (sub_create_date - interval '1 day'), change_date - interval '1 day'
                ))
                THEN link_rub
            WHEN
                status = 'Удалена'
                AND payment_date > change_date
                AND payment_date - interval '1 day' BETWEEN create_date AND change_date - interval '1 day'
                THEN link_rub
        end
        AS payment_amount_1,
        CASE
            WHEN
                status = 'Удалена'
                AND payment_date <= change_date
                AND (payment_date - interval '7 day' BETWEEN date_input AND LEAST(
                    (sub_create_date - interval '1 day'), change_date - interval '1 day'
                )
                OR payment_date - interval '7 day' BETWEEN sub_create_date - interval '1 day' AND LEAST(
                    (sub_create_date - interval '1 day'), change_date - interval '1 day'
                ))
                THEN link_rub
            WHEN
                status = 'Удалена'
                AND payment_date > change_date
                AND payment_date - interval '7 day' BETWEEN create_date AND change_date - interval '1 day'
                THEN link_rub
        end
        as
        payment_amount_7,
        CASE
            WHEN
                status = 'Удалена'
                AND payment_date <= change_date
                AND (payment_date - interval '14 day' BETWEEN date_input AND LEAST(
                    (sub_create_date - interval '1 day'), change_date - interval '1 day'
                )
                OR payment_date - interval '14 day' BETWEEN sub_create_date - interval '1 day' AND LEAST(
                    (sub_create_date - interval '1 day'), change_date - interval '1 day'
                ))
                THEN link_rub
            WHEN
                status = 'Удалена'
                AND payment_date > change_date
                AND payment_date - interval '14 day' BETWEEN create_date AND change_date - interval '1 day'
                THEN link_rub
        end
        as
        payment_amount_14,
        create_date AS create_date,
        change_date AS change_date,
        kurator,
        author,
        be,
        be_name,
        status,
        doc_text,
        zdognr,
        direction,
        teamlead,
        orgunit_1,
        orgunit_2,
        orgunit_3,
        orgunit_4,
        orgunit_5,
        orgunit_6,
        'да' AS link_flag,
        link_cashflow_number,
        link_cashflow_create_date,
        link_rub,
        COALESCE(link_cashflow_create_date, create_date) AS application_create_date_calculate,
        direction,
        ABS(DATEDIFF('day', payment_date, COALESCE(link_cashflow_create_date, create_date))) AS days_deltas,
        null AS coeff
    FROM APP_EXPENSE_NOT_NULL
) AS final_dataset
WHERE NOT (payment_amount_1 IS NULL AND payment_amount_7 IS NULL AND payment_amount_14 IS NULL);
