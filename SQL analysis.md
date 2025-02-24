Description</span> <span style="color:#32CD32; font-family:'Arial', sans-serif;">Опис SQL-запитів</span>

---

## <span style="color:#000080; font-family:'Georgia', serif;">1. This SQL query aggregates e-commerce data from BigQuery to analyze account creation and email activity.</span>

### <span style="color:#8B0000; font-family:'Georgia', serif;">1. Цей SQL-запит агрегує дані електронної комерції з BigQuery для аналізу створення акаунтів та активності електронної пошти.</span>

> Він обчислює ключові показники (кількість акаунтів, надісланих, відкритих і переходів) за такими параметрами, як дата, країна, інтервал між відправленнями, верифікація і статус підписки. Використовуючи кілька CTE та UNION, він обчислює загальні показники та рейтинги на рівні країни, фільтруючи до 10 найкращих країн. Отриманий набір даних слугує основою для візуалізацій Looker Studio для порівняння поведінки користувачів та визначення ключових ринків.

#### <span style="color:#2E8B57; font-family:'Verdana', sans-serif;">SQL Query for E-commerce Data Analysis</span>

```sql
-- Основний SQL-запит для аналізу даних про акаунти та email-активність.
WITH account_details AS (
    -- CTE: Рахуємо кількість створених акаунтів (account_cnt) у розрізі:
    -- дати створення акаунта, країни, інтервалу відправлення, верифікації та підписки.
    SELECT
        s.date AS date,                          -- Дата створення акаунта.
        sp.country,                              -- Країна користувача.
        a.send_interval,                         -- Інтервал відправлення, встановлений акаунтом.
        a.is_verified,                           -- Прапорець: акаунт верифіковано (TRUE(1)/FALSE(0)).
        a.is_unsubscribed,                       -- Прапорець: користувач відписався (TRUE(1)/FALSE(0)).
        COUNT(DISTINCT a.id) AS account_cnt       -- Кількість унікальних створених акаунтів.
    FROM `DA.session` s
    JOIN `DA.account_session` acs ON acs.ga_session_id = s.ga_session_id
    JOIN `DA.account` a ON acs.account_id = a.id
    JOIN `DA.session_params` sp ON sp.ga_session_id = s.ga_session_id
    GROUP BY s.date, sp.country, a.send_interval, a.is_verified, a.is_unsubscribed
),
email_details AS (
    -- CTE: Рахуємо основні метрики email-активності:
    -- кількість відправлених (sent_msg), відкритих (open_msg) та переходів по email (visit_msg) 
    -- у розрізі дати відправлення email, країни, інтервалу відправлення, верифікації та підписки.
    SELECT
        DATE_ADD(s.date, INTERVAL es.sent_date DAY) AS date, -- Обчислення дати з урахуванням інтервалу відправлення.
        sp.country,                            -- Країна користувача.
        a.send_interval,                       -- Інтервал відправлення, встановлений акаунтом.
        a.is_verified,                         -- Прапорець: акаунт верифіковано (TRUE(1)/FALSE(0)).
        a.is_unsubscribed,                     -- Прапорець: користувач відписався (TRUE(1)/FALSE(0)).
        COUNT(DISTINCT es.id_message) AS sent_msg,          -- Унікальна кількість відправлених email.
        COUNT(DISTINCT eo.id_message) AS open_msg,          -- Унікальна кількість відкритих email.
        COUNT(DISTINCT ev.id_message) AS visit_msg          -- Унікальна кількість переходів по email.
    FROM `DA.email_sent` es
    LEFT JOIN `DA.email_open` eo ON es.id_message = eo.id_message
    LEFT JOIN `DA.email_visit` ev ON es.id_message = ev.id_message
    JOIN `DA.account` a ON es.id_account = a.id
    JOIN `DA.account_session` acs ON acs.account_id = a.id
    JOIN `DA.session` s ON acs.ga_session_id = s.ga_session_id
    JOIN `DA.session_params` sp ON sp.ga_session_id = s.ga_session_id
    GROUP BY date, sp.country, a.send_interval, a.is_verified, a.is_unsubscribed
),
combined_data AS (
    -- CTE: Об’єднуємо дані з account_details та email_details в одному наборі даних.
    SELECT
        date,
        country,
        send_interval,
        is_verified,
        is_unsubscribed,
        account_cnt,
        NULL AS sent_msg,
        NULL AS open_msg,
        NULL AS visit_msg
    FROM account_details
    UNION ALL
    SELECT
        date,
        country,
        send_interval,
        is_verified,
        is_unsubscribed,
        NULL AS account_cnt,
        sent_msg,
        open_msg,
        visit_msg
    FROM email_details
),
union_all AS (
    -- CTE: Об’єднуємо та агрегуємо дані з combined_data.
    SELECT
        date,
        country,
        send_interval,
        is_verified,
        is_unsubscribed,
        SUM(account_cnt) AS account_cnt,     -- Сумарна кількість акаунтів (без NULL значень).
        SUM(sent_msg) AS sent_msg,           -- Сумарна кількість відправлених email (без NULL значень).
        SUM(open_msg) AS open_msg,           -- Сумарна кількість відкритих email (без NULL значень).
        SUM(visit_msg) AS visit_msg          -- Сумарна кількість переходів по email (без NULL значень).
    FROM combined_data
    GROUP BY date, country, send_interval, is_verified, is_unsubscribed
),
account_metrics AS (
    -- CTE: Додаємо до даних із union_all додаткову метрику:
    -- загальну кількість акаунтів для кожної країни (total_country_account_cnt).
    SELECT *,
        SUM(account_cnt) OVER (PARTITION BY country) AS total_country_account_cnt -- Загальна кількість акаунтів по країні.
    FROM union_all
),
account_ranks AS (
    -- CTE: Додаємо рейтинг країн (rank_total_country_account_cnt) за кількістю акаунтів.
    SELECT *,
        DENSE_RANK() OVER (ORDER BY total_country_account_cnt DESC) AS rank_total_country_account_cnt -- Рейтинг країн.
    FROM account_metrics
),
email_metrics AS (
    -- CTE: Додаємо до даних із account_ranks додаткову метрику:
    -- загальну кількість відправлених email для кожної країни (total_country_sent_cnt).
    SELECT *,
        SUM(sent_msg) OVER (PARTITION BY country) AS total_country_sent_cnt -- Загальна кількість відправлених email по країні.
    FROM account_ranks
),
email_ranks AS (
    -- CTE: Додаємо рейтинг країн (rank_total_country_sent_cnt) за кількістю відправлених email.
    SELECT *,
        DENSE_RANK() OVER (ORDER BY total_country_sent_cnt DESC) AS rank_total_country_sent_cnt -- Рейтинг країн.
    FROM email_metrics
)
-- Фінальний запит: Фільтруємо дані для топ-10 країн за створеними акаунтами або відправленими email
-- та сортуємо за датою та країною.
SELECT *
FROM email_ranks
WHERE rank_total_country_account_cnt <= 10 OR rank_total_country_sent_cnt <= 10
ORDER BY date, country;
```

---


## <span style="color:#000080; font-family:'Georgia', serif;">2. SQL Query for Session Engagement Ratio.</span>

### <span style="color:#8B0000; font-family:'Georgia', serif;">1. Цей SQL-запит агрегує дані електронної комерції з BigQuery для аналізу створення акаунтів та активності електронної пошти.</span>

> Розраховую долю івентів, в яких є відмітка <code>session_engaged = 1</code> від усіх івентів, де є значення в цьому полі (відмінне від <code>NULL</code>). Вивожу інформацію в розрізі пристроїв</span>

#### <span style="color:#2E8B57; font-family:'Verdana', sans-serif;">SQL Query for E-commerce Data Analysis</span>
```sql
-- Вибираємо тип пристрою та розраховуємо долю сесій, де session_engaged = '1'
SELECT
    sp.device AS device,  -- Тип пристрою
    -- Розрахунок долі: сума подій з session_engaged = '1' поділена на загальну кількість подій
    ROUND(
        SUM(CASE WHEN ep.value.string_value = '1' THEN 1 ELSE 0 END) / COUNT(*),
        5
    ) AS session_ratio  -- Доля сесій з engaged (округлено до 5 знаків)
FROM
    `DA.event_params` p,
    UNNEST(p.event_params) AS ep  -- Розпаковуємо масив event_params
JOIN 
    `DA.session_params` sp ON sp.ga_session_id = p.ga_session_id  -- З'єднуємо з інформацією сесій
WHERE
    ep.key = 'session_engaged'           -- Фільтруємо за ключем session_engaged
    AND ep.value.string_value IS NOT NULL -- Враховуємо лише записи, де значення не NULL
GROUP BY 
    sp.device  -- Групуємо результати за пристроєм
ORDER BY 
    session_ratio DESC;  -- Сортуємо за спаданням долі сесій
```

---

<span style="color:#1E90FF; font-family:'Arial', sans-serif;">3. SQL Query Description</span> <span style="color:#32CD32; font-family:'Arial', sans-serif;">Опис SQL-запиту</span>
<span style="color:#000080; font-family:'Georgia', serif;">Рахую відсоток витрат по місяцях від загальних витрат за весь період</span>
<span style="color:#8B0000; font-family:'Georgia', serif;"> Використовую window функцію для обчислення відсотків. Представляю результат у форматі, де для кожного місяця відображається відсоток від загальних витрат. </span>
<span style="color:#2E8B57; font-family:'Verdana', sans-serif;">SQL Query for Expense Percentage by Month</span>
```sql
-- Основний SQL-запит для обчислення відсотку витрат по місяцях від загальних витрат.
SELECT
    sales_month,                  -- Місяць (формат: 'YYYY-MM')
    cost_by_month,                -- Загальна сума витрат за місяць
    -- Обчислення відсотку: витрати за місяць / загальна сума витрат * 100
    cost_by_month / SUM(cost_by_month) OVER () * 100 AS percentage_of_total_expenses
FROM (
    SELECT
        FORMAT_TIMESTAMP('%Y-%m', date) AS sales_month,  -- Форматуємо дату до формату 'YYYY-MM'
        SUM(cost) AS cost_by_month                        -- Обчислюємо загальну суму витрат за місяць
    FROM `DA.paid_search_cost`
    GROUP BY sales_month                                  -- Групуємо дані за місяцем
) AS montly_cost
ORDER BY 
    sales_month;  -- Сортуємо результати за місяцем
```

---

<span style="color:#1E90FF; font-family:'Arial', sans-serif;">4. SQL Query Description</span> <span style="color:#32CD32; font-family:'Arial', sans-serif;">Опис SQL-запиту</span>
<span style="color:#000080; font-family:'Georgia', serif;">Рахую кількість івентів типу <code>user_engagement</code> тільки для тих сесій, в яких загальна кількість івентів перевищує 2.</span>
<span style="color:#8B0000; font-family:'Georgia', serif;"> Використовується підзапит для визначення сесій з більш ніж двома івентами, після чого здійснюється підрахунок івентів <code>user_engagement</code> для цих сесій. </span>
<span style="color:#2E8B57; font-family:'Verdana', sans-serif;">SQL Query for User Engagement Event Count</span>

```sql
-- Основний запит: підрахунок кількості івентів типу 'user_engagement'
SELECT 
    COUNT(*) AS user_engagement_events  -- Рахуємо кількість івентів user_engagement
FROM 
    data-analytics-mate.DA.event_params evp
JOIN 
    (
        -- Підзапит: відбір сесій, де загальна кількість івентів більше 2
        SELECT 
            ga_session_id, 
            COUNT(event_name) AS event_cnt  -- Рахуємо кількість івентів для кожної сесії
        FROM 
            data-analytics-mate.DA.event_params
        GROUP BY 
            ga_session_id
        HAVING 
            COUNT(event_name) > 2  -- Фільтруємо сесії з більше ніж 2 івентами
    ) events 
    ON evp.ga_session_id = events.ga_session_id 
    AND evp.event_name = 'user_engagement';  -- Обираємо лише івенти типу user_engagement
```    
