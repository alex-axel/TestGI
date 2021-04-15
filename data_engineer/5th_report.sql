/* по каждому дню последних двух недель считаем процент пользователей,
   воспользовавшихся приложением на 3-й и 7-й день, считая со дня своей
   регистрации
   - Date
   - 3rd day retention
   - 7th day retention
 */
--------------------------------------------------------------------------------------
with first_visits as (
    select distinct user_id,
                    event_timestamp::date                                      as Date,
                    min(event_timestamp::date)
                    over (partition by user_id order by event_timestamp::date) as First_visit
    from user_events
    order by event_timestamp::date
    ),
    new_users as (
         select Date,
                count(user_id) as Number_of_new_users
         from first_visits
         where Date = First_visit
         group by Date
    ),
    new_users_days_ago as (
         select Date,
                lag(Number_of_new_users, 3) over (order by Date) as Number_of_new_users_3days_ago,
                lag(Number_of_new_users, 7) over (order by Date) as Number_of_new_users_7days_ago
         from new_users
    )
select fv.Date,
       concat(trunc(case
                         when Number_of_new_users_7days_ago <> 0
                             then
                                 count(user_id) filter (where fv.Date - First_visit = 3)
                                 / Number_of_new_users_7days_ago::float
                         else 0 end::numeric * 100, 2), '%') as Third_day_retention,
       concat(trunc(case
                         when Number_of_new_users_7days_ago <> 0
                             then
                                 count(user_id) filter (where fv.Date - First_visit = 7)
                                 / Number_of_new_users_7days_ago::float
                         else 0 end::numeric * 100, 2), '%')
                                                              as Seventh_day_retention
from first_visits fv
         inner join new_users_days_ago nu
                    on fv.Date = nu.Date
where fv.Date between current_date - interval '2 week' and current_date
group by fv.Date, Number_of_new_users_3days_ago, Number_of_new_users_7days_ago
order by fv.Date