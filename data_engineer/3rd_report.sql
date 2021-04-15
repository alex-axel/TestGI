/* по каждой неделе текущего года считаем количество активных пользователей
   - Number of week
   - Number of active users
 */
--------------------------------------------------------------------------------------
select
       extract(week from event_timestamp) as Number_of_week,
       count(distinct user_id) as Number_of_active_users
from user_events
where
      extract(year from event_timestamp) = extract(year from current_date)
group by Number_of_week
order by Number_of_week;
