/* по каждому дню последних двух недель считаем количество вернувшихся пользователей
   - Date
   - Number of returning users
 */
--------------------------------------------------------------------------------------
with first_visits as (
    select
           user_id,
           min(event_timestamp::date) as first_visit
    from user_events
    group by user_id
)
select
       event_timestamp::date as Date,
       count(distinct ue.user_id) as Number_of_returning_users
from user_events ue
inner join
    first_visits fv on ue.user_id = fv.user_id
where
      event_timestamp between current_date - interval '2 week' and current_date
and ue.event_timestamp::date <> fv.first_visit
group by Date
order by Date;
