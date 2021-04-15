/* по каждому дню последних двух недель считаем количество новых пользователей
   - Date
   - Number of new users
 */
------------------------------------1-й способ-------------------------------------------
with first_visits as (
    select user_id, min(event_timestamp) as first_visit
    from user_events
    group by user_id
)
select
       event_timestamp::date as Date,
       count(fv.user_id) as Number_of_new_users
from user_events ue
inner join
    first_visits fv on ue.user_id = fv.user_id and ue.event_timestamp = fv.first_visit
where
      event_timestamp between current_date - interval '2 week' and current_date
group by Date
order by Date;
------------------------------------2--й способ---------------------------------------------
with first_visits as (
    select
           user_id,
           event_timestamp,
           min(event_timestamp)
               over (partition by user_id order by event_timestamp) as first_visit
    from user_events
)
select
       event_timestamp::date as Date,
       count(user_id) as Number_of_new_users
from first_visits
where
      event_timestamp between current_date - interval '2 week' and current_date
  and event_timestamp = first_visit
group by Date;