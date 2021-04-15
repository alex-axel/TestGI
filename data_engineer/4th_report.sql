/* по каждому месяцу текущего года считаем средний чек на платящего игрока
   - Month
   - ARPPU
 */
--------------------------------------------------------------------------------------
select
       extract(month from event_timestamp) as Month,
       trunc(case
           when count(distinct user_id) <> 0
               then sum(amount::float) / count(distinct user_id)
           else 0
       end::numeric, 2) as ARPPU
from user_events
where
      extract(year from event_timestamp) = extract(year from current_date)
and amount is not null
group by Month
order by Month;
