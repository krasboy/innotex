

/* 
 *не должно быть незаполненных ключей клиента, ожидаемое значение 0
 */
with d_p as
(
 select mdm_customer_rk,
  case 
    when (mdm_customer_rk is null)  then 1
    else 0
  end as count_null
 from dfct_phone
)
select sum(count_null) result from d_p;

/* 
 *не должно быть ключей клиента содержащих -1, ожидаемое значение 0
 */
with d_p as
(
 select mdm_customer_rk,
  case 
    when (mdm_customer_rk = -1)  then 1
    else 0
  end as count_pk  
 from dfct_phone
)
select sum(count_pk) result from d_p;

/* 
 *не должно быть дублей по ключу (если составной, то по всем атрибутам) , ожидаемое значение 0
 */
 select
  mdm_customer_rk,
  phone_type_cd,
  business_start_dt,
  phone_num,
  count(*) as count
 from
  dfct_phone
 group by
  mdm_customer_rk,
  phone_type_cd,
  business_start_dt,
  phone_num
 having
  count(*)>1;
 
 /* 
 *  заполненность атрибута , ожидаемое значение 0
 */
  select count(1) result from dfct_phone where &attr_name is null;
--где &attr_name - название атрибута, например, business_start_dt

 /* 
 *  пересечения по истории , ожидаемое значение 0
 * сделано так длинно, что бы в случае получения значения отличного от нуля
 * можно было бы подняться на уровень запросов выше и увидеть типы пересеченния
 * количество пересечений демонстрирует корректно - количество реально
 */
 
 
with cte_outside as
(
 select c.mdm_customer_rk , c.business_start_dt, c.business_end_dt, c.phone_num 
  from dfct_phone c,dfct_phone d 
 where  c.mdm_customer_rk = d.mdm_customer_rk 
  and c.phone_num = d.phone_num
  and c.business_start_dt < d.business_start_dt 
  and d.business_end_dt  <= c.business_end_dt
),
cte_left as 
(
 select c.mdm_customer_rk , c.business_start_dt, c.business_end_dt, c.phone_num 
  from dfct_phone c,dfct_phone d 
 where  c.mdm_customer_rk = d.mdm_customer_rk 
  and c.phone_num = d.phone_num
  and  d.business_start_dt < c.business_start_dt 
  and   c.business_start_dt < d.business_end_dt 
  and  d.business_end_dt < c.business_end_dt 
),
cte_right as 
(
 select c.mdm_customer_rk , c.business_start_dt, c.business_end_dt, c.phone_num 
  from dfct_phone c,dfct_phone d 
 where  c.mdm_customer_rk = d.mdm_customer_rk 
  and c.phone_num = d.phone_num
  and  c.business_start_dt < d.business_start_dt 
  and   d.business_start_dt < c.business_end_dt 
  and  c.business_end_dt < d.business_end_dt  
),
cte_inside as
(
 select c.mdm_customer_rk , c.business_start_dt, c.business_end_dt, c.phone_num 
  from dfct_phone c,dfct_phone d 
 where  c.mdm_customer_rk = d.mdm_customer_rk 
  and c.phone_num = d.phone_num
  and d.business_start_dt < c.business_start_dt  
  and c.business_end_dt <= d.business_end_dt  
),
set_flags as
(
 select 
 case 
 when (mdm_customer_rk,business_start_dt,business_end_dt,phone_num) in (select * from cte_outside ) then true
 else false 
 end outside_flg,
 case 
 when (mdm_customer_rk,business_start_dt,business_end_dt,phone_num) in (select * from cte_inside ) then true
 else false 
 end inside_flg,
 case 
 when (mdm_customer_rk,business_start_dt,business_end_dt,phone_num) in (select * from cte_right ) then true
 else false 
 end right_num,
 case 
 when (mdm_customer_rk,business_start_dt,business_end_dt,phone_num) in (select * from cte_left ) then true
 else false 
 end left_flg
 from dfct_phone df
),
count_flags as
(
  select outside_flg,inside_flg,right_num,left_flg, count(*) as count  
    from set_flags
  group by(outside_flg,inside_flg,right_num,left_flg)
    having count(*)>0
    order by outside_flg,inside_flg,right_num,left_flg
)
select  
  case 
   when outside_flg or inside_flg or right_num or left_flg then 1
   else 0
  end result
  from count_flags;
 
/* -- проверка условия скорректированы мной, 
 * так как в текущей задаче нет требования что бы весрии заканчивались какой либо фиксированной датой.
 * 
 * дырки в истории , ожидаемое значение 0
 * проверяем, что нет разрывов среди версий:
 * 1. последовательность версий либо просто заканчиваются
 * 2. либо датой начала следующей версии совпадает с концом предыдущей 
 */ 
with lead_start as
(
select 
 case 
  when lead(business_start_dt,1) over (partition by mdm_customer_rk,phone_num order by business_start_dt ) = business_end_dt 
        or lead(business_start_dt,1) over (partition by mdm_customer_rk,phone_num order by business_start_dt ) is null
  then 0
  else 1
 end as result
from dfct_phone
)
select sum(result) as result from lead_start;

/* 
 * версии из будущего, ожидаемое значение 0
*/ 
with coun as
(
 select * from dfct_phone 
  where   business_start_dt>now()
)
select count(*) from coun;
/* 
 * здесь и ниже мои проверки
 * 
 * проверка на неправильные даты версии, дата начала больше даты конца, ожидаемое значение 0
*/ 
with coun as
(
 select * from dfct_phone 
  where   business_start_dt>business_end_dt
)
select count(*) from coun;

/* 
 * проверка на коррретность присвоения флага основного клиента, ожидаемое значение 0
 * 
 * проверка того, что флаг основного клиента для этого телефона только один,
 *  среди клиентов, имеющих этот номер в определенный период времени
*/ 
with coun as
(
 select 
  (
   select count(*) from dfct_phone df2_
    where df2_.phone_num = df_.phone_num and
    greatest(df2_.business_start_dt, df_.business_start_dt) < least(df2_.business_end_dt, df_.business_end_dt) and
    df2_.mdm_customer_rk != df_.mdm_customer_rk and 
    df2_.main_dup_flg
  ) count_error
 from dfct_phone df_ where main_dup_flg 
)
select sum(count_error) from coun; 

/* 
 * проверка на коррретность присвоения флага лучшего телефона для клиента, ожидаемое значение 0
 * в результате этой и нижеследующей проверки пришлось перестраивать логику отношений :)
 * 
 * проверка того, что флаг лучшего телефона для клиента  только один,
 *  среди телефонов, которые имеет клиет в определенный период времени
*/ 
with coun as
(
 select 
  (
   select count(*) from dfct_phone df2_
    where df2_.mdm_customer_rk = df_.mdm_customer_rk and
    greatest(df2_.business_start_dt, df_.business_start_dt) < least(df2_.business_end_dt, df_.business_end_dt) and
    df2_.phone_num != df_.phone_num and 
    df2_.main_phone_flg
  ) count_error
 from dfct_phone df_ where main_phone_flg 
)
select sum(count_error) from coun;

/* 
 * проверка  флага дупликации, ожидаемое значение 0
 * 
 * проверка того, что флаг дупликации проставлен верно
*/
with dupl_1 as
(
/*
 * Строим наборы интервалов для телефона, по которым есть дубли
 */
    select distinct pdf_.phone_num, pdf_.business_start_dt from dfct_phone pdf_ 
 left join dfct_phone pdf2_
 on pdf_.phone_num = pdf2_.phone_num and pdf_.mdm_customer_rk != pdf2_.mdm_customer_rk 
 and  greatest(pdf_.business_start_dt, pdf2_.business_start_dt) < least(pdf_.business_end_dt, pdf2_.business_end_dt)
 where (pdf2_.mdm_customer_rk is not null)
),
dupl_2 as 
( 
/*
 * Записи, по которым есть дубли помечаем флагом dupl_check_flg = true
 * А затем сравниваем данный флаг с 
 */
 select 
  case 
   when (pdf_.phone_num, pdf_.business_start_dt) in ( select phone_num,business_start_dt from dupl_1)  then true
      else false
  end dupl_check_flg,
  pdf_.duplication_flg
 from dfct_phone pdf_
)
select max( (not(dupl_check_flg= duplication_flg )):: int) as result from dupl_2;