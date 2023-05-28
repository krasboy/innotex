drop table if exists p_dfct_phone_0010;
create table p_dfct_phone_0010 as
select 
	c_.counterparty_rk,
	/* интервалы действия могут не совпадать, поэтому  берем их пересечение*/
	greatest(c_.effective_from_date, dcp_.effective_from_date) as effective_from_date,
	least(c_.effective_to_date, dcp_.effective_to_date) as effective_to_date,
	c_.counterparty_type_cd
from counterparty c_
/* соединяемся с dict_counterparty_type_cd, берем пересечение интервалов действия версии
 * и отсекаем не подходящие источники и не подходящие типы клиентов
*/
left join dict_counterparty_type_cd dcp_ 
	on c_.counterparty_type_cd  = dcp_.counterparty_type_cd 
where c_.src_cd = 'MDMP' and dcp_.src_cd in ('MDMP', 'RTLL', 'RTLS', 'CFTB', 'WAYN')
     and dcp_.counterparty_type_desc  = 'физическое лицо' 
	 and greatest(c_.effective_from_date, dcp_.effective_from_date) <= least(c_.effective_to_date, dcp_.effective_to_date);
table p_dfct_phone_0010;

drop table if exists p_dfct_phone_0020;
create table p_dfct_phone_0020 as
with p_dfct_phone_0020_1 as
(
/*
 * Для записей из   counterparty_contact у которых источник не равен 'MDMP'
 *  производим поиск глобального идентификатора клиента пересечением с counterparty_x_uniq_counterparty 
 * Отбрасываем записи с неподходящим источником и неперсекающимися интервалами
 */
select 
	c_c_.counterparty_rk,
	c_c_.contact_type_cd, 
	greatest(c_c_.effective_from_date, cxupdf_.effective_from_date) as effective_from_date,
	least(c_c_.effective_to_date, cxupdf_.effective_to_date) as effective_to_date,
	c_c_.contact_desc,	
	contact_quality_code,
	trust_system_flg,	
	cxupdf_.uniq_counterparty_rk,
	c_c_.src_cd
from counterparty_contact c_c_
inner join counterparty_x_uniq_counterparty cxupdf_
on c_c_.counterparty_rk = cxupdf_.counterparty_rk
where cxupdf_.src_cd in ('MDMP', 'RTLL', 'RTLS', 'CFTB', 'WAYN')  and c_c_.src_cd  != 'MDMP'
and greatest(c_c_.effective_from_date, cxupdf_.effective_from_date) <= least(c_c_.effective_to_date, cxupdf_.effective_to_date)
),
p_dfct_phone_0020_2 as
(
/*
 * Для записей из   counterparty_contact у которых источник равен 'MDMP',
 *  глобальный идентификатор клиента берем равный локальному идентификатору.
 */
select 
	c_c_.counterparty_rk,
	c_c_.contact_type_cd, 
	c_c_.effective_from_date,
	c_c_.effective_to_date,
	c_c_.contact_desc,
	contact_quality_code,
	c_c_.trust_system_flg,		
	c_c_.counterparty_rk as uniq_counterparty_rk,
	c_c_.src_cd
from counterparty_contact c_c_
where  c_c_.src_cd  = 'MDMP'
)
/*
 * Объединяем наборы строк, полученные в  p_dfct_phone_0020_1 и  p_dfct_phone_0020_2
 */
	select * from p_dfct_phone_0020_1
	union 
	select * from p_dfct_phone_0020_2;

drop table if exists p_dfct_phone_0030;
create table p_dfct_phone_0030 as
/*
 * Отбрасываем из  p_dfct_phone_0020 записи о клиентах, которые не физ лица, путем пересечения с  p_dfct_phone_0010. 
 * Отбрасываем где нет пересечения интервалов
 */
select 
	pdf20_.counterparty_rk,
	pdf20_.contact_type_cd, 
	greatest(pdf20_.effective_from_date, pdf10_.effective_from_date) as effective_from_date,
	least(pdf20_.effective_to_date, pdf10_.effective_to_date) as effective_to_date,
	pdf20_.contact_desc,
	pdf20_.trust_system_flg,
	pdf20_.uniq_counterparty_rk,
	pdf20_.contact_quality_code,	
	pdf20_.src_cd
from p_dfct_phone_0020 pdf20_
inner join p_dfct_phone_0010 pdf10_
on pdf20_.uniq_counterparty_rk = pdf10_.counterparty_rk
where  greatest(pdf20_.effective_from_date, pdf10_.effective_from_date) <= least(pdf20_.effective_to_date, pdf10_.effective_to_date);

/*
 * Подготавливаем таблицы к следующей обработке, вводим временные атрибуты и заполняем их
 * Делаем что можем для оценки каждой записи, без разделения на меньшие интервалы
 */
drop table if exists p_dfct_phone_0040;
create table p_dfct_phone_0040 as
with p_dfct_phone_0040_1 as
( 
/*  
 * Заполняем  notification_flg , atm_flg
 */
select pdf_.*,
case 
	when contact_type_cd = 'NotificPhone' then true
	else false
end notification_flg,
case 
	when contact_type_cd = 'ATMPhone' then true
	else false
end atm_flg
from p_dfct_phone_0030 pdf_ 
where pdf_.src_cd in ('MDMP', 'RTLL', 'RTLS', 'CFTB', 'WAYN')
order by  contact_desc
),
p_dfct_phone_0040_2 as
(
/*  
 * ранжируем версии номеров по дате начала
 * ранжируем версии клиентов по дате начала
 * используя row_number, мы можем получить что с одинаковыми параметрами получать разный ранг, 
 * но иначе мы можем получить, в ситуции, когда равны даты и другие условия, дубли по майн флагам
 */
	select pdf_.*, row_number() over (partition by contact_desc order by effective_from_date ) rang_start_phone_num ,
	  row_number() over (partition by counterparty_rk order by effective_from_date ) rang_start_client_num
    from p_dfct_phone_0040_1 pdf_	 
),
p_dfct_phone_0040_3 as
(
/*  
 * строим ранжированную  запись по каждому критерию из п. 6 бизнес-требования
 */
select pdf_.*,
  case
    when trust_system_flg then 1
    else 0 
  end as range_trust_num, 
  case 
  	when contact_quality_code like '%GOOD%' then 1
  	else 0
  end as range_quality_num,  
  case 
  	when src_cd = 'MDMP' then 5
  	when src_cd = 'WAYN' then 4
  	when src_cd = 'RTLL' then 3
  	when src_cd = 'RTLS' then 2
  	when src_cd = 'CFTB' then 1
  end range_src_num,
  case 
  	when contact_type_cd = 'NotificPhone' then 5
  	when contact_type_cd = 'ATMPhone' then 4
  	when contact_type_cd = 'MobilePersonalPhone' then 3
  	when contact_type_cd = 'MobileWorkNumber' then 2
  	when contact_type_cd = 'HomePhone' then 1
  end range_type_num 
from p_dfct_phone_0040_2 pdf_
)
select 
 uniq_counterparty_rk as mdm_customer_rk,
 contact_type_cd as	phone_type_cd,
 effective_from_date as	business_start_dt,
 effective_to_date as business_end_dt,
 contact_desc as phone_num,
 trust_system_flg,
 notification_flg,
 atm_flg,
 rang_start_phone_num,
 rang_start_client_num,
 range_trust_num,
 range_quality_num,
 range_src_num,
 range_type_num
from p_dfct_phone_0040_3;

drop table if exists p_dfct_phone_0050;
create table p_dfct_phone_0050 as
 with p_dfct_phone_0050_1 as(
/*  
 * Дробим на минимальные интервалы действия по каждому телефону
 * Собираем точки начала и конца интервалов по каждому телефону
 */
	select phone_num, business_start_dt as date from p_dfct_phone_0040 
	union 
	select phone_num, business_end_dt  as date from p_dfct_phone_0040
),
p_dfct_phone_0050_2 as
/*  
 * Объединяем точки начала и конца интервала в один список
 */
(
	select phone_num, date from p_dfct_phone_0050_1 order by 1,2
),
p_dfct_phone_0050_3 as 
/*  
 * Строим минимальные интервалы действия по каждому телефону
 */
(
	select phone_num, date as business_start_dt, lead(date) over w as business_end_dt
	from p_dfct_phone_0050_2
	window w as (partition by phone_num order by date)
),
p_dfct_phone_0050_4 as
(
/*  
 * Наполняем значениями минимальные интервалы действия по каждому телефону,
 * отбрасываем пустые интервалы
 */
select 
	pdf40_.mdm_customer_rk,
	pdf40_.phone_type_cd,
	pdf50_.business_start_dt,
	pdf50_.business_end_dt,
	pdf40_.phone_num,
	pdf40_.notification_flg,
	pdf40_.atm_flg,
	pdf40_.trust_system_flg,
	pdf40_.rang_start_phone_num,
	pdf40_.rang_start_client_num,
	pdf40_.range_trust_num,
	pdf40_.range_quality_num,
	pdf40_.range_src_num,
	pdf40_.range_type_num
from p_dfct_phone_0050_3 pdf50_ left join p_dfct_phone_0040 pdf40_
on pdf50_.phone_num = pdf40_.phone_num 
and greatest(pdf40_.business_start_dt , pdf50_.business_start_dt) < least(pdf40_.business_end_dt , pdf50_.business_end_dt)
where pdf50_.business_end_dt is not null  and pdf40_.mdm_customer_rk is not null  order by 5,3,4,1
),
p_dfct_phone_0050_5 as
(
/*   
 *  Собираем интервалы, по которым есть пересечения по номеру телефона и дате действия
 */
	select distinct pdf_.phone_num, pdf_.business_start_dt, pdf_.business_end_dt from p_dfct_phone_0050_4 pdf_ 
	left join p_dfct_phone_0040 pdf2_
	on pdf_.phone_num = pdf2_.phone_num and pdf_.mdm_customer_rk != pdf2_.mdm_customer_rk 
	and  greatest(pdf_.business_start_dt, pdf2_.business_start_dt) < least(pdf_.business_end_dt, pdf2_.business_end_dt)
	where (pdf2_.mdm_customer_rk is not null)
),
p_dfct_phone_0050_6 as
(
/*   
 *  Заполняем атрибут duplication_flg
 */
select pdf_.*, 
case 
	when (pdf_.phone_num, pdf_.business_start_dt, pdf_.business_end_dt) in ( select phone_num,business_start_dt,business_end_dt from p_dfct_phone_0050_5)  then true
    else false
end duplication_flg
from p_dfct_phone_0050_4 pdf_ 
),
p_dfct_phone_0050_7 as
(
/*   
 *  Заполняем вспомогательный атрибут range_duplication_num
 */
select pdf_.*,
  case
    when duplication_flg then 0
    else 1 
  end as range_duplication_num
  from p_dfct_phone_0050_6 pdf_
),
p_dfct_phone_0050_8 as
(
/*  
 * взвешиваем каждую запись в зависимости от важности оценок для определения .
* когда дубля нет вес будет максимальным, в противном случае будет выбран по указанному  в п.6 бизнес требований 
 */
 select c.* ,
   range_duplication_num * 100000000 + range_trust_num * 10000000 + range_quality_num * 1000000 + range_src_num * 100000 + range_type_num * 10000 + rang_start_phone_num as weght_phone_num
   
 from p_dfct_phone_0050_7 c
),
p_dfct_phone_0050_9 as
(
/*  
 * Для заполнения флаг основного клиента для данного телефона и флага лучшего телефона для клиента
 * Сравниваем вес каждой записи с максимальным значением  на выборке  для текущего номера, аналогично для клиента * 
 */
	select pdf_.* ,
	case 
		when pdf_.weght_phone_num = (
			select max(pdf2_.weght_phone_num) from p_dfct_phone_0050_8 pdf2_ where 
			pdf_.phone_num = pdf2_.phone_num  
			and greatest(pdf_.business_start_dt, pdf2_.business_start_dt) < least(pdf_.business_end_dt, pdf2_.business_end_dt)
			) then true
		else false 
	end main_dup_flg
	from p_dfct_phone_0050_8 pdf_
)
select * from p_dfct_phone_0050_9;

 drop table if exists p_dfct_phone_0060;
create table p_dfct_phone_0060 as
 with p_dfct_phone_0060_1 as(
/*  
 * Делал, для удобства
 * Нужно удалить, ничего не потеряем
 */
 select  
    mdm_customer_rk,
	phone_type_cd,
	business_start_dt,
	business_end_dt,
	phone_num,
	notification_flg,
	atm_flg,
	trust_system_flg,
	rang_start_phone_num,
	rang_start_client_num,
	range_trust_num,
	range_quality_num,
	range_src_num,
	range_type_num,
	duplication_flg,
	range_duplication_num,
	weght_phone_num,
	main_dup_flg
from 	p_dfct_phone_0050
),
p_dfct_phone_0060_2 as
(
SELECT MIN(business_start_dt) AS business_start_dt , MAX(business_end_dt) AS business_end_dt, mdm_customer_rk, phone_type_cd,	phone_num, notification_flg, atm_flg, trust_system_flg,	rang_start_phone_num, rang_start_client_num, range_trust_num,	range_quality_num, range_src_num, range_type_num, duplication_flg, range_duplication_num, weght_phone_num, main_dup_flg 
  FROM (
         SELECT  business_start_dt, business_end_dt, SUM(sog) OVER(ORDER BY business_start_dt,business_end_dt,mdm_customer_rk, phone_type_cd,	phone_num, notification_flg, atm_flg, trust_system_flg,	rang_start_phone_num, rang_start_client_num, range_trust_num,	range_quality_num, range_src_num, range_type_num, duplication_flg, range_duplication_num, weght_phone_num, main_dup_flg) AS grp_id,
         mdm_customer_rk, phone_type_cd,	phone_num, notification_flg, atm_flg, trust_system_flg,	rang_start_phone_num, rang_start_client_num, range_trust_num,	range_quality_num, range_src_num, range_type_num, duplication_flg, range_duplication_num, weght_phone_num, main_dup_flg
           FROM (
                  SELECT business_start_dt, business_end_dt, mdm_customer_rk, phone_type_cd,	phone_num, notification_flg, atm_flg, trust_system_flg,	rang_start_phone_num, rang_start_client_num, range_trust_num,	range_quality_num, range_src_num, range_type_num, duplication_flg, range_duplication_num, weght_phone_num, main_dup_flg,	 
                         CASE 
                           WHEN MAX(business_end_dt) 
                               OVER(ORDER BY business_start_dt, business_end_dt ,mdm_customer_rk, phone_type_cd,	phone_num, notification_flg, atm_flg, trust_system_flg,	rang_start_phone_num, rang_start_client_num, range_trust_num,	range_quality_num, range_src_num, range_type_num, duplication_flg, range_duplication_num, weght_phone_num, main_dup_flg
                                        ROWS BETWEEN unbounded preceding
                                                         AND 1 preceding
                                   ) >= business_start_dt
                           THEN 0 
                           ELSE 1 
                         END AS sog 
                    FROM p_dfct_phone_0060_1 t
                ) v0
       ) v1
 GROUP BY grp_id, mdm_customer_rk, phone_type_cd,	phone_num, notification_flg, atm_flg, trust_system_flg,	rang_start_phone_num, rang_start_client_num, range_trust_num,	range_quality_num, range_src_num, range_type_num, duplication_flg, range_duplication_num, weght_phone_num, main_dup_flg
)
select * from p_dfct_phone_0060_2;

drop table if exists p_dfct_phone_0070;
create table p_dfct_phone_0070 as
 with p_dfct_phone_0070_1 as(
/*  
 * Дробим на минимальные интервалы действия по каждому клиенту
 * Собираем точки начала и конца интервалов по каждому клиенту
 */
	select mdm_customer_rk, business_start_dt as date from p_dfct_phone_0060 
	union 
	select mdm_customer_rk, business_end_dt  as date from p_dfct_phone_0060
),
p_dfct_phone_0070_2 as
/*  
 * Объединяем точки начала и конца интервала в один список
 */
(
	select mdm_customer_rk, date from p_dfct_phone_0070_1 order by 1,2
),
p_dfct_phone_0070_3 as 
/*  
 * Строим минимальные интервалы действия по каждому телефону
 */
(
	select mdm_customer_rk, date as business_start_dt, lead(date) over w as business_end_dt
	from p_dfct_phone_0070_2
	window w as (partition by mdm_customer_rk order by date)
),
p_dfct_phone_0070_4 as
(
/*  
 * Наполняем значениями минимальные интервалы действия по каждому клиенту,
 * отбрасываем пустые интервалы
 */
select 
	pdf60_.mdm_customer_rk mdm_customer_rk,
	pdf70_.mdm_customer_rk mdm_customer_rk7,
	pdf60_.phone_type_cd,
	pdf60_.business_start_dt business_start_dt6,
	pdf60_.business_end_dt business_end_dt6,
	pdf70_.business_start_dt,
	pdf70_.business_end_dt,
	pdf60_.phone_num,
	pdf60_.notification_flg,
	pdf60_.atm_flg,
	pdf60_.trust_system_flg,
	pdf60_.rang_start_phone_num,
	pdf60_.rang_start_client_num,
	pdf60_.range_trust_num,
	pdf60_.range_quality_num,
	pdf60_.range_src_num,
	pdf60_.range_type_num,
	pdf60_.range_duplication_num,
	pdf60_.duplication_flg,
	pdf60_.main_dup_flg,
	pdf60_.weght_phone_num  
from p_dfct_phone_0070_3 pdf70_ left join p_dfct_phone_0060 pdf60_
on pdf70_.mdm_customer_rk = pdf60_.mdm_customer_rk 
and greatest(pdf60_.business_start_dt , pdf70_.business_start_dt) < least(pdf60_.business_end_dt , pdf70_.business_end_dt)
where pdf70_.business_end_dt is not null  and pdf60_.mdm_customer_rk is not null  order by 5,3,4,1
)
,
p_dfct_phone_0070_8 as
(
/*  
 * взвешиваем каждую запись в зависимости от важности оценок для определения .
* когда дубля нет вес будет максимальным, в противном случае будет выбран по указанному  в п.6 бизнес требований 
 */
 select c.* ,
   range_duplication_num * 100000000 + range_trust_num * 10000000 + range_quality_num * 1000000 + range_src_num * 100000 + range_type_num * 10000 + rang_start_client_num as weght_client_num   
 from p_dfct_phone_0070_4 c
),
p_dfct_phone_0070_9 as
(
/*  
 * Для заполнения флаг основного клиента для данного телефона и флага лучшего телефона для клиента
 * Сравниваем вес каждой записи с максимальным значением  на выборке  для текущего номера, аналогично для клиента * 
 */
	select pdf_.* ,
	case 
		when pdf_.weght_client_num = (
			select max(pdf2_.weght_client_num) from p_dfct_phone_0070_8 pdf2_ where 
			pdf_.mdm_customer_rk = pdf2_.mdm_customer_rk  
			and greatest(pdf_.business_start_dt, pdf2_.business_start_dt) < least(pdf_.business_end_dt, pdf2_.business_end_dt)
			) then true
		else false 
	end main_phone_flg
	from p_dfct_phone_0070_8 pdf_
)
select * from p_dfct_phone_0070_9;

drop table if exists p_dfct_phone_0080;
create table p_dfct_phone_0080 as
 with p_dfct_phone_0080_1 as(
/*  
 * Делал, для удобства
 * Нужно удалить, ничего не потеряем
 */
 select  
    mdm_customer_rk,
	phone_type_cd,
	business_start_dt,
	business_end_dt,
	phone_num,
	notification_flg,
	atm_flg,
	trust_system_flg,
	rang_start_phone_num,
	rang_start_client_num,
	range_trust_num,
	range_quality_num,
	range_src_num,
	range_type_num,
	duplication_flg,
	range_duplication_num,
	weght_phone_num,
	main_dup_flg,
	weght_client_num,
	main_phone_flg
from 	p_dfct_phone_0070
)
,
p_dfct_phone_0080_2 as
(
SELECT MIN(business_start_dt) AS business_start_dt , MAX(business_end_dt) AS business_end_dt, mdm_customer_rk, phone_type_cd, phone_num, notification_flg, atm_flg, trust_system_flg, duplication_flg, main_dup_flg, main_phone_flg	
  FROM (
         SELECT  business_start_dt, business_end_dt, SUM(sog) OVER(ORDER BY business_start_dt,business_end_dt,mdm_customer_rk, phone_type_cd, phone_num, notification_flg, atm_flg, trust_system_flg, duplication_flg, main_dup_flg, main_phone_flg	) AS grp_id, mdm_customer_rk, phone_type_cd,  phone_num, notification_flg, atm_flg, trust_system_flg, duplication_flg, main_dup_flg, main_phone_flg	
           FROM (
                  SELECT business_start_dt, business_end_dt, mdm_customer_rk, phone_type_cd, phone_num, notification_flg, atm_flg, trust_system_flg, duplication_flg, main_dup_flg, main_phone_flg	,
                         CASE 
                           WHEN MAX(business_end_dt) 
                               OVER(ORDER BY business_start_dt, business_end_dt ,mdm_customer_rk, phone_type_cd, phone_num, notification_flg, atm_flg, trust_system_flg, duplication_flg, main_dup_flg, main_phone_flg	
                                        ROWS BETWEEN unbounded preceding
                                                         AND 1 preceding
                                   ) >= business_start_dt
                           THEN 0 
                           ELSE 1 
                         END AS sog 
                    FROM p_dfct_phone_0080_1 t
                ) v0
       ) v1
 GROUP BY grp_id, mdm_customer_rk, phone_type_cd,  phone_num, notification_flg, atm_flg, trust_system_flg, duplication_flg, main_dup_flg, main_phone_flg	
)
select * from p_dfct_phone_0080_2;

drop table if exists dfct_phone;
create table dfct_phone as
/*
 * Переименовываем поля, в соответствии с требованием.
 *  Отбрасываем не используемые атрибуты.
 */
select 
distinct
	mdm_customer_rk, 
	phone_type_cd,
	business_start_dt,
	business_end_dt,
	phone_num,
	notification_flg,
	atm_flg,
	trust_system_flg,
	duplication_flg,
	main_dup_flg,
	main_phone_flg		 
from p_dfct_phone_0080 ;

