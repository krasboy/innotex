/*
* Создадим, схему, таблицу и процедуру логирования
*/

create schema log;

create table log.log(
	id serial primary key,
	category int,
	source text,
	destination text,
	message text,
	type_operation text,
	count_row int,
	stamp timestamp
);
/*
* В лог не будем писать вставки, удаления, обновления с нулевым количеством обновленных строк
*/
create or replace function log.write_log(
	message text default '',
	category int default 0 ,
	source text default '',
	destination text default '',
	type_operation text default '',
	count_row int default 0,
	stamp timestamp default now()) 
	returns boolean
	language plpgsql
	as $function$
declare
    v_rc int;
begin  	
  if not((type_operation = 'insert' or type_operation = 'update' or type_operation = 'delete') and count_row = 0) then
	  insert into log.log(category, source, destination,	message, type_operation, count_row,	stamp)
	  values
	   (category, source, destination, message, type_operation, count_row, stamp); 	  
  end if; 
  return true;
end $function$;
/*
* Создадим представление в котором не будем выводить события по модифицированному вводу данных 
*( не через cdc) и режим отображения установим, для удобства в порядке убывания номера события
*/
create or replace
view log.v_show_log as
select
	*
from
	log.log
where
	source not like 'oltp_src_system.mod_%'
order by id desc;

/*
* Создаем генераторы синтетических данных
*/

create schema oltp_src_system;

create table oltp_src_system.request_data (
	id int4 not null,
	status_nm text null,
	create_dttm timestamp null,
	update_dttm timestamp null
);
/*
* Процедура для генерации вставки новых записей
*/
create or replace function oltp_src_system.create_request()
	returns boolean
	language plpgsql
	as $function$
declare
    v_rc int;
	max_id int;
begin
	perform  log.write_log('',0 , 'oltp_src_system.create_request()','', 'start', 0);
	select id into max_id
	from
	coalesce
	 (
	   ( 
	   	 select max(id) from oltp_src_system.request_data
	   	) + 1,	1
	  ) id ;

	insert into	oltp_src_system.request_data(
		id,
		status_nm,
		create_dttm,
		update_dttm)
	select
		id,
		'accepted' status_nm,
		now() create_dttm,
		now() update_dttm
	from
		(
		select n id
		from
			generate_series
			(
				max_id ,
				max_id + round(random()* 3)::int
      	  ) n
  	   ) nn;
	get diagnostics v_rc = row_count;
	perform  log.write_log('',0 ,'oltp_src_system.create_request()','oltp_src_system.request_data', 'insert', v_rc);
	raise notice '% rows inserted into task_data', v_rc;
	perform  log.write_log('',0 , 'oltp_src_system.create_request()','', 'stop', 0) ;
	return true;
end $function$
;
/*
* Процедура для генерации изменения статуса заявок
*/
create or replace function oltp_src_system.update_existed_request()
	returns boolean
	language plpgsql
	as $function$
declare
    v_rc int;
begin    
	perform  log.write_log('',0 , 'oltp_src_system.update_existed_request()','', 'start', 0);
	update oltp_src_system.request_data
	set
		status_nm = case
			(floor(random() * 4 + 1)::int)                     
        	when 1 then 'executed'
			when 2 then 'paused'
			when 3 then 'complited'
			when 4 then 'canselled'
			else null
	   	end,
		update_dttm = now()
	where
		id in
		(
			select id from
			(
				select	id,	round(random()* 10) rnd
				from
					oltp_src_system.request_data
				where
					status_nm not in ('complited', 'canselled')
        	 ) rnd_tbl
			where
			(rnd - floor(rnd / 10)) = 1
		);
	get diagnostics v_rc = row_count;
	perform  log.write_log('',0 ,'oltp_src_system.update_existed_request()','oltp_src_system.request_data', 'update', v_rc);
	raise notice '% rows updated into task_data', v_rc;
	perform  log.write_log('',0 , 'oltp_src_system.update_existed_request()','', 'stop', 0);
	return true;
end $function$
;
/*
* Процедура для генерации удаления заявок
* Удаляем старые закрытые или отмененные задачи
*/
create or replace function oltp_src_system.deleted_existed_request()
	returns boolean
	language plpgsql
	as $function$
declare
    v_rc int;
begin                   
	perform  log.write_log('',0 , 'oltp_src_system.deleted_existed_request()','', 'start', 0);
	delete
	from
		oltp_src_system.request_data
	where
		status_nm in ('complited', 'canselled')
		and update_dttm in 
			(
				select min(update_dttm)
				from
					oltp_src_system.request_data
				where
					status_nm in ('complited', 'canselled')                      
    	     );
	get diagnostics v_rc = row_count;
	perform  log.write_log('',0 ,'oltp_src_system.deleted_existed_request()','oltp_src_system.request_data', 'delete', v_rc);
	raise notice '% rows deleted into task_data', v_rc;
	perform  log.write_log('',0 , 'oltp_src_system.deleted_existed_request()','', 'stop', 0);
	return true;
end $function$
;
/*
* Создаем для схемы cdc таблицу, где будут храниться изменения исходных данных,
* а так же создаем тригер, который будет выдавать в схему cdc изменения исходных данных
*/

create schema oltp_cdc_src_system;

create table oltp_cdc_src_system.request_data_changes (
	id int4 not null,
	status_nm text null,
	create_dttm timestamp null,
	update_dttm timestamp null,
	operation bpchar(1) not null,
	stamp timestamp not null
);
/*
* К тригеру присоединяем логирование
*/
create or replace function oltp_cdc_src_system.request_data_changes()
	returns trigger
	language plpgsql
	as $function$
declare
    v_rc int;
begin
    if (tg_op = 'DELETE') then
    	insert into oltp_cdc_src_system.request_data_changes
    	select old.*, 'D', now();
    	get diagnostics v_rc = row_count;
	    perform  log.write_log('triger ',0 ,'oltp_cdc_src_system.request_data_changes()','oltp_cdc_src_system.request_data_changes', 'insert', v_rc);
        return old;
	elsif (tg_op = 'UPDATE') then
        insert into oltp_cdc_src_system.request_data_changes
        select new.*, 'U', now();
    	get diagnostics v_rc = row_count;
	    perform  log.write_log('triger ',0 ,'oltp_cdc_src_system.request_data_changes()','oltp_cdc_src_system.request_data_changes', 'insert', v_rc);
        return new;
    elsif (tg_op = 'INSERT') then
    	insert into oltp_cdc_src_system.request_data_changes
    	select new.*, 'I', now();
       	get diagnostics v_rc = row_count;
	    perform  log.write_log('triger ',0 ,'oltp_cdc_src_system.request_data_changes()','oltp_cdc_src_system.request_data_changes', 'insert', v_rc);
    	return new;
    end if;
	return null;
end;
$function$
;

create trigger request_data_changes after
insert
	or
delete
	or
update
	on
	oltp_src_system.request_data for each row execute function oltp_cdc_src_system.request_data_changes();
/*
* Создаем: первый слой  хранилища,  таблицу для хранения данных в этом и процедуру наполняющую таблицу
*/	
	
create schema dwh_stage ;

create extension if not exists pgcrypto;
/*
* В таблице создаем хэш ключ, для упрощения идентификации заявок
*/
create table dwh_stage.request_data_changes_dwh (
	id int4 not null,
	status_nm text null,
	create_dttm timestamp null,
	update_dttm timestamp null,
	operation bpchar(1) not null,
	stamp timestamp not null,
	hash_key bytea not null generated always as 
	(
	  digest
	       (
	         id::text ||
	        	 (
	         		coalesce
	         		(
	        	 		create_dttm,'1901-01-01'
	       		  	)
	      		   	-'1900-01-01'
	      		  )::text,
	      	  'sha256'::text
	       )
	 ) stored,
	is_processed_flg bool not null default false
);
/*
* Загружаем данные только те, которых нет в целевой таблице
*/
create or replace function dwh_stage.load_dwh_stage()
	returns boolean
	language plpgsql
	as $function$
declare
    v_rc int;
begin        
	perform log.write_log('',0 , 'dwh_stage.load_dwh_stage()','', 'start', 0);
	insert
		into dwh_stage.request_data_changes_dwh 
		(id, status_nm,	create_dttm, update_dttm, operation, stamp) 
	select
		id,	status_nm, create_dttm,	update_dttm, operation,	stamp
	from
		oltp_cdc_src_system.request_data_changes cdc
	where
		(id, status_nm,	create_dttm, update_dttm, operation, stamp) not in 
   		(
			select
				id,	status_nm, create_dttm,	update_dttm, operation,	stamp
			from
				dwh_stage.request_data_changes_dwh stage
		);

	get diagnostics v_rc = row_count;
	perform  log.write_log('',0 ,'dwh_stage.load_dwh_stage()','dwh_stage.request_data_changes_dwh', 'insert', v_rc);
	raise notice '% rows inserted into task_data',v_rc;
	perform log.write_log('',0 , 'dwh_stage.load_dwh_stage()','', 'stop', 0);
	return true;
end $function$;
/*
* Создаем: второй (детальный) слой  хранилища,  таблицу для хранения данных в этом и процедуру наполняющую таблицу
*/	

create schema dwh_ods;

create table dwh_ods.request_data_hist (
	id int4 not null,
	status_nm text null,
	create_dttm timestamp null,
	hash_key bytea not null,
	valid_from_dttm timestamp not null,
	valid_to_dttm timestamp null,
	deleted_flg bool null,
	deleted_dttm timestamp null,
	primary key(hash_key, valid_from_dttm)
);

/*
*  Самая большая процедура на заполнение  таблицы request_data_hist и куча проверок
*/
create or replace function dwh_ods.load_from_stage_hist_request()
	returns boolean
	language plpgsql
	as $function$
declare
    v_rc int;   
    v2_rc int;
begin  
	perform log.write_log('',0 , 'dwh_ods.load_from_stage_hist_request()','', 'start', 0);
/*
*  Создаем временную вспомогательную таблицу и переносим все необработанные записи из request_data_changes_dwh 
*/
	create temp table temp_stage  as
	select *
	from dwh_stage.request_data_changes_dwh
	where not is_processed_flg ;
	get diagnostics v_rc = row_count;
	perform  log.write_log('вставка во временную таблицу ',0 ,'dwh_ods.load_from_stage_hist_request()','temp_stage', 'insert', v_rc);

/*
 * Сделаем проверку, нет ли новых заявок с неправильным статусом
 */
		
	select 
		count(hash_key) 
	into 
		v_rc 
	from
		temp_stage t_stage
	where 
		t_stage.operation = 'I'
		and t_stage.status_nm != 'accepted'
		and t_stage.hash_key not in
		(
			select
				hash_key
			from
				dwh_ods.request_data_hist
		);
	if v_rc != 0 
	then
		perform  log.write_log('при вставка новых заявок обнаружены новые заявки, имеющие статус не accepted', 2, 'dwh_ods.load_from_stage_hist_request()','dwh_ods.request_data_hist', 'check', v_rc);
	end if;
/*
*  Вставляем новые заявки
*/
 	insert into	dwh_ods.request_data_hist 
        (
        	id,
	        status_nm,
    	    create_dttm,
        	hash_key,
	        valid_from_dttm,
    	    valid_to_dttm,
        	deleted_flg,
	        deleted_dttm
        )
  	select  
  		id,
  		status_nm,
  		create_dttm,
  		hash_key,
  		stamp valid_from_dttm,
  		'2999/12/31 23:59:59'::timestamp valid_to_dttm,
  		false deleted_flg,
  		null deleted_dttm
	from
		temp_stage t_stage
	where
		t_stage.operation = 'I'
		and t_stage.status_nm = 'accepted'
		and t_stage.hash_key not in
		(
			select
				hash_key
			from
				dwh_ods.request_data_hist
		);
	get diagnostics v_rc = row_count;
	perform  log.write_log('вставка новых заявок',0 ,'dwh_ods.load_from_stage_hist_request()','dwh_ods.request_data_hist', 'insert', v_rc);


/*
 *  Сделаем проверку на встаку  новых статусов существующих заявок
 */ 

	with c_status_max_data as
	(
		select 
			hash_key,
			coalesce(status_nm,'complited') status_nm
		from 
			dwh_ods.request_data_hist
		where 
		  valid_to_dttm = '2999/12/31 23:59:59'
			
	)
	select 
		count(t_stage.hash_key) 
	into 
		v_rc 
	from
		temp_stage t_stage, c_status_max_data csmd
	where 
		t_stage.operation = 'U'
		and csmd.status_nm in ('complited','canselled')
		and t_stage.hash_key = csmd.hash_key	
		;
	if v_rc != 0 
	then
		perform  log.write_log('при вставка новых статусов существующих заявок, обнаружены изменение статуса законченных заявок', 2, 'dwh_ods.load_from_stage_hist_request()','dwh_ods.request_data_hist', 'check', v_rc);
	end if;	
/*
 *  Сделаем ещё одну проверку на встаку  новых статусов существующих заявок
 */ 

	select 
		count(hash_key) 
	into 
		v_rc 
	from
		temp_stage t_stage
	where 
		t_stage.operation = 'D'
		and t_stage.status_nm not in ('complited','canselled')
		and t_stage.hash_key  in 
		(
			select
				hash_key
			from
				dwh_ods.request_data_hist
		);
	if v_rc != 0 
	then
		perform  log.write_log('при вставка новых статусов существующих заявок, обнаружено удаление не законченных заявок', 2, 'dwh_ods.load_from_stage_hist_request()','dwh_ods.request_data_hist', 'check', v_rc);
	end if;	
/*
 *  И ещё одну проверку на встаку  новых статусов существующих заявок
 */ 

	select 
		count(hash_key) 
	into 
		v_rc 
	from
		temp_stage t_stage
	where 
		t_stage.operation in ('U', 'D')		
		and t_stage.hash_key  not in 
		(
			select
				hash_key
			from
				dwh_ods.request_data_hist
		);
	if v_rc != 0 
	then
		perform  log.write_log('Внимание! обновление записи, которая отсутсвует в слое dwh_ods, возможно нарушение работы хранилища данных', 3, 'dwh_ods.load_from_stage_hist_request()','dwh_ods.request_data_hist', 'check', v_rc);
	end if;	

/*
 *  вставляем записи с новыми статусвми существующих заявок
 */ 
	insert
		into
		  dwh_ods.request_data_hist 
        	(
        		id,
        		status_nm,
        		create_dttm,
        		hash_key,
        		valid_from_dttm,
        		valid_to_dttm,
        		deleted_flg,
        		deleted_dttm
        	)
	select
		id,
		case
			when t_stage.operation = 'D' then null
			else t_stage.status_nm
		end status_nm,
		create_dttm,
		hash_key,
		stamp valid_from_dttm,
		'2999/12/31 23:59:59'::timestamp valid_to_dttm,
		case
			when t_stage.operation = 'D' then true
			else false
		end deleted_flg,
		case
			when t_stage.operation = 'D' then stamp
			else null
		end deleted_dttm
	from
		temp_stage t_stage
	where
		t_stage.operation in ('U', 'D')
		and t_stage.hash_key in
		(
			select
				hash_key
			from
				dwh_ods.request_data_hist
		);
	get diagnostics v_rc = row_count;
	perform  log.write_log('вставка новых статусов существующих заявок',0 ,'dwh_ods.load_from_stage_hist_request()','dwh_ods.request_data_hist', 'insert', v_rc);

 /*
 *  правим valid_to_dttm
 */ 	 
	with v_table as
	(
		select
			lead(valid_from_dttm,
			1,
			'2999/12/31 23:59:59') over (partition by hash_key order by	valid_from_dttm) as lead_from_dttm,
			hash_key v_hash_key,
			valid_from_dttm v_valid_from_dttm
		from
			dwh_ods.request_data_hist
	)
	update
		dwh_ods.request_data_hist
	set
		valid_to_dttm = lead_from_dttm
	from
		v_table
	where
		v_hash_key = hash_key
		and v_valid_from_dttm = valid_from_dttm
		and valid_to_dttm = '2999/12/31 23:59:59';
	get diagnostics v_rc = row_count;
	perform  log.write_log('правка окончания действия заявок',0 ,'dwh_ods.load_from_stage_hist_request()','dwh_ods.request_data_hist', 'update', v_rc);


/*
 *  объединяем интервалы касающиеся или пересекающиеся которые принадлежат одной операции
 */

	create temp table temp_ods as	
	select
		id,
		status_nm,
		create_dttm,
		hash_key,
		MIN(valid_from_dttm) as valid_from_dttm,
		MAX(valid_to_dttm) as valid_to_dttm ,
		deleted_flg,
		deleted_dttm
	from
		(
			select
			valid_from_dttm,
			valid_to_dttm,
			SUM(sog) over(
							partition by 
								id,
								status_nm,
								create_dttm,
								hash_key,
								deleted_flg,
								deleted_dttm
							order by
								valid_from_dttm,
								valid_to_dttm
							) as grp_id,
			id,
			status_nm,
			create_dttm,
			hash_key,
			deleted_flg,
			deleted_dttm
		from
			(
			select
				valid_from_dttm,
				valid_to_dttm,
				id,
				status_nm,
				create_dttm,
				hash_key,
				deleted_flg,
				deleted_dttm,
				case
					when MAX(valid_to_dttm) 
                               over(
                               			partition by 
                               				id,
                               				status_nm,
                               				create_dttm,
                               				hash_key,
                               				deleted_flg,
                               				deleted_dttm
                               			order by
                               				valid_from_dttm,
                               				valid_to_dttm 
                                        rows between unbounded preceding
                                                         and 1 preceding
                                   ) >= valid_from_dttm
                           then 0
					else 1
				end as sog
			from
				dwh_ods.request_data_hist t
			) v0
		) v1
	group by
		id,
		status_nm,
		create_dttm,
		hash_key,
		deleted_flg,
		deleted_dttm,
		grp_id
	order by
		valid_from_dttm;
	select count(*) into v_rc from dwh_ods.request_data_hist;
	truncate
		dwh_ods.request_data_hist;

	insert
		into
			dwh_ods.request_data_hist 
 	select
		*
	from
		temp_ods;
	select count(*) into v2_rc from dwh_ods.request_data_hist;	
	perform  log.write_log('объединение касающихся интервалов', 1, 'dwh_ods.load_from_stage_hist_request()', 'dwh_ods.request_data_hist', '', v_rc-v2_rc);

/*
 * Метим обработанные записи из предыдущего слоя флагом - отработан (is_processed_flg = true) 
 */ 
	update
		dwh_stage.request_data_changes_dwh
	set
		is_processed_flg = true
	where
	(
		operation,
		stamp,
		hash_key
	) in 
	(
	select
		operation,
		stamp,
		hash_key
	from
		temp_stage
	);
	get diagnostics v_rc = row_count;
	perform  log.write_log('отмечаем отработанные записи ',0 ,'dwh_ods.load_from_stage_hist_request()','dwh_stage.request_data_changes_dwh', 'update', v_rc);
	drop table temp_stage;
	drop table temp_ods;	
	raise notice 'dwh_ods.load_from_stage_hist_request() executed';
	perform log.write_log('',0 , 'dwh_ods.load_from_stage_hist_request()','', 'stop', 0);
	return true;
end $function$;


 /*
 *  Таблица -  производственный календарь
 */ 
create table dwh_ods.dim_date (
   date_key int primary key,
   date_actual date not null,
   year int not null,
   quarter int not null,
   month int not null,
   week int not null,
   day_of_month int not null,
   day_of_week int not null,
   is_weekday boolean not null,
   is_holiday boolean not null,
   fiscal_year int not null
);
 /*
 * Формируем календарь
 * Слегка упростим задачу - календарь на 2023 год
 */ 
create or replace function dwh_ods.load_dim_date()
	returns boolean
	language plpgsql
	as $function$
declare
    v_rc int;
/*
*  Так как праздники и переносы праздников утверждаются правительством, 
* то на кажый год нужны свои данные по праздничным дням,
* и переносам праздничных дней.
* http://duma.gov.ru/news/55144/
*/	
begin    
	perform log.write_log('',0 , 'dwh_ods.load_dim_date()','', 'start', 0);
	create temp table t_holidays 
		(y int,	m int,	d int);
	insert
		into
			t_holidays(y, m, d)
		values
    	(2023, 1, 1),
    	(2023, 1, 2),     
    	(2023, 1, 3),
    	(2023, 1, 4),
    	(2023, 1, 5),      
    	(2023, 1, 6),
    	(2023, 1, 8),
    	(2023, 1, 7),
    	(2023, 2, 23),
    	(2023, 3, 8),      
    	(2023, 5, 1),      
    	(2023, 5, 9),      
    	(2023, 6, 12),
    	(2023, 11, 4);

	create temp table t_change_weekend
		(y int, m int, d int);
	insert
		into
			t_change_weekend(y,	m, d)
		values	  
		(2023, 2, 24),
		(2023, 5, 8);
	
	truncate dwh_ods.dim_date;

	insert
		into
			dwh_ods.dim_date 
		select
		date_part('year', dt)::int * 10000 + date_part('month', dt)::int * 100 + date_part('day', dt)::int as date_key,
		dt as date_actual,
		date_part('year', dt) as year,
		date_part('quarter', dt) as quarter,
		date_part('month', dt) as month,
		date_part('week', dt) as week,
		date_part('day', dt) as day_of_month,
		date_part('isodow',	dt) as day_of_week,
		case
			when date_part('isodow', dt) in (6, 7)
				or (date_part('year', dt), date_part('month',	dt), date_part('day', dt)) 
					in (
						select
							*
						from
							t_change_weekend
						)
				then true
			else false
		end is_weekday,
		case
			when (date_part('year',	dt), date_part('month', dt), date_part('day', dt)) 
				in (
					select
						*
					from
						t_holidays
					)
			  then true
			else false
		end is_holiday,
		date_part('year',dt) as fiscal_year
	from
	generate_series('2023-01-01' ::timestamp, '2023-12-31'::timestamp, '1 day'::interval) as dt ;

	get diagnostics v_rc = row_count;
	perform  log.write_log('',0 ,'dwh_ods.load_dim_date()','dwh_ods.request_data_hist', 'insert', v_rc);
	drop table t_change_weekend;
	drop table t_holidays;
	raise notice '% rows inserted into task_data',v_rc;
	perform log.write_log('',0 , 'dwh_ods.load_dim_date()','', 'stop', 0);
	return true;
end $function$; 

/*
* Так как календарь dwh_ods.dim_date не меняется на протяжении работы, то сразу же его создадим
*/
select dwh_ods.load_dim_date();


/*
* Календарь с агрегированными данными
*/
create table dwh_ods.dim_work (
   date_key int primary key,
   date_actual date not null,
   year int not null,
   quarter int not null,
   month int not null,
   week int not null,
   day_of_month int not null,
   day_of_week int not null,
   is_weekday boolean not null,
   is_holiday boolean not null,
   fiscal_year int not null,
   complited_per_day int not null default 0,
   canselled_per_day int not null default 0,
   accepted_per_day int not null default 0,
   paused_per_day int not null default 0,
   deleted_per_day int not null default 0,
   executed_during_day int not null default 0,
   paused_during_day int not null default 0
);
/*
* Создадим вспомогательную таблицу dwh_ods.agg_request_data_hist
*/
create table dwh_ods.agg_request_data_hist (
	id int4 not null,
	status_nm text null,
	create_dttm date null,
	hash_key bytea not null,
	valid_from_dttm date not null,
	valid_to_dttm date null,
	deleted_flg bool null,
	deleted_dttm date null	
);
/*
* Наполним вспомогательную таблицу dwh_ods.agg_request_data_hist
*/
create or replace function dwh_ods.load_agg_request_data_hist()
	returns boolean
	language plpgsql
	as $function$
declare
    v_rc int;
begin
	perform log.write_log('',0 , 'dwh_ods.load_agg_request_data_hist()','', 'start', 0);
    truncate dwh_ods.agg_request_data_hist;
	insert
		into
			dwh_ods.agg_request_data_hist
		select 
			id,
			status_nm,
			date_trunc('day', create_dttm)::date create_dttm,
			hash_key bytea,
			date_trunc('day', valid_from_dttm)::date valid_from_dttm,
			date_trunc('day', valid_to_dttm)::date valid_to_dttm,
			deleted_flg,
			date_trunc('day', deleted_dttm)::date deleted_dttm
		from
			dwh_ods.request_data_hist;
	get diagnostics v_rc = row_count;
	perform  log.write_log('',0 ,'dwh_ods.load_agg_request_data_hist()','dwh_ods.agg_request_data_hist', 'insert', v_rc);
	raise notice '% rows inserted into task_data', v_rc;
	perform log.write_log('',0 , 'dwh_ods.load_agg_request_data_hist()','', 'stop', 0);
	return true;
end $function$;
/*
* Заполняем основную таблицу - витрину данных dwh_ods.dim_work
*/
create or replace function dwh_ods.load_dim_work()
	returns boolean
	language plpgsql
	as $function$
declare
    v_rc int;
begin 
	perform log.write_log('',0 , 'ddwh_ods.load_dim_work()','', 'start', 0);
	truncate dwh_ods.dim_work;

	with v_complited_per_day as
	(
		select
			valid_from_dttm valid_from,
			count(hash_key) count_key
		from
			dwh_ods.agg_request_data_hist ardh
		where
			status_nm = 'complited'
		group by
			valid_from_dttm
	),
	v_canselled_per_day as
	(
		select
			valid_from_dttm valid_from,
			count(hash_key) count_key
		from
			dwh_ods.agg_request_data_hist ardh
		where
			status_nm = 'canselled'
		group by
			valid_from_dttm
	),
	v_accepted_per_day as
	(
		select
			valid_from_dttm valid_from,
			count(hash_key) count_key
		from
			dwh_ods.agg_request_data_hist ardh
		where
			status_nm = 'accepted'
		group by
			valid_from_dttm
	),
	v_paused_per_day as
	(
		select
			valid_from_dttm valid_from,
			count(hash_key) count_key
		from
			dwh_ods.agg_request_data_hist ardh
		where
			status_nm = 'paused'
		group by
			valid_from_dttm
	),
	v_deleted_per_day as
	(
		select
			deleted_dttm valid_from,
			count(hash_key) count_key
		from
			dwh_ods.agg_request_data_hist ardh
		where
			deleted_flg
		group by
			deleted_dttm
	),
	v_executed_during_day_temp as
	(
		select
			dd.date_actual date_actual,
			ardh.hash_key hash_key
		from
			dwh_ods.dim_date dd,
		    dwh_ods.agg_request_data_hist ardh
		where
			ardh.status_nm = 'executed'
			and ardh.valid_from_dttm <= dd.date_actual
			and dd.date_actual <= ardh.valid_to_dttm	  
	),
	v_executed_during_day as
	(
		select
			date_actual valid_from,
			count(hash_key) count_key
		from
			v_executed_during_day_temp
		group by
			date_actual
	),
	v_paused_during_day_temp as
	(
		select
			dd.date_actual date_actual,
			ardh.hash_key hash_key
		from
			dwh_ods.dim_date dd,
		    dwh_ods.agg_request_data_hist ardh
		where
			ardh.status_nm = 'paused'
			and ardh.valid_from_dttm <= dd.date_actual
			and dd.date_actual <= ardh.valid_to_dttm	  
	),
	v_paused_during_day as
	(
		select
			date_actual valid_from,
			count(hash_key) count_key
		from
			v_paused_during_day_temp
		group by
			date_actual
	),
	v_join as 
	(
		select
			 date_key,
	   		 date_actual,
	 		 year,
	 	     quarter,
		     month,
		     week,
	   		 day_of_month,
	   		 day_of_week,
		     is_weekday,
	   		 is_holiday,
		     fiscal_year,        	 
	     	 coalesce (v_complited_per_day.count_key, 0) complited_per_day,
	    	 coalesce (v_canselled_per_day.count_key, 0) canselled_per_day,
 		     coalesce (v_accepted_per_day.count_key, 0) accepted_per_day,
		     coalesce (v_paused_per_day.count_key, 0) paused_per_day,
	    	 coalesce (v_deleted_per_day.count_key, 0) deleted_per_day,
	     	 coalesce (v_executed_during_day.count_key, 0) executed_during_day,
   	     	 coalesce (v_paused_during_day.count_key, 0) paused_during_day
		from 
 		 	 dwh_ods.dim_date
		  	 left join v_complited_per_day
		 		on date_actual = v_complited_per_day.valid_from
			 left join  v_canselled_per_day
		 		on date_actual = v_canselled_per_day.valid_from
			 left join  v_accepted_per_day
   		  		on date_actual = v_accepted_per_day.valid_from
		 	 left join v_paused_per_day
 				on date_actual = v_paused_per_day.valid_from
 	 		 left join v_deleted_per_day
 		 		on date_actual = v_deleted_per_day.valid_from
 			 left join v_executed_during_day
		 		on date_actual = v_executed_during_day.valid_from
			 left join v_paused_during_day
		 		on date_actual = v_paused_during_day.valid_from
		)
    insert
		into
			dwh_ods.dim_work
		select 
			*
		 from 
			v_join;	
	get diagnostics v_rc = row_count;
	perform  log.write_log('',0 ,'ddwh_ods.load_dim_work()','dwh_ods.dim_work', 'insert', v_rc);
	raise notice '% rows inserted into task_data', v_rc;
	perform log.write_log('',0 , 'ddwh_ods.load_dim_work()','', 'stop', 0);
	return true;
end $function$;

/*
* Схема для витрины данных
*/

create schema report;
/*
* Восстановим текущее состояние исходной таблицы на основании слоя dwh_ods
*/
create or replace
view report.v_recover_request_data as
select
	id,
	status_nm,
	create_dttm,
	valid_from_dttm update_dttm
from
	dwh_ods.request_data_hist
where
	not(deleted_flg)
	and valid_to_dttm = '2999-12-31 23:59:59';
	
/*
* Создадим представление  в котором в разрезе дней недели сгруппируем агрегированные  данные из таблицы dwh_ods.dim_work 
*/	
create or replace view report.v_agg_per_week as
select
	day_of_week,
	sum(complited_per_day) complited,
	sum(canselled_per_day) canselled,
	sum(accepted_per_day) accepted,
	sum(paused_per_day) paused,
	sum(deleted_per_day) deleted,
	sum(executed_during_day) executed_during_day,
	sum(paused_during_day) paused_during_day
from
	dwh_ods.dim_work
group by
	day_of_week
order by
	day_of_week;
/*
* Создадим представление  показывающее разницу между исходными даными и восстановленными на основании детального слоя в хранилище
*/		
create or replace view report.v_dif_source_target as	
	select 'report' source, a.* from report.v_recover_request_data a
	except
	select 'report' source, b.* from oltp_src_system.request_data b
	union
	select 'src' source, b.* from oltp_src_system.request_data b
	except
	select 'src' source, a.* from report.v_recover_request_data a
	order by id, create_dttm, update_dttm  ;	
/*
* Создадим представление  показывающее частичную разницу между исходными даными и восстановленными на основании детального слоя в хранилище
* без поля update_dttm
*/		
create or replace view report.v_dif_source_target_partial as	
	select 'report' source, a.id, a.status_nm, a.create_dttm  from report.v_recover_request_data a
	except
	select 'report' source, b.id, b.status_nm, b.create_dttm from oltp_src_system.request_data b
	union
	select 'src' source, b.id, b.status_nm, b.create_dttm from oltp_src_system.request_data b
	except
	select 'src' source, a.id, a.status_nm, a.create_dttm from report.v_recover_request_data a
	order by id, create_dttm  ;
/*
* Создадим представление в котором  разместим матрицу корреляций между агрегированными данными из таблицы dwh_ods.dim_work
*/

create or replace view report.v_corr as
select
	'complited_per_day' agg_name,
	corr(complited_per_day,
	complited_per_day) complited_per_day,
	corr(complited_per_day,
	canselled_per_day) canselled_per_day,
	corr(complited_per_day,
	accepted_per_day) accepted_per_day,
	corr(complited_per_day,
	paused_per_day) paused_per_day,
	corr(complited_per_day,
	deleted_per_day) deleted_per_day,
	corr(complited_per_day,
	executed_during_day) executed_during_day,
	corr(complited_per_day,
	paused_during_day) paused_during_day
from
	dwh_ods.dim_work
union
select
	'canselled_per_day' agg_name,
	corr(canselled_per_day,
	complited_per_day) complited_per_day,
	corr(canselled_per_day,
	canselled_per_day) canselled_per_day,
	corr(canselled_per_day,
	accepted_per_day) accepted_per_day,
	corr(canselled_per_day,
	paused_per_day) paused_per_day,
	corr(canselled_per_day,
	deleted_per_day) deleted_per_day,
	corr(complited_per_day,
	executed_during_day) executed_during_day,
	corr(complited_per_day,
	paused_during_day) paused_during_day
from
	dwh_ods.dim_work
union
select
	'accepted_per_day' agg_name,
	corr(accepted_per_day,
	complited_per_day) complited_per_day,
	corr(accepted_per_day,
	canselled_per_day) canselled_per_day,
	corr(accepted_per_day,
	accepted_per_day) accepted_per_day,
	corr(accepted_per_day,
	paused_per_day) paused_per_day,
	corr(accepted_per_day,
	deleted_per_day) deleted_per_day,
	corr(accepted_per_day,
	executed_during_day) executed_during_day,
	corr(accepted_per_day,
	paused_during_day) paused_during_day
from
	dwh_ods.dim_work
union
select
	'paused_per_day' agg_name,
	corr(paused_per_day,
	complited_per_day) complited_per_day,
	corr(paused_per_day,
	canselled_per_day) canselled_per_day,
	corr(paused_per_day,
	accepted_per_day) accepted_per_day,
	corr(paused_per_day,
	paused_per_day) paused_per_day,
	corr(paused_per_day,
	deleted_per_day) deleted_per_day,
	corr(paused_per_day,
	executed_during_day) executed_during_day,
	corr(paused_per_day,
	paused_during_day) paused_during_day
from
	dwh_ods.dim_work
union
select
	'deleted_per_day' agg_name,
	corr(deleted_per_day,
	complited_per_day) complited_per_day,
	corr(deleted_per_day,
	canselled_per_day) canselled_per_day,
	corr(deleted_per_day,
	accepted_per_day) accepted_per_day,
	corr(deleted_per_day,
	paused_per_day) paused_per_day,
	corr(deleted_per_day,
	deleted_per_day) deleted_per_day,
	corr(deleted_per_day,
	executed_during_day) executed_during_day,
	corr(deleted_per_day,
	paused_during_day) paused_during_day
from
	dwh_ods.dim_work
union
select
	'executed_during_day' agg_name,
	corr(executed_during_day,
	complited_per_day) complited_per_day,
	corr(executed_during_day,
	canselled_per_day) canselled_per_day,
	corr(executed_during_day,
	accepted_per_day) accepted_per_day,
	corr(executed_during_day,
	paused_per_day) paused_per_day,
	corr(executed_during_day,
	deleted_per_day) deleted_per_day,
	corr(executed_during_day,
	executed_during_day) executed_during_day,
	corr(executed_during_day,
	paused_during_day) paused_during_day
from
	dwh_ods.dim_work
union	
select
	'paused_during_day' agg_name,
	corr(paused_during_day,
	complited_per_day) complited_per_day,
	corr(paused_during_day,
	canselled_per_day) canselled_per_day,
	corr(paused_during_day,
	accepted_per_day) accepted_per_day,
	corr(paused_during_day,
	paused_per_day) paused_per_day,
	corr(paused_during_day,
	deleted_per_day) deleted_per_day,
	corr(paused_during_day,
	executed_during_day) executed_during_day,
	corr(paused_during_day,
	paused_during_day) paused_during_day
from
	dwh_ods.dim_work
where
	date_actual <= now();

  
 
/*
*  При помощи airflow данные будут сгенерированы всего за пару дней,
* для тестирования хочется побольше.
* Сделаем эмуляторы ситетичестих генераторов
* Для этого: сделаем модифицированые генераторы
*/




create or replace function oltp_src_system.mod_create_request(dt timestamp)
	returns boolean
	language plpgsql
	as $function$
declare
    v_rc int;
	max_id int;
	temp_time timestamp;
begin	
	temp_time = now()::timestamp;
    perform log.write_log('', 0, 'oltp_src_system.mod_create_request()', '', 'start',0 ,(dt + ((now()::timestamp-temp_time)))::timestamp) ;
	select id into max_id from coalesce((select	max(id) from oltp_src_system.request_data) + 1, 1) id ;
	create temp table t_gen_request as
    select
      id,
      'accepted' status_nm,
      (dt + (now()::timestamp-temp_time))::timestamp create_dttm,
      (dt + (now()::timestamp-temp_time))::timestamp update_dttm
	from
		(select	n id from	generate_series(max_id , max_id + round(random()* 3)::int) n) nn;

	insert
		into
			oltp_src_system.request_data (id, status_nm, create_dttm, update_dttm)
    select * from t_gen_request;

	get diagnostics v_rc = row_count;
	perform log.write_log('', 0, 'oltp_src_system.mod_create_request()', 'oltp_src_system.request_data', 'insert', v_rc, (dt + (now()::timestamp-temp_time))::timestamp);

	insert
		into
			oltp_cdc_src_system.request_data_changes
	select
		tgr.*,
		'I',
		(dt + (now()::timestamp-temp_time))::timestamp
	from
		t_gen_request tgr;

	get diagnostics v_rc = row_count;
	perform log.write_log('', 0, 'oltp_src_system.mod_create_request()', 'oltp_cdc_src_system.request_data_changes', 'insert', v_rc, (dt + (now()::timestamp-temp_time))::timestamp);

	raise notice '% rows inserted into task_data', v_rc;

	drop table t_gen_request;
	perform log.write_log('', 0, 'oltp_src_system.mod_create_request', '', 'stop', 0, (dt + (now()::timestamp-temp_time))::timestamp) ;

	return true;
end $function$
;

create or replace function oltp_src_system.mod_update_existed_request(dt timestamp)
	returns boolean
	language plpgsql
	as $function$
declare
    v_rc int;
	rnd_num int;
	temp_time timestamp;
begin   
	temp_time = now()::timestamp;
	perform log.write_log('',0 , 'oltp_src_system.mod_update_existed_request()', '', 'start', 0, (dt + (now()::timestamp-temp_time))::timestamp);

	create temp table t_id as
		select
			id
		from
			(
				select
					id,
					round(random()* 10) rnd
				from
					oltp_src_system.request_data
				where
					status_nm not in ('complited', 'canselled')
            ) rnd_tbl
		where
			(rnd - floor(rnd / 10)) = 1 ;

	rnd_num = floor(random() * 4 + 1)::int;

	update
		oltp_src_system.request_data
	set
		status_nm = case rnd_num                     
                	    when 1 then 'executed'
						when 2 then 'paused'
						when 3 then 'complited'
						when 4 then 'canselled'
						else null
					end,
	update_dttm = (dt + (now()::timestamp-temp_time))::timestamp
	where
	 id in (	select	id from	t_id);

	get diagnostics v_rc = row_count;
	perform log.write_log('', 0 , 'oltp_src_system.mod_update_existed_request()', 'oltp_src_system.request_data', 'update', v_rc, (dt + (now()::timestamp-temp_time))::timestamp);

	insert
	into
		oltp_cdc_src_system.request_data_changes
	select
		rd.*,
		'U',
		(dt + (now()::timestamp-temp_time))::timestamp
	from
		oltp_src_system.request_data rd
	where
		id in (
				select
					id
				from
					t_id
				);

	get diagnostics v_rc = row_count;

	perform log.write_log('', 0 , 'oltp_src_system.mod_update_existed_request()', 'oltp_cdc_src_system.request_data_changes', 'insert', v_rc, (dt + (now()::timestamp-temp_time))::timestamp);

	drop table t_id;

	perform log.write_log('', 0 , 'oltp_src_system.mod_update_existed_request()', '', 'stop', 0, (dt + (now()::timestamp-temp_time))::timestamp);

	raise notice '% rows updated into task_data', v_rc;

	return true;
end $function$
;
create or replace function oltp_src_system.mod_deleted_existed_request(dt timestamp)
	returns boolean
	language plpgsql
	as $function$
declare
    v_rc int;
	temp_time timestamp;
begin                   
/*
* удаляем старые закрытые или отмененные задачи
*/
	temp_time = now()::timestamp;
	perform log.write_log('', 0 , 'oltp_src_system.mod_deleted_existed_request()','','start',0,(dt + (now()::timestamp-temp_time))::timestamp);

	create temp table t_id as
	select
		id
	from
		oltp_src_system.request_data
	where
		status_nm in ('complited', 'canselled')
		and update_dttm in
			(
				select
					min(update_dttm)
				from
					oltp_src_system.request_data
				where
					status_nm in ('complited', 'canselled')                      
            );

	insert
		into
			oltp_cdc_src_system.request_data_changes
	select
		rd.*,
		'D',
		(dt + (now()::timestamp-temp_time))::timestamp
	from
		oltp_src_system.request_data rd
	where
		id in
			(
				select
					id
				from
					t_id
			);

	get diagnostics v_rc = row_count;
	perform log.write_log('', 0 , 'oltp_src_system.mod_deleted_existed_request()', 'oltp_cdc_src_system.request_data_changes', 'insert', v_rc, (dt + (now()::timestamp-temp_time))::timestamp);

	delete 
		from
			oltp_src_system.request_data
		where
			id in
				(
					select
						id
					from
						t_id
				);

	get diagnostics v_rc = row_count;
	perform log.write_log('',0 ,'oltp_src_system.mod_deleted_existed_request()','oltp_src_system.request_data','delete',v_rc,(dt + (now()::timestamp-temp_time))::timestamp);
	drop table t_id;
	perform log.write_log('',0 ,'oltp_src_system.mod_deleted_existed_request()','','stop',0,(dt + (now()::timestamp-temp_time))::timestamp);
	raise notice '% rows deleted into task_data',v_rc;
	return true;
end $function$
;


/*
* Эмулируем работу генераторов данных за период от начала года по текущую дату.
* Запуск ежедневно в случайное время
*/

/*
*не забыть перед вызовом отключить тригеры,
*затем включить, либо ession_replication_role,
* либо удалить а потом создать тригер
*/
set session_replication_role = replica;
	with gen_dt as
	(
	select
		dt + random() * (timestamp '2023-01-01 23:59:59' -
                   timestamp '2023-01-01 00:00:00') as dt
	from
		generate_series('2023-01-01 00:00'::timestamp ,
		now()::timestamp,
		'1 day') dt
	)                       
	select
		oltp_src_system.mod_create_request(dt),
		oltp_src_system.mod_update_existed_request(dt + interval '1 sec'),
		oltp_src_system.mod_deleted_existed_request(dt + interval '2 sec')		
	from 
		gen_dt
	order by dt; 
set	session_replication_role = default;
