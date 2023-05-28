
insert into dict_account_status(account_status_name) values('Закрыт');
insert into dict_account_status(account_status_name) values('Открыт');
insert into dict_account_status(account_status_name) values('Заблокирован');
insert into dict_account_status(account_status_name) values('Арестован');
insert into dict_account_status(account_status_name) values('Зарезервирован');

insert into dict_counterparty_type(counterparty_type_cd,counterparty_type_name) values ('0','Юридическое лицо');
insert into dict_counterparty_type(counterparty_type_cd,counterparty_type_name) values ('1','Физическое  лицо');

insert into counterparty(counterparty_rk, counterparty_type_cd,counterparty_inn,  counterparty_name)
values (0,'0','7715805253', 'ООО "АБРАДОКС"');
insert into counterparty(counterparty_rk, counterparty_type_cd,counterparty_inn,  counterparty_name)
values (1,'0','7706107510', 'ПАО "НК Роснефть"');
insert into counterparty(counterparty_rk, counterparty_type_cd,counterparty_inn,  counterparty_name)
values (2,'0','7106059039', 'ООО "ГОЛДОВЪ"');
insert into counterparty(counterparty_rk, counterparty_type_cd,counterparty_inn,  counterparty_name)
values (3,'1','780100325933', 'Иванов Иван Иванович');
insert into counterparty(counterparty_rk, counterparty_type_cd,counterparty_inn,  counterparty_name, counterparty_resident_flg)
values (4,'0','9909123486', 'ДЖИ ЭС ЭЛЬ КОНСАЛТИНГ', false);

insert into counterparty(counterparty_rk, counterparty_type_cd,counterparty_inn,  counterparty_name)
values (5,'0','7704182141' ,'ООО "РЛП-ЯРМАРКА" '); --pc 40702810700000001045
insert into counterparty(counterparty_rk, counterparty_type_cd,counterparty_inn,  counterparty_name, counterparty_resident_flg)
values (6,'1','526211497460', 'Захар Петрович Заграницын', false);
insert into dict_account_branch(account_branch_cd, account_branch_name, account_branch_adr, effective_from_date) values
('0081','"Центральный" в г. Москве','107031, г. Москва, ул. Рождественка, д. 10/2, строен. 1','2016-03-25');
insert into dict_account_branch(account_branch_cd, account_branch_name, account_branch_adr, effective_from_date) values
('0094','№ 2351 в г. Краснодаре','350000, г. Краснодар, Центральный внутригородской округ, ул. Красноармейская / ул. им. Гоголя, дом № 43/68','2017-07-13');
insert into dict_account_branch(account_branch_cd, account_branch_name, account_branch_adr, effective_from_date) values
('0096','№ 3652 в г. Воронеже','394030, г. Воронеж, ул. Кольцовская, д. 31','2017-07-13');

insert into account(account_rk, counterparty_rk,account_currency_cd,account_branch_cd, effective_from_date ) values 
('40817810100810000001',3,810,'0081','2020-12-12'); --карточный резидента ФЛ 40817 +
insert into account(account_rk, counterparty_rk,account_currency_cd,account_branch_cd, effective_from_date ) values 
('40820810100810000002',6,810,'0081','2022-10-13');--карточный нерезидента ФЛ 40820 +
insert into account(account_rk, counterparty_rk,account_currency_cd,account_branch_cd, effective_from_date ) values 
('45206810100940000003',0,810,'0094','2020-12-13'); --кредит ЮЛ 45206
insert into account(account_rk, counterparty_rk,account_currency_cd,account_branch_cd, effective_from_date ) values 
('42301810100810000004',1,810,'0081','2019-01-15'); --депозит  ФЛ 42301
insert into account(account_rk, counterparty_rk,account_currency_cd,account_branch_cd, effective_from_date ) values 
('40702810100960000005',2,810,'0096','2018-06-17'); -- РКО 40702
insert into account(account_rk, counterparty_rk,account_currency_cd,account_branch_cd, effective_from_date ) values 
('45206810100810000006',5,810,'0081','2019-07-18'); -- кредит ЮЛ 45206
insert into account(account_rk, counterparty_rk,account_currency_cd,account_branch_cd, effective_from_date ) values 
('45601810100810000007',4,810,'0081','2023-04-02'); -- кредит ЮЛ нерезиденту 45601

insert into  account_x_dict_account_status(account_rk, account_status_rk, effective_from_date)
values ('40817810100810000001',3,current_date);
insert into  account_x_dict_account_status(account_rk, account_status_rk, effective_from_date)
values ('40820810100810000002',3,current_date);
insert into  account_x_dict_account_status(account_rk, account_status_rk, effective_from_date)
values ('45206810100940000003',3,current_date);
insert into  account_x_dict_account_status(account_rk, account_status_rk, effective_from_date)
values ('42301810100810000004',3,current_date);
insert into  account_x_dict_account_status(account_rk, account_status_rk, effective_from_date)
values ('40702810100960000005',3,current_date);
insert into  account_x_dict_account_status(account_rk, account_status_rk, effective_from_date)
values ('45206810100810000006',3,current_date);
insert into  account_x_dict_account_status(account_rk, account_status_rk, effective_from_date)
values ('45601810100810000007',3,current_date);

insert into account_x_dict_account_plan(account_rk , account_plan_cd) 
values ('40817810100810000001','40817');
insert into account_x_dict_account_plan(account_rk , account_plan_cd) 
values ('40817810100810000001','20208');
insert into account_x_dict_account_plan(account_rk , account_plan_cd) 
values ('40820810100810000002','40820');
insert into account_x_dict_account_plan(account_rk , account_plan_cd) 
values ('40820810100810000002','20208'); 
insert into account_x_dict_account_plan(account_rk , account_plan_cd) 
values ('45206810100940000003','45206');
insert into account_x_dict_account_plan(account_rk , account_plan_cd) 
values ('45206810100940000003','40702'); 
insert into account_x_dict_account_plan(account_rk , account_plan_cd) 
values ('42301810100810000004' ,'42301');
insert into account_x_dict_account_plan(account_rk , account_plan_cd) 
values ('42301810100810000004' ,'40817');
insert into account_x_dict_account_plan(account_rk , account_plan_cd) 
values ('40702810100960000005' ,'40702');
insert into account_x_dict_account_plan(account_rk , account_plan_cd) 
values ('40702810100960000005' ,'20202');
insert into account_x_dict_account_plan(account_rk , account_plan_cd) 
values ('45206810100810000006' ,'45206');
insert into account_x_dict_account_plan(account_rk , account_plan_cd) 
values ('45206810100810000006' ,'40702');
insert into account_x_dict_account_plan(account_rk , account_plan_cd) 
values ('45206810100810000007' ,'45601');
insert into account_x_dict_account_plan(account_rk , account_plan_cd) 
values ('45206810100810000007' ,'40807');

