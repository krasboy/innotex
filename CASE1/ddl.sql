create schema example;

create table dict_account_branch(
account_branch_cd text not null,
account_branch_name text not null,
account_branch_adr text not null,
effective_from_date date default '1900-01-01' ,
effective_to_date date default'2999-12-31',
"user" text default current_user,
deleted_flg bool  default false,
processed_dttm timestamp default CURRENT_TIMESTAMP,
primary key(account_branch_cd,effective_from_date ));

create table dict_counterparty_type(
counterparty_type_cd text primary key ,
counterparty_type_name text,
"user" text default current_user,
deleted_flg bool  default false,
processed_dttm timestamp default CURRENT_TIMESTAMP);

create table counterparty(
counterparty_rk int ,
counterparty_type_cd text not null  references dict_counterparty_type ( counterparty_type_cd ),
counterparty_inn text not null,
counterparty_open_date date not null default current_date,
counterparty_resident_flg bool not null default true,
counterparty_name text not null,
effective_from_date date default '1900-01-01' ,
effective_to_date date default'2999-12-31',
"user" text default current_user,
deleted_flg bool  default false,
processed_dttm timestamp default CURRENT_TIMESTAMP,
primary key (counterparty_rk,effective_from_date));

create table dict_account_currency(
account_currency_cd text primary key ,
account_currency_name text not null,
account_currency_strcode text,
account_currency_country text,
"user" text default current_user,
deleted_flg bool  default false,
processed_dttm timestamp default CURRENT_TIMESTAMP);

create table account(
account_rk text ,
counterparty_rk int ,--references counterparty ( counterparty_rk ),
account_descr text, 
account_currency_cd  text not null references dict_account_currency (account_currency_cd),
account_branch_cd text references dict_account_branch (account_branch_cd),
effective_from_date date default '1900-01-01' ,
effective_to_date date default'2999-12-31',
"user" text default current_user,
deleted_flg bool  default false,
processed_dttm timestamp default CURRENT_TIMESTAMP,
primary key(account_rk,effective_from_date));

create table dict_account_plan(
account_plan_cd text ,
parent_account_plan_cd text ,
account_plan_name text ,
account_plan_attribute text,
effective_from_date date default '1900-01-01' ,
effective_to_date date default'2999-12-31',
"user" text default current_user,
deleted_flg bool  default false,
processed_dttm timestamp default CURRENT_TIMESTAMP,
primary key(account_plan_cd,effective_from_date ));

create table account_x_dict_account_plan(
account_rk text, ---- references account(account_rk),
account_plan_cd text,-- references dict_account_plan(account_plan_cd) ,
effective_from_date date default '1900-01-01' ,
effective_to_date date default'2999-12-31',
"user" text default current_user,
deleted_flg bool  default false,
processed_dttm timestamp default CURRENT_TIMESTAMP,
primary key (account_rk,account_plan_cd,effective_from_date ));

create table dict_account_status(
account_status_rk serial  primary key,
account_status_name text not null,
"user" text default current_user,
deleted_flg bool  default false,
processed_dttm timestamp default CURRENT_TIMESTAMP);

create table account_x_dict_account_status(
account_rk  text,-- references account(account_rk),
account_status_rk int,-- references dict_account_status(account_status_rk) ,
effective_from_date date default '1900-01-01' ,
effective_to_date date default'2999-12-31',
"user" text default current_user,
deleted_flg bool  default false,
processed_dttm timestamp default CURRENT_TIMESTAMP,
primary key (account_rk,account_status_rk,effective_from_date ));
