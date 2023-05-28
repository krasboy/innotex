CREATE OR replace PROCEDURE do_account_desc()
LANGUAGE SQL
AS $$
update example.account
  set account_descr=
		((select account_plan_name from dict_account_plan where account_plan_cd = substring(account_rk for 3))||' / '||
		(select account_plan_name from dict_account_plan where account_plan_cd = substring(account_rk for 5)));
$$;
