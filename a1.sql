create table {ost_business_dt}
as 
with  business_dt as (
                select * from {ad_records} a
                where 1=1
                    and a.report_dt = '{dt}'
            )
    , t as 
            (
            select 
                    a1.c_client cft_id
                , a.acc_ost  
                , a1.c_unique_code 
                , a.ost
                , d.c_currency               -- Валюта договора
                , d.c_date_begin             -- Дата начала действия договора
                , d.c_date_end               -- Дата окончания договора
                , d.c_date_close             -- Дата закрытия договора
                , ta.c_name as acc_type_name --тип счета
                , a.report_dt
        
            from business_dt a
            join {r2_ac_fin} a1 on a1.c_main_v_id = a.acc_ost 
            left join {z_r2_acc} a2 on a1.c_unique_code = a2.c_unique_code
            left join {r2_type_acc} ta on ta.id = a2.c_type_acc
            left join {r2_deposit} d on a2.c_product = d.id
            left join {r2_vid_deposit} vd on vd.id = d.c_vid_deposit
            )
select * from t