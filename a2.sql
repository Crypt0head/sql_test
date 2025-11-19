with vtb as (

                select distinct cast(id2 as decimal(18)) cft_id from {vtb_clients} vtb

        )

        , br as (
                select *
                        from {rsb_bo_iis} iis
                        join {z_product} pr on pr.id = iis.id
                        where c_date_begin < '{dt}'
                        )
        , k as (
                select acc_ost
                from {ad_records} a
                where 1=1
                group by acc_ost
        )
        
        , m as(
                    select 
                         a1.c_client cft_id
                        , a1.c_main_v_id  
                    from k
                    join {r2_ac_fin} a1 on a1.c_main_v_id = k.acc_ost
                    left join {z_r2_acc} a2 on a1.c_unique_code = a2.c_unique_code
                    left join {r2_type_acc} ta on ta.id = a2.c_type_acc
                    left join {r2_deposit} d on a2.c_product = d.id
                    left join {r2_vid_deposit} vd on vd.id = d.c_vid_deposit)

            , move as (
                    select 
                          m.cft_id
                        , m.type_plan
                        , sum(sum_spends_last_month) sum_spends_last_month
                        , sum(cnt_spends_last_month) cnt_spends_last_month
                        , round(sum(sum_spends_last_month)/ nvl(nullif(sum(cnt_spends_last_month),0), 1), 2) avg_spends_last_month
                        , max(max_spends_last_month) max_spends_last_month
                        , sum(sum_income_last_month) sum_income_last_month
                        , max(max_income_last_month) max_income_last_month
                    from m
                    group by m.cft_id
                            , m.type_plan
                            )
                            
            , k1 as (
                    select acc_ost
                    from {ad_records} a
                    where 1=1
                    group by acc_ost)
            
            
            , m1 as(
                            select *
                            from k1
                            join {r2_ac_fin} a1 on a1.c_main_v_id = k1.acc_ost
                            left join {z_r2_acc} a2 on a1.c_unique_code = a2.c_unique_code
                            left join {r2_type_acc} ta on ta.id = a2.c_type_acc
                            left join {r2_deposit} d on a2.c_product = d.id
                            left join {r2_vid_deposit} vd on vd.id = d.c_vid_deposit
                            )

         , final as (
           select *
        from {phyz_acc_ost} t
         left join {z_client} al on al.id = t.cft_id
         left join {z_cl_group} gr on al.c_rsb_cl_cat = gr.id
         left join vtb on vtb.cft_id = t.cft_id
         left join br on br.cft_id = t.cft_id and br.rn=1
         left join move mkk on mkk.cft_id = t.cft_id and mkk.type_plan = 'КК'
         left join move mdk on mdk.cft_id = t.cft_id and mdk.type_plan = 'ДК'
         left join move_month_ago makk on makk.cft_id = t.cft_id and makk.type_plan = 'КК'
         left join move_month_ago madk on madk.cft_id = t.cft_id and madk.type_plan = 'ДК'
         where t.business_dt = to_date('{dt}')
         )