BEGIN TRANSACTION;

MERGE INTO gdl_layer.impulse_customer_dim AS gdl
USING (
       SELECT *
       FROM   (
               SELECT CASE WHEN g.customer_key IS NOT NULL
                           THEN g.customer_key
                           ELSE m.max_customer_key + ROW_NUMBER() OVER(ORDER BY g.customer_key,
                                                                                c.branch_customer_number_bk,
                                                                                c.company_code_bk)
                      END                                                                                   AS new_customer_key,
                      c.branch_customer_number_bk,
                      c.company_code_bk,
                      c.branch_number,
                      c.customer_number,
                      c.customer_name,
                      c.credit_limit_amt,
                      c.current_due_amt,
                      c.terms_desc,
                      c.bump_rate,
                      c.customer_mtd_sales,
                      c.customer_mtd_adj_cost,
                      c.customer_ytd_sales,
                      c.customer_ytd_adj_cost,
                      c.customer_prev_year_sales,
                      c.customer_prev_year_adj_cost,
                      c.contact_name,
                      c.phone_number,
                      c.credit_review_date,
                      c.total_balance_amt,
                      c.currency_code,
                      c.country_code,
                      c.state_code,
                      c.county_code,
                      c.city_code,
                      c.customer_group_code,
                      c.end_user_flag,
                      c.master_customer_name,
                      c.cust_location,
                      c.load_mod_code                                                                       AS cor_load_mod_code,
                      g.load_mod_code                                                                       AS gdl_load_mod_code,
                      c.src_sys_crt_date,
                      c.src_sys_clos_date,
                      c.db_rec_del_flag,
                      SHA256(FORMAT("%t", STRUCT(c.branch_customer_number_bk,
                                                 c.company_code_bk,
                                                 c.branch_number,
                                                 c.customer_number,
                                                 c.customer_name,
                                                 c.credit_limit_amt,
                                                 c.current_due_amt,
                                                 c.terms_desc,
                                                 c.bump_rate,
                                                 c.customer_mtd_sales,
                                                 c.customer_mtd_adj_cost,
                                                 c.customer_ytd_sales,
                                                 c.customer_ytd_adj_cost,
                                                 c.customer_prev_year_sales,
                                                 c.customer_prev_year_adj_cost,
                                                 c.contact_name,
                                                 c.phone_number,
                                                 c.credit_review_date,
                                                 c.total_balance_amt,
                                                 c.currency_code,
                                                 c.country_code,
                                                 c.state_code,
                                                 c.county_code,
                                                 c.city_code,
                                                 c.customer_group_code,
                                                 c.end_user_flag,
                                                 c.master_customer_name,
                                                 c.cust_location)))                                         AS cor_hash,
                      SHA256(FORMAT("%t", STRUCT(g.branch_customer_number_bk,
                                                 g.company_code_bk,
                                                 g.branch_number,
                                                 g.customer_number,
                                                 g.customer_name,
                                                 g.credit_limit_amt,
                                                 g.current_due,
                                                 g.terms_desc,
                                                 g.bump_rate,
                                                 g.customer_mtd_sales,
                                                 g.customer_mtd_adj_cost,
                                                 g.customer_ytd_sales,
                                                 g.customer_ytd_adj_cost,
                                                 g.customer_prev_year_sales,
                                                 g.customer_prev_year_adj_cost,
                                                 g.contact_name,
                                                 g.phone_number,
                                                 g.credit_review_date,
                                                 g.total_balance_amt,
                                                 g.currency_code,
                                                 g.country_code,
                                                 g.state_code,
                                                 g.county_code,
                                                 g.city_code,
                                                 g.customer_group_code,
                                                 g.end_user_flag,
                                                 g.master_customer_name,
                                                 g.cust_location)))                                         AS gdl_hash
               FROM   (
                       WITH ck_locn
                            AS (
                                SELECT customer_key
                                FROM   core_impulse.impulse_customer_location AS c
                                WHERE  batch_number      = (SELECT batch_number FROM audit_database.batch_control)
                                  AND  db_rec_close_date = CAST('9999-12-31' AS DATE)
                                  AND  EXISTS
                                       (
                                        SELECT 1
                                        FROM   audit_database.load_control    AS lc
                                        WHERE  lc.source_system   = 'impulse'
                                          AND  lc.source_table    = 'impulse_customer_location'
                                          AND  lc.company_code    = c.company_code_bk
                                          AND  lc.ssot_layer      = 'core'
                                          AND  lc.load_batch_date = (SELECT batch_date FROM audit_database.batch_control)
                                       )
                                  AND  NOT EXISTS
                                       (
                                        SELECT 1
                                        FROM   audit_database.load_control    AS lc
                                        WHERE  lc.source_system   = 'impulse'
                                          AND  lc.source_table    = 'impulse_customer_location'
                                          AND  lc.company_code    = c.company_code_bk
                                          AND  lc.ssot_layer      = 'gdl'
                                          AND  lc.load_batch_date = (SELECT batch_date FROM audit_database.batch_control)
                                       )
                                GROUP BY 1
                               ),
                            ck_cust
                            AS (
                                SELECT customer_key
                                FROM   core_impulse.impulse_customer       AS c
                                WHERE  batch_number      = (SELECT batch_number FROM audit_database.batch_control)
                                  AND  db_rec_close_date = CAST('9999-12-31' AS DATE)
                                  AND  EXISTS
                                       (
                                        SELECT 1
                                        FROM   audit_database.load_control AS lc
                                        WHERE  lc.source_system   = 'impulse'
                                          AND  lc.source_table    = 'impulse_customer'
                                          AND  lc.company_code    = c.company_code_bk
                                          AND  lc.ssot_layer      = 'core'
                                          AND  lc.load_batch_date = (SELECT batch_date FROM audit_database.batch_control)
                                       )
                                  AND  NOT EXISTS
                                       (
                                        SELECT 1
                                        FROM   audit_database.load_control AS lc
                                        WHERE  lc.source_system   = 'impulse'
                                          AND  lc.source_table    = 'impulse_customer'
                                          AND  lc.company_code    = c.company_code_bk
                                          AND  lc.ssot_layer      = 'gdl'
                                          AND  lc.load_batch_date = (SELECT batch_date FROM audit_database.batch_control)
                                       )
                                GROUP BY 1
                               ),
                            locn
                            AS (
                                SELECT customer_key,
                                       ARRAY_AGG(STRUCT(UPPER(TO_HEX(SHA256(FORMAT("%t", STRUCT(customer_suffix_bk,
                                                                                                cust_add_1_desc,
                                                                                                cust_add_2_desc,
                                                                                                cust_add_3_desc,
                                                                                                cust_city,
                                                                                                state,
                                                                                                zip,
                                                                                                country_code,
                                                                                                load_mod_code))))) AS locn_bus_col_set_hash,
                                                        customer_suffix_bk,
                                                        cust_add_1_desc,
                                                        cust_add_2_desc,
                                                        cust_add_3_desc,
                                                        cust_city,
                                                        state,
                                                        zip,
                                                        country_code,
                                                        load_mod_code) ORDER BY customer_suffix_bk)                                         AS cust_location
                                FROM   core_impulse.impulse_customer_location AS l
                                WHERE  db_rec_close_date = CAST('9999-12-31' AS DATE)
                                  AND  (
                                            EXISTS
                                            (
                                             SELECT 1
                                             FROM   ck_locn                   AS cl
                                             WHERE  cl.customer_key = l.customer_key
                                            )
                                        OR  EXISTS
                                            (
                                             SELECT 1
                                             FROM   ck_cust                   AS cc
                                             WHERE  cc.customer_key = l.customer_key
                                            )
                                       )
                                GROUP BY 1
                               )
                       SELECT c.branch_customer_number_bk,
                              c.company_code_bk,
                              c.branch_number,
                              c.customer_number,
                              c.customer_name,
                              c.credit_limit_amt,
                              c.current_due_amt,
                              c.terms_desc,
                              c.bump_rate,
                              c.customer_mtd_sales,
                              c.customer_mtd_adj_cost,
                              c.customer_ytd_sales,
                              c.customer_ytd_adj_cost,
                              c.customer_prev_year_sales,
                              c.customer_prev_year_adj_cost,
                              c.contact_name,
                              c.phone_number,
                              c.credit_review_date,
                              c.total_balance_amt,
                              c.currency_code,
                              c.country_code,
                              c.state_code,
                              c.county_code,
                              c.city_code,
                              c.customer_group_code,
                              c.end_user_flag,
                              c.master_customer_name,
                              l.cust_location,
                              c.load_mod_code,
							  /*new col*/
							  (select max(core.customer_key) from core_impulse.impulse_customer core, 
                imgcp-20220521020353.gdl_layer.impulse_customer_dim gdl
	where 		core.branch_customer_number_bk = gdl.branch_customer_number_bk 
			AND core.company_code_bk = gdl.company_code_bk) as master_customer_key,
				/* TERM_DAYS (NEW_COLUMN ADDING) */
								CASE   
									WHEN c.terms IN ('001','201') THEN 1  
									WHEN c.terms IN ('003','793','998','999') THEN 3  
									WHEN c.terms IN ('005','006','794') THEN 5  
									WHEN c.terms IN ('066','106','806') THEN 6  
									WHEN c.terms IN ('000','002','004','007','108','115','175','207','315','350','362','363','795') THEN 7  
									WHEN c.terms IN ('010','104','110','111','117','119','210','796','801','802','817') THEN 10  
									WHEN c.terms IN ('212') THEN 12  
									WHEN c.terms IN ('100','101','126','316','797','808') THEN 15  
									WHEN c.terms IN ('318') THEN 18  
									WHEN c.terms IN ('319') THEN 19  
									WHEN c.terms IN ('102','200','220','798') THEN 20  
									WHEN c.terms IN ('245','825') THEN 25  
									WHEN c.terms IN ('813') THEN 26  
									WHEN c.terms IN ('011','013','103','112','113','121','122','125','130','215','231','232','300', '306','310','311','320','337','340','344','345','346','351','355', '361','505', '530','799','804','807','809','810','811','815','992') THEN 30  
									WHEN c.terms IN ('803','805','903') THEN 31  
									WHEN c.terms IN ('127','335','792') THEN 35  
									WHEN c.terms IN ('537') THEN 37  
									WHEN c.terms IN ('230','400','790') THEN 40  
									WHEN c.terms IN ('045','105','500','545','645','791','812','814','816','845') THEN 45  
									WHEN c.terms IN ('050','114','550') THEN 50  
									WHEN c.terms IN ('107','560','600','630','860') THEN 60  
									WHEN c.terms IN ('165') THEN 65  
									WHEN c.terms IN ('070') THEN 70  
									WHEN c.terms IN ('075') THEN 75  
									WHEN c.terms IN ('885') THEN 85  
									WHEN c.terms IN ('090','109','890') THEN 90  
									WHEN c.terms IN ('120','820') THEN 120  
									WHEN c.terms IN ('150') THEN 150  
									WHEN c.terms IN ('180') THEN 180  
									ELSE NULL END AS term_days,
				/* STATE_ID (NEW COLUMN ADDING) */
								CASE   
									WHEN c.company_code_bk  = 'MD' AND c.state_code  = 'AL' THEN 1  
									WHEN c.company_code_bk  = 'MD' AND c.state_code  = 'AK' THEN 2  
									WHEN c.company_code_bk  = 'MD' AND c.state_code  = 'AZ' THEN 3  
									WHEN c.company_code_bk  = 'MD' AND c.state_code  = 'AR' THEN 4  
									WHEN c.company_code_bk  = 'MD' AND c.state_code  = 'CA' THEN 5  
									WHEN c.company_code_bk  = 'MD' AND c.state_code  = 'CO' THEN 6  
									WHEN c.company_code_bk  = 'MD' AND c.state_code  = 'CT' THEN 7  
									WHEN c.company_code_bk  = 'MD' AND c.state_code  = 'DC' THEN 8  
									WHEN c.company_code_bk  = 'MD' AND c.state_code  = 'DE' THEN 9  
									WHEN c.company_code_bk  = 'MD' AND c.state_code  = 'FL' THEN 10  
									WHEN c.company_code_bk  = 'MD' AND c.state_code  = 'GA' THEN 11  
									WHEN c.company_code_bk  = 'MD' AND c.state_code  = 'HI' THEN 12  
									WHEN c.company_code_bk  = 'MD' AND c.state_code  = 'ID' THEN 13  
									WHEN c.company_code_bk  = 'MD' AND c.state_code  = 'IL' THEN 14  
									WHEN c.company_code_bk  = 'MD' AND c.state_code  = 'IN' THEN 15  
									WHEN c.company_code_bk  = 'MD' AND c.state_code  = 'IA' THEN 16  
									WHEN c.company_code_bk  = 'MD' AND c.state_code  = 'KS' THEN 17  
									WHEN c.company_code_bk  = 'MD' AND c.state_code  = 'KY' THEN 18  
									WHEN c.company_code_bk  = 'MD' AND c.state_code  = 'LA' THEN 19  
									WHEN c.company_code_bk  = 'MD' AND c.state_code  = 'ME' THEN 20  
									WHEN c.company_code_bk  = 'MD' AND c.state_code  = 'MD' THEN 21  
									WHEN c.company_code_bk  = 'MD' AND c.state_code  = 'MA' THEN 22  
									WHEN c.company_code_bk  = 'MD' AND c.state_code  = 'MI' THEN 23  
									WHEN c.company_code_bk  = 'MD' AND c.state_code  = 'MN' THEN 24  
									WHEN c.company_code_bk  = 'MD' AND c.state_code  = 'MS' THEN 25  
									WHEN c.company_code_bk  = 'MD' AND c.state_code  = 'MO' THEN 26  
									WHEN c.company_code_bk  = 'MD' AND c.state_code  = 'MT' THEN 27  
									WHEN c.company_code_bk  = 'MD' AND c.state_code  = 'NE' THEN 28  
									WHEN c.company_code_bk  = 'MD' AND c.state_code  = 'NV' THEN 29  
									WHEN c.company_code_bk  = 'MD' AND c.state_code  = 'NH' THEN 30  
									WHEN c.company_code_bk  = 'MD' AND c.state_code  = 'NJ' THEN 31  
									WHEN c.company_code_bk  = 'MD' AND c.state_code  = 'NM' THEN 32  
									WHEN c.company_code_bk  = 'MD' AND c.state_code  = 'NY' THEN 33  
									WHEN c.company_code_bk  = 'MD' AND c.state_code  = 'NC' THEN 34  
									WHEN c.company_code_bk  = 'MD' AND c.state_code  = 'ND' THEN 35  
									WHEN c.company_code_bk  = 'MD' AND c.state_code  = 'OH' THEN 36  
									WHEN c.company_code_bk  = 'MD' AND c.state_code  = 'OK' THEN 37  
									WHEN c.company_code_bk  = 'MD' AND c.state_code  = 'OR' THEN 38  
									WHEN c.company_code_bk  = 'MD' AND c.state_code  = 'PA' THEN 39  
									WHEN c.company_code_bk  = 'MD' AND c.state_code  = 'RI' THEN 40  
									WHEN c.company_code_bk  = 'MD' AND c.state_code  = 'SC' THEN 41  
									WHEN c.company_code_bk  = 'MD' AND c.state_code  = 'SD' THEN 42  
									WHEN c.company_code_bk  = 'MD' AND c.state_code  = 'TN' THEN 43  
									WHEN c.company_code_bk  = 'MD' AND c.state_code  = 'TX' THEN 44  
									WHEN c.company_code_bk  = 'MD' AND c.state_code  = 'UT' THEN 45  
									WHEN c.company_code_bk  = 'MD' AND c.state_code  = 'VT' THEN 46  
									WHEN c.company_code_bk  = 'MD' AND c.state_code  = 'VA' THEN 47  
									WHEN c.company_code_bk  = 'MD' AND c.state_code  = 'WA' THEN 48  
									WHEN c.company_code_bk  = 'MD' AND c.state_code  = 'WV' THEN 49  
									WHEN c.company_code_bk  = 'MD' AND c.state_code  = 'WI' THEN 50  
									WHEN c.company_code_bk  = 'MD' AND c.state_code  = 'WY' THEN 51  
									WHEN c.company_code_bk  = 'MD' AND (c.state_code NOT IN 
									('AL','AK','AZ','AR','CA','CO','CT','DC','DE','FL','GA','HI','ID','IL','IN','IA','KS','KY','LA','ME','MD','MA','MI','MN','MS','MO','MT','NE','NV','NH','NJ','NM','NY','NC','ND','OH','OK','OR','PA','RI','SC','SD','TN','TX','UT','VT','VA','WA','WV','WI','WY') 
									OR c.state_code IS NULL) THEN 52  
									WHEN c.company_code_bk  = 'FT' AND c.state_code  = 'AB' THEN 53  
									WHEN c.company_code_bk  = 'FT' AND c.state_code  = 'BC' THEN 54  
									WHEN c.company_code_bk  = 'FT' AND c.state_code  = 'MB' THEN 55  
									WHEN c.company_code_bk  = 'FT' AND c.state_code  = 'NB' THEN 56  
									WHEN c.company_code_bk  = 'FT' AND c.state_code  = 'NL' THEN 57  
									WHEN c.company_code_bk  = 'FT' AND c.state_code  = 'NT' THEN 58  
									WHEN c.company_code_bk  = 'FT' AND c.state_code  = 'NS' THEN 59  
									WHEN c.company_code_bk  = 'FT' AND c.state_code  = 'NU' THEN 60  
									WHEN c.company_code_bk  = 'FT' AND c.state_code  = 'ON' THEN 61  
									WHEN c.company_code_bk  = 'FT' AND c.state_code  = 'PE' THEN 62  
									WHEN c.company_code_bk  = 'FT' AND c.state_code  = 'QC' THEN 63  
									WHEN c.company_code_bk  = 'FT' AND c.state_code  = 'SK' THEN 64  
									WHEN c.company_code_bk  = 'FT' AND c.state_code  = 'YT' THEN 65  
									WHEN c.company_code_bk  = 'FT' AND (c.state_code NOT IN ('AB','BC','MB','NB','NL','NT','NS','NU','ON','PE','QC','SK','YT') OR c.state_code IS NULL) THEN 66  
									ELSE 67 END  AS STATE_ID,
									(select country_hierarchy_key from`imgcp-20220521020353.core_sap.country_hierarchy_dim` country_hierarchy_dim   INNER JOIN 
imgcp-20220521020353.core_impulse.impulse_customer impulse_customer on impulse_customer.company_code_bk = country_hierarchy_dim.company_code_bk 
and country_hierarchy_dim.source_system_code_bk = 'IMP') as company_key,
                              c.src_sys_crt_date,
                              c.src_sys_clos_date,
                              c.db_rec_del_flag
                       FROM   core_impulse.impulse_customer AS c
                              LEFT OUTER JOIN
                              locn                          AS l
                                 ON l.customer_key = c.customer_key
                       WHERE  c.db_rec_close_date = CAST('9999-12-31' AS DATE)
                         AND  (
                                   EXISTS
                                   (
                                    SELECT 1
                                    FROM   ck_locn          AS cl
                                    WHERE  cl.customer_key = c.customer_key
                                   )
                               OR  EXISTS
                                   (
                                    SELECT 1
                                    FROM   ck_cust          AS cc
                                    WHERE  cc.customer_key = c.customer_key
                                   )
                              )
                      ) AS c
                      LEFT OUTER JOIN
                      (
                       SELECT customer_key,
                              branch_customer_number_bk,
                              company_code_bk,
                              branch_number,
                              customer_number,
                              customer_name,
                              credit_limit_amt,
                              current_due,
                              terms_desc,
                              bump_rate,
                              customer_mtd_sales,
                              customer_mtd_adj_cost,
                              customer_ytd_sales,
                              customer_ytd_adj_cost,
                              customer_prev_year_sales,
                              customer_prev_year_adj_cost,
                              contact_name,
                              phone_number,
                              credit_review_date,
                              total_balance_amt,
                              currency_code,
                              country_code,
                              state_code,
                              county_code,
                              city_code,
                              customer_group_code,
                              end_user_flag,
                              master_customer_name,
                              CASE WHEN FORMAT("%t", cust_location) = '[]' THEN NULL ELSE cust_location END AS cust_location,
                              load_mod_code
                       FROM   gdl_layer.impulse_customer_dim
                      ) AS g
                         ON c.branch_customer_number_bk = g.branch_customer_number_bk
                        AND c.company_code_bk           = g.company_code_bk
                      CROSS JOIN
                      (
                       SELECT COALESCE(MAX(customer_key), 0) AS max_customer_key
                       FROM   gdl_layer.impulse_customer_dim
                      ) AS m
              )
       WHERE  NOT(gdl_hash = cor_hash AND ((cor_load_mod_code <> 'D' AND gdl_load_mod_code <> 'D') OR (cor_load_mod_code = 'D' AND gdl_load_mod_code = 'D')))
      )                               AS cor
         ON cor.branch_customer_number_bk = gdl.branch_customer_number_bk
        AND cor.company_code_bk           = gdl.company_code_bk
WHEN MATCHED THEN
UPDATE SET
       gdl.branch_number               = cor.branch_number,
       gdl.customer_number             = cor.customer_number,
       gdl.customer_name               = cor.customer_name,
       gdl.credit_limit_amt            = cor.credit_limit_amt,
       gdl.current_due                 = cor.current_due_amt,
       gdl.terms_desc                  = cor.terms_desc,
       gdl.bump_rate                   = cor.bump_rate,
       gdl.customer_mtd_sales          = cor.customer_mtd_sales,
       gdl.customer_mtd_adj_cost       = cor.customer_mtd_adj_cost,
       gdl.customer_ytd_sales          = cor.customer_ytd_sales,
       gdl.customer_ytd_adj_cost       = cor.customer_ytd_adj_cost,
       gdl.customer_prev_year_sales    = cor.customer_prev_year_sales,
       gdl.customer_prev_year_adj_cost = cor.customer_prev_year_adj_cost,
       gdl.contact_name                = cor.contact_name,
       gdl.phone_number                = cor.phone_number,
       gdl.credit_review_date          = cor.credit_review_date,
       gdl.total_balance_amt           = cor.total_balance_amt,
       gdl.currency_code               = cor.currency_code,
       gdl.country_code                = cor.country_code,
       gdl.state_code                  = cor.state_code,
       gdl.county_code                 = cor.county_code,
       gdl.city_code                   = cor.city_code,
       gdl.customer_group_code         = cor.customer_group_code,
       gdl.end_user_flag               = cor.end_user_flag,
       gdl.master_customer_name        = cor.master_customer_name,
       gdl.cust_location               = cor.cust_location,
       gdl.batch_number                = (SELECT batch_number FROM audit_database.batch_control),
       gdl.load_mod_code               = cor.cor_load_mod_code,
       gdl.src_sys_crt_date            = cor.src_sys_crt_date,
       gdl.src_sys_clos_date           = cor.src_sys_clos_date,
       gdl.db_rec_del_flag             = cor.db_rec_del_flag,
       gdl.db_prcsd_dttm               = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
INSERT
(
       customer_key,
       branch_customer_number_bk,
       company_code_bk,
       branch_number,
       customer_number,
       customer_name,
       credit_limit_amt,
       current_due,
       terms_desc,
       bump_rate,
       customer_mtd_sales,
       customer_mtd_adj_cost,
       customer_ytd_sales,
       customer_ytd_adj_cost,
       customer_prev_year_sales,
       customer_prev_year_adj_cost,
       contact_name,
       phone_number,
       credit_review_date,
       total_balance_amt,
       currency_code,
       country_code,
       state_code,
       county_code,
       city_code,
       customer_group_code,
       end_user_flag,
       master_customer_name,
       cust_location,
       batch_number,
       load_mod_code,
       src_sys_crt_date,
       src_sys_clos_date,
       db_rec_begin_date,
       db_rec_close_date,
       db_rec_del_flag,
       db_prcsd_dttm
)
VALUES
(
       cor.new_customer_key,
       cor.branch_customer_number_bk,
       cor.company_code_bk,
       cor.branch_number,
       cor.customer_number,
       cor.customer_name,
       cor.credit_limit_amt,
       cor.current_due_amt,
       cor.terms_desc,
       cor.bump_rate,
       cor.customer_mtd_sales,
       cor.customer_mtd_adj_cost,
       cor.customer_ytd_sales,
       cor.customer_ytd_adj_cost,
       cor.customer_prev_year_sales,
       cor.customer_prev_year_adj_cost,
       cor.contact_name,
       cor.phone_number,
       cor.credit_review_date,
       cor.total_balance_amt,
       cor.currency_code,
       cor.country_code,
       cor.state_code,
       cor.county_code,
       cor.city_code,
       cor.customer_group_code,
       cor.end_user_flag,
       cor.master_customer_name,
       cor.cust_location,
       (SELECT batch_number FROM audit_database.batch_control),
       cor.cor_load_mod_code,
       cor.src_sys_crt_date,
       cor.src_sys_clos_date,
       (SELECT batch_date FROM audit_database.batch_control),
       CAST('9999-12-31' AS DATE),
       cor.db_rec_del_flag,
       CURRENT_TIMESTAMP()
);

COMMIT TRANSACTION;