--это тесты сырых данных которые проверяют сырые данные на целостность
select 
  *
from 
(select 
  *,
  ROW_NUMBER() over (partition by cst_id order by cst_id) as rn
from bronze.crm_cust_info)t
where rn > 1 or rn is null;
--проверка столбца на наличие дубликатов и нулов(в основном применяется к PK)

select 
  *
from bronze.crm_cust_info
where cst_lastname != trim(cst_lastname);
--проверяет колонку на наличие leading и trailing space

select distinct 
  cst_gndr
from bronze.crm_cust_info
--проверка целостности категоризации определенных столбцов





