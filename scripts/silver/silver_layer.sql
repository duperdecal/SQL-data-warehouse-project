go
create or alter procedure silver.load_silver as --создание сторд просиджр для полу-автоматизации
begin
declare @start_time as datetime declare @end_time as datetime declare @start_time1 as datetime declare @end_time1 as datetime
set @start_time1 = getdate()
set @start_time = getdate()
begin try --первая попытка кода если выйдет ошибка то произведется другой кусок кода
print '---------------------------------------------------------------'
Print 'Starting updating Silver Layer of Database!'
print '---------------------------------------------------------------'
	--Это запрос чтобы создать silver layer таблицы из сырых данных с bronze layer 
	print '---------------------------------------------------------------'
	print '>>>> Dropping table silver.crm_cust_info  <<<<'
	drop table if exists silver.crm_cust_info;
	--удаляем таблицу чтобы ее обновить
	print '>>>> Cleaning table silver.crm_cust_info  <<<<'
	print '---------------------------------------------------------------'
	select 
		cast(cst_id as int) as cst_id,
		cast(trim(cst_key) as nvarchar(50)) as cst_key, --кастом изменяем типы данных 
		trim(cst_firstname) cst_firstname, 
		trim(cst_lastname) cst_lastname, --убираем лидинг и трейлинг пробелы
		case 
			when upper(cst_marital_status) = 'M' then 'Married' 
			when upper(cst_marital_status) = 'S' then 'Single'
			else 'Undefined'
		end as cst_marital_status,
		case 
			when upper(cst_gndr) = 'M' then 'Male'
			when upper(cst_gndr) = 'F' then 'Female'
			else 'Undefined' --Добавляем целостную категоризацию
		end as cst_gndr,
		cast(cst_create_date as date) as cst_create_date,
		getdate() as date_added --Добавляем дефолт столбец чтобы знать последнее время изменения
	into silver.crm_cust_info --в SQL Server так работает CTAS
	from
	(select 
		*,
		row_number() over (partition by cst_id order by cast(cst_create_date as date) desc) as rn
	from bronze.crm_cust_info)t
	where rn = 1 and cst_id is not null;
set @end_time = GETDATE()
print 'Loading complete it took '+cast(datediff( second, @start_time, @end_time) as nvarchar)+' seconds'
end try 
begin catch 
Print 'There is error while loading silver.crm_cust_info'
end catch
	--та же самая обработка с crm_prd_info
set @start_time = GETDATE()
print '---------------------------------------------------------------'
	print '>>>>  Dropping table silver.crm_prd_info  <<<<'
	drop table if exists silver.crm_prd_info
	print '>>>>  Cleaning table silver.crm_prd_info  <<<<'
	print '---------------------------------------------------------------'
	select 
		cast(prd_id as int) as prd_id,
		replace(substring(prd_key,1, 5), '-', '_') as prd_cat, --разделение категории и кода продукта для JOIN
		substring(prd_key,7, len(prd_key)) as prd_key,
		trim(prd_nm) as prd_nm,
		cast(prd_cost as int) as prd_cost,
		case 
			when upper(prd_line) = 'R' then 'Road'
			when upper(prd_line) = 'S' then 'Other Sales'
			when upper(prd_line) = 'M' then 'Mountain'
			when upper(prd_line) = 'T' then 'Touring'
			else  'Undefined'
		end as prd_line,
		cast(prd_start_dt as date) as prd_start_dt, --ниже использована функция lead() чтобы починить дату конца продукта
		cast(lead(prd_start_dt) over (partition by prd_key order by prd_start_dt) as date) as prd_end_date,
		getdate() as date_added
	into silver.crm_prd_info
	from bronze.crm_prd_info
set @end_time = getdate() 
print 'Loading complete it took '+cast(datediff( second, @start_time, @end_time) as nvarchar)+' seconds'

set @start_time = getdate()
print '---------------------------------------------------------------'
	print '>>>>  Dropping table silver.crm_sales_details  <<<<'
	drop table if exists silver.crm_sales_details;
	print '>>>>  Cleaning table silver.crm_sales_details  <<<<'
	print '---------------------------------------------------------------'
	select 
		trim(sls_ord_num) as sls_ord_num,
		trim(sls_prd_key) as sls_prd_key,
		sls_cust_id,
		cast(cast(sls_order_dt as nvarchar(50)) as date) as sls_order_dt,
		cast(cast(sls_ship_dt as nvarchar(50)) as date) as sls_ship_dt, --конвертация требует чтобы тип был варчар
		cast(cast(sls_due_dt as nvarchar(50)) as date) as sls_due_dt,
		coalesce(sls_price,sls_sales/sls_quantity, 'Undefined') as sls_price,
		coalesce(sls_sales,sls_quantity*sls_price, 'Undefined') as sls_sales,
		coalesce(sls_quantity, sls_sales/sls_price, 'Undefined') as sls_quantity, --починка цены, колво и суммы продаж
		getdate() as date_added
	into silver.crm_sales_details
	from bronze.crm_sales_details
	where len(sls_order_dt) = 8 and len(sls_ship_dt) = 8 and len(sls_due_dt) = 8;
set @end_time = getdate()
print 'Loading complete it took '+cast(datediff( second, @start_time, @end_time) as nvarchar)+' seconds'
set @start_time = getdate()
print '---------------------------------------------------------------'
	print '>>>>  Dropping table silver.erp_cust_az12  <<<<'
	drop table if exists silver.erp_cust_az12;
	print '>>>>  Cleaning table silver.erp_cust_az12  <<<<'
	print '---------------------------------------------------------------'
	select 
		SUBSTRING(cid, 4, len(cid)) as cid,
		cast(case 
			when bdate > getdate() then null
			else cast(bdate as nvarchar)
		end as date) as bdate,
		case 
			when lower(gen) = 'f' then 'Female'
			when lower(gen) = 'm' then 'Male'
			when lower(gen) = 'female' then 'Female'
			when lower(gen) = 'male' then 'Male'
			else 'Undefined'
		end as gen
	into silver.erp_cust_az12
	from bronze.erp_cust_az12
set @end_time = GETDATE()
print 'Loading complete it took '+cast(datediff( second, @start_time, @end_time) as nvarchar)+' seconds'
set @start_time = getdate()
print '---------------------------------------------------------------'
	print '>>>>  Dropping table silver.erp_loc_a101  <<<<'
	drop table if exists silver.erp_loc_a101
	print '>>>>  Cleaning table silver.erp_loc_a101  <<<<'
	print '---------------------------------------------------------------'
	select 
		replace(cid,'-','') as cid,
		case 
			when lower(cntry) = 'de' then 'Germany'
			when lower(cntry) = 'usa' then 'United States of America'
			when lower(cntry) = 'united states' then 'United States of America'
			when lower(cntry) = 'us' then 'United States of America'
			when replace(cntry, ' ','') = '' then 'Undefined'
			when cntry is null then 'Undefined'
			else cntry
		end as cntry
	into silver.erp_loc_a101
	from bronze.erp_loc_a101
set @end_time = GETDATE()
print 'Loading complete it took '+cast(datediff( second, @start_time, @end_time) as nvarchar)+' seconds'

set @start_time = getdate()
print '---------------------------------------------------------------'
	print '>>>>  Dropping table silver.erp_px_cat_g1v2  <<<<'
	drop table if exists silver.erp_px_cat_g1v2
	print '>>>>  Cleaning table silver.erp_px_cat_g1v2  <<<<'
print '---------------------------------------------------------------'
	select *
	into silver.erp_px_cat_g1v2
	from bronze.erp_px_cat_g1v2
set @end_time = getdate()
print 'Loading complete it took '+cast(datediff( second, @start_time, @end_time) as nvarchar)+' seconds'
set @end_time1 = getdate()
print '#######################################################################################################'
print 'Whole loading complete! Silver layer has taken '+cast(datediff(second,@start_time1,@end_time1) as nvarchar)+' seconds to complete'
print '#######################################################################################################'
end
