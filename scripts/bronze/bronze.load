create or alter procedure bronze.load_bronze as
begin
	declare @start_time datetime, @end_time datetime, @start_time1 datetime, @end_time1 datetime
begin try
set @start_time1= GETDATE()
		print '========================================'
		print 'Loading Bronze Layer'
		print '========================================'

		print '----------------------------------------'
		print 'Loading CRM table'
		print '----------------------------------------'
		--Loading crm_cust_info into table truncating it in advance
		set @start_time = GETDATE();
		Print '>>>Truncation table crm_cust_info'
		truncate table bronze.crm_cust_info;
		Print '>>>Bulk inserting to table crm_cust_info'
		bulk insert bronze.crm_cust_info
		from 'C:\Users\dilsh\OneDrive\Рабочий стол\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		with (
		  firstrow= 2,
		  fieldterminator= ',',
		  tablock
		);
		set @end_time = GETDATE();
		print '>>> Loading duration: '+ cast(datediff(second, @start_time, @end_time) as nvarchar)+ 'seconds'

		--Loading prd_info into table truncating it in advance

		set @start_time = GETDATE();
		Print '>>>Truncation table crm_prd_info'
		truncate table bronze.crm_prd_info;
		print '>>>Bulk inserting table crm_prd_info'
		bulk insert bronze.crm_prd_info
		from 'C:\Users\dilsh\OneDrive\Рабочий стол\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		with (
		  firstrow= 2,
		  fieldterminator= ',',
		  tablock
		);
		set @end_time = getdate()
		print '>>> Loading duration: '+ cast(datediff(second, @start_time, @end_time) as nvarchar)+ 'seconds'	

		--Loading sales_details into table truncating it in advance
		set @start_time = GETDATE();
		Print '>>>Truncation table crm_sales_details'
		truncate table bronze.crm_sales_details;
		print '>>>Bulk inserting table crm_prd_info'
		bulk insert bronze.crm_sales_details
		from 'C:\Users\dilsh\OneDrive\Рабочий стол\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		with (
		  firstrow= 2,
		  fieldterminator= ',',
		  tablock
		);
		set @end_time = getdate()
		print '>>> Loading duration: '+ cast(datediff(second, @start_time, @end_time) as nvarchar)+ 'seconds'

		print '----------------------------------------'
		print 'Loading ERP tables'
		print '----------------------------------------'

		--Loading CUST_AZ12 into table truncating it in advance

		truncate table bronze.erp_CUST_AZ12;

		bulk insert bronze.erp_CUST_AZ12
		from 'C:\Users\dilsh\OneDrive\Рабочий стол\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		with (
		  firstrow= 2,
		  fieldterminator= ',',
		  tablock
		);

	

		--Loading LOC_A101 into table truncating it in advance

		truncate table bronze.erp_LOC_A101;

		bulk insert bronze.erp_LOC_A101
		from 'C:\Users\dilsh\OneDrive\Рабочий стол\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		with (
		  firstrow= 2,
		  fieldterminator= ',',
		  tablock
		);

	


		--Loading PX_CAT_G1V2 into table truncating it in advance

		truncate table bronze.erp_PX_CAT_G1V2;

		bulk insert bronze.erp_PX_CAT_G1V2
		from 'C:\Users\dilsh\OneDrive\Рабочий стол\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		with (
		  firstrow= 2,
		  fieldterminator= ',',
		  tablock
		);
		set @end_time1=GETDATE()
		Print '>>>Whole batch loading time: ' + cast(datediff(second, @start_time1,@end_time1) as nvarchar) + ' seconds'
	end try
	begin catch 
		Print '==================================='
		Print 'Error occured during loading bronze layer'
		print 'Error message' + error_message();
		Print '==================================='
	end catch
	

