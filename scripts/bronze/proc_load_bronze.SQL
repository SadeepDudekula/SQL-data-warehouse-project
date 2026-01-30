/*

==========================================================================================

Stored procedure : Load bronze layer (source -> bronze)

==========================================================================================

Script purpose :
	This stored procedure loads data into the 'bronze' schema from external CSV files.
	It performs the following actions:
	- Truncates the bronze tables loading data.
	-Uses the 'BULK INSERT' command to load the data from csv files to bronze tables

Parameters:
	None.
      this stored procedure does not accept any parameters or return any values.

Usage examples:
	EXEC bronze.load_bronze; 
========================================================================================

*/

create or alter procedure bronze.load_bronze as
begin
	DECLARE @start_time datetime , @end_time datetime,@batch_start_time datetime,@batch_end_time datetime;
	begin try
	set @batch_start_time = getdate();
		print'=====================================';
		print'loading bronze layer';
		print'=====================================';
		print'-------------------------------------';
		print'loading CRM layer';
		print'-------------------------------------';

		set @start_time = getdate();
		PRINT'>> Truncating table : bronze.crm_cust_info';
		truncate table bronze.crm_cust_info ;
		print'>> Inserting table : bronze.crm_cust_info';
		bulk insert bronze.crm_cust_info 
		from 'C:\New folder 99\sql\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		with(
			firstrow = 2,
			fieldterminator =',',
			tablock
		);
		set @end_time = getdate();
		print'>> load duration ' + cast(datediff(second,@start_time,@end_time) as nvarchar) + 'seconds';
		PRINT'-------------------------------------';


		set @start_time = getdate();
		PRINT'>> Truncating table : bronze.crm_prd_info';
		truncate table bronze.crm_prd_info ;
		print'>> Inserting table :bronze.crm_prd_info';
		bulk insert bronze.crm_prd_info 
		from 'C:\New folder 99\sql\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		with(
			firstrow = 2,
			fieldterminator =',',
			tablock
		);
		set @end_time = getdate();
		print'>> load duration ' + cast(datediff(second,@start_time,@end_time) as nvarchar) + 'seconds';
		PRINT'-------------------------------------';

		set @start_time = getdate();
		PRINT'>> Truncating table :bronze.crm_sales_details';
		truncate table bronze.crm_sales_details;
		print'>> Inserting table :bronze.crm_sales_details';
		bulk insert bronze.crm_sales_details
		from 'C:\New folder 99\sql\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		with(
			firstrow = 2,
			fieldterminator =',',
			tablock
		);
		set @end_time = getdate();
		print'>> load duration ' + cast(datediff(second,@start_time,@end_time) as nvarchar) + 'seconds';
		PRINT'-------------------------------------';

		print'-------------------------------------';
		print'loading ERP layer';
		print'-------------------------------------';


		set @start_time = getdate();
		PRINT'>> Truncating table :erp_loc_a101';
		truncate table bronze.erp_loc_a101;
		print'>> Inserting table :bronze.erp_loc_a101';
		bulk insert bronze.erp_loc_a101
		from 'C:\New folder 99\sql\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
		with(
			firstrow = 2,
			fieldterminator =',',
			tablock
		);
		set @end_time = getdate();
		print'>> load duration ' + cast(datediff(second,@start_time,@end_time) as nvarchar) + 'seconds';
		PRINT'-------------------------------------';

		set @start_time = getdate();
		PRINT'>> Truncating table :bronze.erp_cust_az12';
		truncate table bronze.erp_cust_az12;
		print'>> Inserting table :bronze.erp_cust_az12';
		bulk insert bronze.erp_cust_az12
		from 'C:\New folder 99\sql\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
		with(
			firstrow = 2,
			fieldterminator =',',
			tablock
		);
		set @end_time = getdate();
		print'>> load duration ' + cast(datediff(second,@start_time,@end_time) as nvarchar) + 'seconds';
		PRINT'-------------------------------------';

		set @start_time = getdate();
		PRINT'>> Truncating table :bronze.erp_px_cat_g1v2';
		truncate table bronze.erp_px_cat_g1v2;
		print'>> Inserting table :bronze.erp_px_cat_g1v2';
		bulk insert bronze.erp_px_cat_g1v2
		from 'C:\New folder 99\sql\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
		with(
			firstrow = 2,
			fieldterminator =',',
			tablock
		);
		set @end_time = getdate();
		print'>> load duration ' + cast(datediff(second,@start_time,@end_time) as nvarchar) + 'seconds';
		PRINT'-------------------------------------';
		set @batch_end_time = getdate();
		print'====================================='
		print'loading bronze layer is completed';
		print' -- total load duration '+ cast(datediff(second,@batch_start_time,@batch_end_time) as nvarchar) + ' seconds';
		print'====================================='
	end try
	begin catch
		print'==========================================='
		print'ERROR OCCURED DURING LOADING BRONZE LAYER';
		print'ERROR MESSAGE' + ERROR_MESSAGE();
		print'ERROR MESSAGE' + CAST(ERROR_NUMBER() AS NVARCHAR);
		print'ERROR MESSAGE' + CAST(ERROR_STATE() AS NVARCHAR);
		print'===========================================';
	end catch
end
