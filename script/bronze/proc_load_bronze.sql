/*
===============================================================================
Stored Procedure: bronze.load_bronze
===============================================================================
Descripción:
    Este procedimiento orquesta la carga completa (Full Load) de la capa "Bronze".
    Se encarga de limpiar las tablas y cargar datos crudos desde archivos CSV.

Funcionalidad Principal:
    1. Reinicia (TRUNCATE) las tablas de la capa Bronze para evitar duplicados.
    2. Carga masiva (BULK INSERT) de datos desde archivos CSV locales.
    3. Mide y registra el tiempo de ejecución por tabla y el tiempo total del lote.

Manejo de Errores:
    - Utiliza un bloque TRY...CATCH. Si ocurre un error en cualquier punto,
      el proceso se detiene y muestra el mensaje de error original sin caerse.

Tablas Afectadas:
    - CRM: crm_cust_info, crm_prd_info, crm_sales_details
    - ERP: erp_cust_az12, erp_loc_a101, erp_px_cat_g1v2
===============================================================================
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN

	DECLARE @start_time DATETIME,
			@end_time DATETIME,
			@start_batch_time DATETIME,
			@end_batch_time DATETIME;

	BEGIN TRY
		-- Punto A: Inicio del cronómetro general
		SET @start_batch_time = GETDATE();

		PRINT '===================================================================';
		PRINT 'Cargando Bronze Layer';
		PRINT '===================================================================';
	
		PRINT '-------------------------------------------------------------------';
		PRINT 'Cargando CRM Tables';
		PRINT '-------------------------------------------------------------------';

		-- Tabla 1: bronze.crm_cust_info
		SET @start_time = GETDATE();
		PRINT '>> Truncating << Tabla: [bronze].[crm_cust_info]';
		TRUNCATE TABLE [bronze].[crm_cust_info];

		PRINT '>> Bulk Insert << Tabla: [bronze].[crm_cust_info]';
		BULK INSERT [bronze].[crm_cust_info]
		FROM 'C:\SQL LAYERS COURSE\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT 'El tiempo de carga fue: ' + cast(datediff(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '-------------------------------------------------------------------';

	
		-- Tabla 2: bronze.crm_prd_info
		SET @start_time = GETDATE();

		PRINT '>> Truncating << Tabla: [bronze].[crm_prd_info]';
		TRUNCATE TABLE [bronze].[crm_prd_info];

		PRINT '>> Bulk Insert << Tabla: [bronze].[crm_prd_info]';
		BULK INSERT [bronze].[crm_prd_info]
		FROM 'C:\SQL LAYERS COURSE\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		SET @end_time = GETDATE();
		PRINT 'El tiempo de carga fue: ' + cast(datediff(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '-------------------------------------------------------------------';


		-- Tabla 3: bronze.crm_sales_details
		SET @start_time = GETDATE();

		PRINT '>> Truncating << Tabla: [bronze].[crm_sales_details]';
		TRUNCATE TABLE [bronze].[crm_sales_details];

		PRINT '>> Bulk Insert << Tabla: [bronze].[crm_sales_details]';
		BULK INSERT [bronze].[crm_sales_details]
		FROM 'C:\SQL LAYERS COURSE\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		SET @end_time = GETDATE();
		PRINT 'El tiempo de carga fue: ' + cast(datediff(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '-------------------------------------------------------------------';

		-- Tabla 4: bronze.erp_cust_az12
		SET @start_time = GETDATE();

		PRINT '>> Truncating << Tabla: [bronze].[erp_cust_az12]';
		TRUNCATE TABLE [bronze].[erp_cust_az12];

		PRINT '>> Bulk Insert << Tabla: [bronze].[erp_cust_az12]';
		BULK INSERT [bronze].[erp_cust_az12]
		FROM 'C:\SQL LAYERS COURSE\datasets\source_erp\CUST_AZ12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		SET @end_time = GETDATE();
		PRINT 'El tiempo de carga fue: ' + cast(datediff(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '-------------------------------------------------------------------';


		-- Tabla 5: bronze.erp_loc_a101
		SET @start_time = GETDATE();

		PRINT '>> Truncating << Tabla: [bronze].[erp_loc_a101]';
		TRUNCATE TABLE [bronze].[erp_loc_a101];

		PRINT '>> Bulk Insert << Tabla: [bronze].[erp_loc_a101]';
		BULK INSERT [bronze].[erp_loc_a101]
		FROM 'C:\SQL LAYERS COURSE\datasets\source_erp\LOC_A101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		SET @end_time = GETDATE();
		PRINT 'El tiempo de carga fue: ' + cast(datediff(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '-------------------------------------------------------------------';

		-- Tabla 6: bronze.erp_px_cat_g1v2
		SET @start_time = GETDATE();

		PRINT '>> Truncating << Tabla: [bronze].[erp_px_cat_g1v2]';
		TRUNCATE TABLE [bronze].[erp_px_cat_g1v2];

		PRINT '>> Bulk Insert << Tabla: [bronze].[erp_px_cat_g1v2]';
		BULK INSERT [bronze].[erp_px_cat_g1v2]
		FROM 'C:\SQL LAYERS COURSE\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		SET @end_time = GETDATE();
		PRINT 'El tiempo de carga fue: ' + cast(datediff(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '-------------------------------------------------------------------';

		-- Punto B: Final del cronómetro general
		SET @end_batch_time = GETDATE();

		PRINT '-------------------------------------------------------------------';
		PRINT 'El proceso de carga bronce fue completado'
		PRINT 'El tiempo de todo el proceso fue: ' + cast(datediff(second, @start_batch_time, @end_batch_time) AS NVARCHAR) + ' seconds';
		PRINT '-------------------------------------------------------------------';
	
	END TRY
	BEGIN CATCH
		PRINT '=================================================================='
		PRINT 'Ocurrió un error inesperado durante la carga de Bronze Layer'
		PRINT 'Error Message: ' + ERROR_MESSAGE();
		PRINT 'Error Number: ' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error State: ' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '=================================================================='
	END CATCH
END
