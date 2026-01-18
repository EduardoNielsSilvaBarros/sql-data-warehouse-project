/*******************************************************************************
Script de Definición de Estructuras (DDL) - Capa Bronze
*******************************************************************************
Propósito: 
    Preparar el esquema 'bronze' eliminando tablas existentes y creándolas 
    desde cero para asegurar una ingesta de datos limpia (Truncate & Load).

Origen de datos: 
    - CRM: Datos de clientes, productos y ventas.
    - ERP: Datos de localización, perfiles adicionales y categorías.

Esquema: bronze
*******************************************************************************/

-- =============================================================================
-- SECCIÓN 1: TABLAS DEL SISTEMA CRM
-- =============================================================================
USE DataWarehouse;

-- 1.1 Creación de la tabla de información de clientes (CRM)
IF OBJECT_ID('bronze.crm_cust_info', 'U') IS NOT NULL
    DROP TABLE bronze.crm_cust_info;
GO

CREATE TABLE bronze.crm_cust_info (
    cst_id              INT,           -- Identificador único del cliente
    cst_key             NVARCHAR(50),  -- Código de negocio del cliente
    cst_firstname       NVARCHAR(50),  -- Nombre
    cst_lastname        NVARCHAR(50),  -- Apellido
    cst_marital_status  NVARCHAR(50),  -- Estado civil
    cst_gndr            NVARCHAR(50),  -- Género
    cst_create_date     DATE           -- Fecha de registro en el CRM
);
GO

-- 1.2 Creación de la tabla de productos (CRM)
IF OBJECT_ID('bronze.crm_prd_info', 'U') IS NOT NULL
    DROP TABLE bronze.crm_prd_info;
GO

CREATE TABLE bronze.crm_prd_info (
    prd_id       INT,           -- Identificador único del producto
    prd_key      NVARCHAR(50),  -- Código de negocio (SKU)
    prd_nm       NVARCHAR(50),  -- Nombre del producto
    prd_cost     INT,           -- Costo unitario
    prd_line     NVARCHAR(50),  -- Línea de producto
    prd_start_dt DATETIME,      -- Fecha de inicio de comercialización
    prd_end_dt   DATETIME       -- Fecha de fin de comercialización
);
GO

-- 1.3 Creación de la tabla de detalles de ventas (CRM)
IF OBJECT_ID('bronze.crm_sales_details', 'U') IS NOT NULL
    DROP TABLE bronze.crm_sales_details;
GO

CREATE TABLE bronze.crm_sales_details (
    sls_ord_num  NVARCHAR(50),  -- Número de orden de venta
    sls_prd_key  NVARCHAR(50),  -- Referencia al producto
    sls_cust_id  INT,           -- Referencia al cliente
    sls_order_dt INT,           -- Fecha de orden (formato YYYYMMDD)
    sls_ship_dt  INT,           -- Fecha de envío (formato YYYYMMDD)
    sls_due_dt   INT,           -- Fecha de vencimiento (formato YYYYMMDD)
    sls_sales    INT,           -- Monto total de venta
    sls_quantity INT,           -- Cantidad de unidades
    sls_price    INT            -- Precio unitario aplicado
);
GO

-- =============================================================================
-- SECCIÓN 2: TABLAS DEL SISTEMA ERP
-- =============================================================================

-- 2.1 Información de localización (ERP)
IF OBJECT_ID('bronze.erp_loc_a101', 'U') IS NOT NULL
    DROP TABLE bronze.erp_loc_a101;
GO

CREATE TABLE bronze.erp_loc_a101 (
    cid    NVARCHAR(50), -- ID de cliente (mapeo con CRM)
    cntry  NVARCHAR(50)  -- País de residencia
);
GO

-- 2.2 Información complementaria de clientes (ERP)
IF OBJECT_ID('bronze.erp_cust_az12', 'U') IS NOT NULL
    DROP TABLE bronze.erp_cust_az12;
GO

CREATE TABLE bronze.erp_cust_az12 (
    cid    NVARCHAR(50), -- ID de cliente
    bdate  DATE,         -- Fecha de nacimiento
    gen    NVARCHAR(50)  -- Género (según registro ERP)
);
GO

-- 2.3 Categorización de productos (ERP)
IF OBJECT_ID('bronze.erp_px_cat_g1v2', 'U') IS NOT NULL
    DROP TABLE bronze.erp_px_cat_g1v2;
GO

CREATE TABLE bronze.erp_px_cat_g1v2 (
    id           NVARCHAR(50), -- ID de subcategoría
    cat          NVARCHAR(50), -- Categoría principal del producto
    subcat       NVARCHAR(50), -- Subcategoría
    maintenance  NVARCHAR(50)  -- Flag o info de mantenimiento
);
GO
