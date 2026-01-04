USE [master];
RESTORE DATABASE [AdventureWorks2019]
FROM DISK = N'C:\MSSQL\Backup\AdventureWorks2019.bak'
WITH FILE = 1
   , MOVE N'AdventureWorks2019'
     TO N'C:\MSSQL\Data\AdventureWorks2019.mdf'
   , MOVE N'AdventureWorks2019_log'
     TO N'C:\MSSQL\Log\AdventureWorks2019_log.ldf'
   , NOUNLOAD
   , REPLACE
   , STATS = 1;
GO

USE [master]
RESTORE DATABASE [AdventureWorksDW2019] 
FROM  DISK = N'C:\MSSQL\Backup\AdventureWorksDW2019.bak' 
WITH  FILE = 1
    ,  MOVE N'AdventureWorksDW2019' TO N'C:\MSSQL\Data\AdventureWorksDW2019.mdf'
    ,  MOVE N'AdventureWorksDW2019_log' TO N'C:\MSSQL\Log\AdventureWorksDW2019_log.ldf'
    ,  NOUNLOAD
    ,  REPLACE
    ,  STATS = 1
GO

USE [master]
RESTORE DATABASE [AdventureWorks2022] 
FROM  DISK = N'C:\MSSQL\Backup\AdventureWorks2022.bak' 
WITH  FILE = 1
    ,  MOVE N'AdventureWorks2022' TO N'C:\MSSQL\Data\AdventureWorks2022.mdf'
    ,  MOVE N'AdventureWorks2022_log' TO N'C:\MSSQL\Log\AdventureWorks2022_log.ldf'
    ,  NOUNLOAD
    ,  REPLACE
    ,  STATS = 1
GO

USE [master]
RESTORE DATABASE [AdventureWorksDW2022] 
FROM  DISK = N'C:\mssql\backup\AdventureWorksDW2022.bak' 
WITH  FILE = 1
    ,  MOVE N'AdventureWorksDW2022' TO N'C:\MSSQL\Data\AdventureWorksDW2022.mdf'
    ,  MOVE N'AdventureWorksDW2022_log' TO N'C:\MSSQL\Log\AdventureWorksDW2022_log.ldf'
    ,  NOUNLOAD
    ,  REPLACE
    ,  STATS = 1
GO



USE [master]
ALTER DATABASE [AdventureWorks2022_Clone] SET SINGLE_USER WITH ROLLBACK IMMEDIATE
--DROP DATABASE [AdventureWorks2022_Clone]
RESTORE DATABASE [AdventureWorks2022_Clone] 
FROM  DISK = N'C:\MSSQL\Backup\AdventureWorks2022_Clone.bak' WITH  FILE = 1
,  MOVE N'AdventureWorks2019' TO N'C:\MSSQL\Data\AdventureWorks2022_Clone.mdf'
,  MOVE N'AdventureWorks2019_log' TO N'C:\MSSQL\Log\AdventureWorks2022_log_Clone.ldf'
,  NOUNLOAD,  REPLACE, STATS = 1
ALTER DATABASE [AdventureWorks2022_Clone] SET MULTI_USER
GO

USE [master]
ALTER DATABASE [AdventureWorksDW2022_Clone] SET SINGLE_USER WITH ROLLBACK IMMEDIATE
--DROP DATABASE [AdventureWorksDW2022_Clone]
RESTORE DATABASE [AdventureWorksDW2022_Clone] 
FROM  DISK = N'C:\MSSQL\Backup\AdventureWorksDW2022_Clone.bak' WITH  FILE = 1
,  MOVE N'AdventureWorksDW2019' TO N'C:\MSSQL\Data\AdventureWorksDW2022_Clone.mdf'
,  MOVE N'AdventureWorksDW2019_log' TO N'C:\MSSQL\Log\AdventureWorksDW2022_log_Clone.ldf'
,  NOUNLOAD,  REPLACE, STATS = 1
ALTER DATABASE [AdventureWorksDW2022_Clone] SET MULTI_USER
GO

USE [master]
ALTER DATABASE [AdventureWorks2025_Clone] SET SINGLE_USER WITH ROLLBACK IMMEDIATE
RESTORE DATABASE [AdventureWorks2025_Clone] 
FROM  DISK = N'C:\mssql\backup\AdventureWorks2022_Clone.bak' 
WITH  FILE = 1,  
MOVE N'AdventureWorks2019' TO N'C:\MSSQL\Data\AdventureWorks2025.mdf',  
MOVE N'AdventureWorks2019_log' TO N'C:\MSSQL\Log\AdventureWorks2025.ldf',  
NOUNLOAD,  REPLACE,  STATS = 1
ALTER DATABASE [AdventureWorks2025_Clone] SET MULTI_USER
GO

USE [master];
GO

IF EXISTS (SELECT 1 FROM sys.databases WHERE [name] = 'AdventureWorks2019_Tgt')
BEGIN
    ALTER DATABASE [AdventureWorks2019_Tgt] SET  SINGLE_USER WITH ROLLBACK IMMEDIATE
    DROP DATABASE [AdventureWorks2019_Tgt]
END

IF EXISTS (SELECT 1 FROM sys.databases WHERE [name] = 'AdventureWorks2022_Tgt')
BEGIN
    ALTER DATABASE [AdventureWorks2022_Tgt] SET  SINGLE_USER WITH ROLLBACK IMMEDIATE
    DROP DATABASE [AdventureWorks2022_Tgt]
END

DBCC CLONEDATABASE ( [AdventureWorks2019], [AdventureWorks2019_Tgt] ) WITH NO_STATISTICS, NO_QUERYSTORE;
DBCC CLONEDATABASE ( [AdventureWorks2022], [AdventureWorks2022_Tgt] ) WITH NO_STATISTICS, NO_QUERYSTORE;
GO

USE [master];
GO

IF EXISTS (SELECT 1 FROM sys.databases WHERE [name] = 'ColumnStoreMigrationDemo_Tgt')
BEGIN
    ALTER DATABASE [ColumnStoreMigrationDemo_Tgt] SET  SINGLE_USER WITH ROLLBACK IMMEDIATE
    DROP DATABASE [ColumnStoreMigrationDemo_Tgt]
END

DBCC CLONEDATABASE ( [ColumnStoreMigrationDemo_Src], [ColumnStoreMigrationDemo_Tgt] ) WITH NO_STATISTICS, NO_QUERYSTORE;
ALTER DATABASE [ColumnStoreMigrationDemo_Tgt] SET SINGLE_USER WITH ROLLBACK IMMEDIATE
ALTER DATABASE [ColumnStoreMigrationDemo_Tgt] SET READ_WRITE
ALTER DATABASE [ColumnStoreMigrationDemo_Tgt] SET MULTI_USER

/*
USE [AdventureWorks2019_Clone]
GO

DROP INDEX [IX_Employee_OrganizationLevel_OrganizationNode] ON [HumanResources].[Employee]
GO

ALTER TABLE [HumanResources].[Employee] DROP COLUMN [OrganizationLevel]

DROP VIEW [Production].[vProductAndDescription];
DROP VIEW [Production].[vProductModelCatalogDescription]
DROP VIEW [Production].[vProductModelInstructions]
--DROP XML SCHEMA COLLECTION  [Production].[ManuInstructionsSchemaCollection]
--DROP XML SCHEMA COLLECTION  [Production].[ProductDescriptionSchemaCollection]

ALTER TABLE [Production].[Product] DROP CONSTRAINT [FK_Product_ProductModel_ProductModelID];
ALTER TABLE [Production].[ProductModelIllustration] DROP CONSTRAINT [FK_ProductModelIllustration_ProductModel_ProductModelID];
ALTER TABLE [Production].[ProductModelProductDescriptionCulture] DROP CONSTRAINT [FK_ProductModelProductDescriptionCulture_ProductModel_ProductModelID];

*/
