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


USE [master];
GO

IF EXISTS (SELECT 1 FROM sys.databases WHERE [name] = 'AdventureWorks2019_Target')
BEGIN
    ALTER DATABASE [AdventureWorks2019_Target] SET  SINGLE_USER WITH ROLLBACK IMMEDIATE
    DROP DATABASE [AdventureWorks2019_Target]
END

DBCC CLONEDATABASE ( [AdventureWorks2019], [AdventureWorks2019_Target] ) WITH NO_STATISTICS, NO_QUERYSTORE;


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
