EXEC sp_configure 'Show Advanced Options', 1
RECONFIGURE


EXEC sp_configure 'Ole Automation Procedures', 1
RECONFIGURE


USE [master];
RESTORE DATABASE [AdventureWorks2019]
FROM DISK = N'C:\MSSQL\Backup\AdventureWorks2019.bak'
WITH FILE = 1
   , MOVE N'AdventureWorks2019'
   , MOVE N'AdventureWorks2019_log'
     TO N'C:\MSSQL\Log\AdventureWorks2019_log.ldf'
   , NOUNLOAD
   , REPLACE
   , STATS = 1;
GO

/* Example 1: 

    Export and import all data from:
        1. AdventureWorks2022 Schemas: 'HumanResources, Production, Purchasing, Sales'
        2. Matching Table Name patterns: 'Product*, *Address, *Tax*, Employee*, Work*'
    Except for:
        1. Any Table Name in any Schema that ends with 'History' or 'Model'
        2. Any Column Name 'LargePhoto'
        3. Any Column with data type xml
        4. Any Identity Columns
    Columns inside all output csv files will be delimited with: '^|^'
    Rows inside all output csv files will be delimited with: '~~~'
    All csv exports will land in D:\DOCKER_SHARE\Windows\BackupCommon\BCP\
    All PowerShell/XmlFormat files will be created by SQL Server in its own directory C:\MSSQL\Backup\BCP\
*/

USE [AdventureWorks2022]
GO

DECLARE

  @InstanceNameSrc                    NVARCHAR(128)     = N'Inst2.docker.internal,1433'
, @InstanceNameTgt                    NVARCHAR(128)     = N'Inst1.docker.internal,1433'
, @SqlAuthentication                  BIT               = 1
, @DbNameSrc                          SYSNAME           = N'AdventureWorks2022'
, @DbNameTgt                          SYSNAME           = N'AdventureWorks2022_Clone'
, @OutputDirectoryPsXml               NVARCHAR(MAX)     = N'C:\MSSQL\Backup\BCP\'
, @OutputDirectoryCsv                 NVARCHAR(MAX)     = N'D:\DOCKER_SHARE\Windows\BackupCommon\BCP\'                                                        
, @SchemaNames                        NVARCHAR(MAX)     = N'HumanResources, Production, Purchasing, Sales'
, @TableNames                         NVARCHAR(MAX)     = N'Product*, *Address, *Tax*, Employee*, Work*'
, @SchemaNamesExpt                    NVARCHAR(MAX)     = N'*'
, @TableNamesExpt                     NVARCHAR(MAX)     = N'*History, *Model'
, @ColumnNamesExpt                    NVARCHAR(MAX)     = N'LargePhoto'
, @DataTypesExpt                      NVARCHAR(MAX)     = N'xml'
, @DelimBcpOutputField                VARCHAR(3)        = '^|^'
, @DelimBcpOutputRow                  VARCHAR(16)       = '~~~'
, @ExportIdentityCols                 BIT               = 0
, @ExportColumnHeaders                BIT               = 1

EXEC [dbo].[sp_BcpRunner]
                                   @InstanceNameSrc         = @InstanceNameSrc     
                                 , @InstanceNameTgt         = @InstanceNameTgt     
                                 , @SqlAuthentication       = @SqlAuthentication    
                                 , @DbNameSrc               = @DbNameSrc           
                                 , @DbNameTgt               = @DbNameTgt           
                                 , @OutputDirectoryPsXml    = @OutputDirectoryPsXml
                                 , @OutputDirectoryCsv      = @OutputDirectoryCsv  
                                 , @SchemaNames             = @SchemaNames         
                                 , @TableNames              = @TableNames          
                                 , @SchemaNamesExpt         = @SchemaNamesExpt     
                                 , @TableNamesExpt          = @TableNamesExpt      
                                 , @ColumnNamesExpt         = @ColumnNamesExpt     
                                 , @DataTypesExpt           = @DataTypesExpt
                                 , @DelimBcpOutputField     = @DelimBcpOutputField
                                 , @DelimBcpOutputRow       = @DelimBcpOutputRow
                                 , @ExportIdentityCols      = @ExportIdentityCols
                                 , @ExportColumnHeaders     = @ExportColumnHeaders
GO

USE [AdventureWorksDW2022]
GO

DECLARE

  @InstanceNameSrc                    NVARCHAR(128)     = N'Inst2.docker.internal,1433'
, @InstanceNameTgt                    NVARCHAR(128)     = N'Inst1.docker.internal,1433'
, @SqlAuthentication                  BIT               = 1
, @DbNameSrc                          SYSNAME           = N'AdventureWorksDW2022'
, @DbNameTgt                          SYSNAME           = N'AdventureWorksDW2022_Clone'
, @OutputDirectoryPsXml               NVARCHAR(MAX)     = N'C:\MSSQL\Backup\BCP\'
, @OutputDirectoryCsv                 NVARCHAR(MAX)     = N'D:\DOCKER_SHARE\Windows\BackupCommon\BCP\'                                                        
, @SchemaNames                        NVARCHAR(MAX)     = N'dbo'
, @TableNames                         NVARCHAR(MAX)     = N'DimProduct'
, @DelimBcpOutputField                VARCHAR(3)        = '^|^'
, @DelimBcpOutputRow                  VARCHAR(16)       = '~~~'
, @ExportIdentityCols                 BIT               = 0
, @ExportColumnHeaders                BIT               = 0

EXEC [dbo].[sp_BcpRunner]
                                   @InstanceNameSrc         = @InstanceNameSrc     
                                 , @InstanceNameTgt         = @InstanceNameTgt     
                                 , @SqlAuthentication       = @SqlAuthentication    
                                 , @DbNameSrc               = @DbNameSrc           
                                 , @DbNameTgt               = @DbNameTgt           
                                 , @OutputDirectoryPsXml    = @OutputDirectoryPsXml
                                 , @OutputDirectoryCsv      = @OutputDirectoryCsv  
                                 , @SchemaNames             = @SchemaNames         
                                 , @TableNames              = @TableNames
                                 , @DelimBcpOutputField     = @DelimBcpOutputField
                                 , @DelimBcpOutputRow       = @DelimBcpOutputRow
                                 , @ExportIdentityCols      = @ExportIdentityCols
                                 , @ExportColumnHeaders     = @ExportColumnHeaders
GO


/* 


USE [AdventureWorks2022]
GO
*/

DECLARE

  @InstanceNameSrc                    NVARCHAR(128)     = N'Inst2.docker.internal,1433'
, @SqlAuthentication                  BIT               = 1
, @DbNameSrc                          SYSNAME           = N'AdventureWorks2022'
, @DbNameTgt                          SYSNAME           = N'AdventureWorks2022'
, @OutputDirectoryPsXml               NVARCHAR(MAX)     = N'C:\MSSQL\Backup\BCP\'
, @OutputDirectoryCsv                 NVARCHAR(MAX)     = N'D:\DOCKER_SHARE\Windows\BackupCommon\BCP\'                                                        
, @SchemaNames                        NVARCHAR(MAX)     = 'Sales'
, @TableNames                         NVARCHAR(MAX)     = 'SalesOrderHeader, SalesOrderDetail, SalesPerson'
, @DataTypesExpt                      NVARCHAR(MAX)     = N'uniqueidentifier'
, @DelimBcpOutputField                VARCHAR(3)        = '^|^'
, @DelimBcpOutputRow                  VARCHAR(16)       = '~~~'
, @ExportIdentityCols                 BIT               = 0
, @ExportColumnHeaders                BIT               = 1
, @AllowNotNullColumnsAsExceptions    BIT               = 1
, @ImportTarget                       VARCHAR(16)       = 'SNOWFLAKE'

EXEC [dbo].[sp_BcpRunner]
                                   @InstanceNameSrc                 = @InstanceNameSrc   
                                 , @SqlAuthentication               = @SqlAuthentication  
                                 , @DbNameSrc                       = @DbNameSrc           
                                 , @DbNameTgt                       = @DbNameTgt           
                                 , @OutputDirectoryPsXml            = @OutputDirectoryPsXml
                                 , @OutputDirectoryCsv              = @OutputDirectoryCsv  
                                 , @SchemaNames                     = @SchemaNames         
                                 , @TableNames                      = @TableNames            
                                 , @DataTypesExpt                   = @DataTypesExpt
                                 , @DelimBcpOutputField             = @DelimBcpOutputField
                                 , @DelimBcpOutputRow               = @DelimBcpOutputRow
                                 , @ExportIdentityCols              = @ExportIdentityCols
                                 , @ExportColumnHeaders             = @ExportColumnHeaders
                                 , @AllowNotNullColumnsAsExceptions = @AllowNotNullColumnsAsExceptions
                                 , @ImportTarget                    = @ImportTarget
GO

-- ############################################################################################################################
USE [AdventureWorks2019]
GO

DECLARE

  @InstanceNameSrc                    NVARCHAR(128)     = N'Inst1.docker.internal,1433'
, @InstanceNameTgt                    NVARCHAR(128)     = N'Inst2.docker.internal,1433'
, @SqlAuthentication                  BIT               = 1
, @DbNameSrc                          SYSNAME           = N'AdventureWorks2019'
, @DbNameTgt                          SYSNAME           = N'AdventureWorks2019_Clone'
, @OutputDirectoryPsXml               NVARCHAR(MAX)     = N'C:\MSSQL\Backup\BCP\'
, @OutputDirectoryCsv                 NVARCHAR(MAX)     = N'D:\DOCKER_SHARE\Windows\BackupCommon\BCP\'                                                        
, @SchemaNames                        NVARCHAR(MAX)     = N'*'
, @TableNames                         NVARCHAR(MAX)     = N'*'
, @DelimBcpOutputField                VARCHAR(3)        = '^|^'
, @DelimBcpOutputRow                  VARCHAR(16)       = '~~~'
, @ExportIdentityCols                 BIT               = 0
, @ExportColumnHeaders                BIT               = 1

EXEC [dbo].[sp_BcpRunner]
                                   @InstanceNameSrc         = @InstanceNameSrc     
                                 , @InstanceNameTgt         = @InstanceNameTgt     
                                 , @SqlAuthentication       = @SqlAuthentication    
                                 , @DbNameSrc               = @DbNameSrc           
                                 , @DbNameTgt               = @DbNameTgt           
                                 , @OutputDirectoryPsXml    = @OutputDirectoryPsXml
                                 , @OutputDirectoryCsv      = @OutputDirectoryCsv  
                                 , @SchemaNames             = @SchemaNames         
                                 , @TableNames              = @TableNames          
                                 , @DelimBcpOutputField     = @DelimBcpOutputField
                                 , @DelimBcpOutputRow       = @DelimBcpOutputRow
                                 , @ExportIdentityCols      = @ExportIdentityCols
                                 , @ExportColumnHeaders     = @ExportColumnHeaders
GO

USE [AdventureWorksDW2019]
GO

DECLARE

  @InstanceNameSrc                    NVARCHAR(128)     = N'Inst1.docker.internal,1433'
, @InstanceNameTgt                    NVARCHAR(128)     = N'Inst2.docker.internal,1433'
, @SqlAuthentication                  BIT               = 1
, @DbNameSrc                          SYSNAME           = N'AdventureWorksDW2019'
, @DbNameTgt                          SYSNAME           = N'AdventureWorksDW2019_Clone'
, @OutputDirectoryPsXml               NVARCHAR(MAX)     = N'C:\MSSQL\Backup\BCP\'
, @OutputDirectoryCsv                 NVARCHAR(MAX)     = N'D:\DOCKER_SHARE\Windows\BackupCommon\BCP\'                                                        
, @SchemaNames                        NVARCHAR(MAX)     = N'*'
, @TableNames                         NVARCHAR(MAX)     = N'*'
, @ColumnNamesExpt                    NVARCHAR(MAX)     = NULL --N'LargePhoto, Arabic*, Hebrew*, Thai*, Japanese*'
, @DelimBcpOutputField                VARCHAR(3)        = '^|^'
, @DelimBcpOutputRow                  VARCHAR(16)       = '~~~'
, @ExportIdentityCols                 BIT               = 1
, @ExportColumnHeaders                BIT               = 1

EXEC [dbo].[sp_BcpRunner]
                                   @InstanceNameSrc         = @InstanceNameSrc     
                                 , @InstanceNameTgt         = @InstanceNameTgt     
                                 , @SqlAuthentication       = @SqlAuthentication    
                                 , @DbNameSrc               = @DbNameSrc           
                                 , @DbNameTgt               = @DbNameTgt           
                                 , @OutputDirectoryPsXml    = @OutputDirectoryPsXml
                                 , @OutputDirectoryCsv      = @OutputDirectoryCsv  
                                 , @SchemaNames             = @SchemaNames         
                                 , @TableNames              = @TableNames
                                 , @ColumnNamesExpt         = @ColumnNamesExpt 
                                 , @DelimBcpOutputField     = @DelimBcpOutputField
                                 , @DelimBcpOutputRow       = @DelimBcpOutputRow
                                 , @ExportIdentityCols      = @ExportIdentityCols
                                 , @ExportColumnHeaders     = @ExportColumnHeaders
GO


USE [AdventureWorks2022]
GO

DECLARE

  @InstanceNameSrc                    NVARCHAR(128)     = N'Inst2.docker.internal,1433'
, @InstanceNameTgt                    NVARCHAR(128)     = N'Inst1.docker.internal,1433'
, @SqlAuthentication                  BIT               = 1
, @DbNameSrc                          SYSNAME           = N'AdventureWorks2022'
, @DbNameTgt                          SYSNAME           = N'AdventureWorks2022_Clone'
, @OutputDirectoryPsXml               NVARCHAR(MAX)     = N'C:\MSSQL\Backup\BCP\'
, @OutputDirectoryCsv                 NVARCHAR(MAX)     = N'D:\DOCKER_SHARE\Windows\BackupCommon\BCP\'                                                        
, @SchemaNames                        NVARCHAR(MAX)     = N'*'
, @TableNames                         NVARCHAR(MAX)     = N'*'
, @DelimBcpOutputField                VARCHAR(3)        = '^|^'
, @DelimBcpOutputRow                  VARCHAR(16)       = '~~~'
, @ExportIdentityCols                 BIT               = 0
, @ExportColumnHeaders                BIT               = 1

EXEC [dbo].[sp_BcpRunner]
                                   @InstanceNameSrc         = @InstanceNameSrc     
                                 , @InstanceNameTgt         = @InstanceNameTgt     
                                 , @SqlAuthentication       = @SqlAuthentication    
                                 , @DbNameSrc               = @DbNameSrc           
                                 , @DbNameTgt               = @DbNameTgt           
                                 , @OutputDirectoryPsXml    = @OutputDirectoryPsXml
                                 , @OutputDirectoryCsv      = @OutputDirectoryCsv  
                                 , @SchemaNames             = @SchemaNames         
                                 , @TableNames              = @TableNames          
                                 , @DelimBcpOutputField     = @DelimBcpOutputField
                                 , @DelimBcpOutputRow       = @DelimBcpOutputRow
                                 , @ExportIdentityCols      = @ExportIdentityCols
                                 , @ExportColumnHeaders     = @ExportColumnHeaders
GO

USE [AdventureWorksDW2022]
GO

DECLARE

  @InstanceNameSrc                    NVARCHAR(128)     = N'Inst2,1433'
, @InstanceNameTgt                    NVARCHAR(128)     = N'Inst1,1433'
, @SqlAuthentication                  BIT               = 0
, @DbNameSrc                          SYSNAME           = N'AdventureWorksDW2022'
, @DbNameTgt                          SYSNAME           = N'AdventureWorksDW2022_Clone'
, @OutputDirectoryPsXml               NVARCHAR(MAX)     = N'C:\MSSQL\Backup\BCP\'
, @OutputDirectoryCsv                 NVARCHAR(MAX)     = N'C:\MSSQL\Backup\BCP\'
, @SchemaNames                        NVARCHAR(MAX)     = N'dbo'
, @TableNames                         NVARCHAR(MAX)     = N'*'
, @SchemaNamesExpt                    NVARCHAR(MAX)     = N'dbo'  
, @TableNamesExpt                     NVARCHAR(MAX)     = N'Fact*'
, @ColumnNamesExpt                    NVARCHAR(MAX)     = N'Arabic*'
, @DelimBcpOutputField                VARCHAR(3)        = '^_^'
, @DelimBcpOutputRow                  VARCHAR(16)       = '~~~'
, @ExportIdentityCols                 BIT               = 1
, @ExportColumnHeaders                BIT               = 1

EXEC [dbo].[sp_BcpRunner]
                                   @InstanceNameSrc         = @InstanceNameSrc     
                                 , @InstanceNameTgt         = @InstanceNameTgt     
                                 , @SqlAuthentication       = @SqlAuthentication    
                                 , @DbNameSrc               = @DbNameSrc           
                                 , @DbNameTgt               = @DbNameTgt           
                                 , @OutputDirectoryPsXml    = @OutputDirectoryPsXml
                                 , @OutputDirectoryCsv      = @OutputDirectoryCsv  
                                 , @SchemaNames             = @SchemaNames         
                                 , @TableNames              = @TableNames
                                 , @SchemaNamesExpt         = @SchemaNamesExpt
                                 , @TableNamesExpt          = @TableNamesExpt 
                                 , @ColumnNamesExpt         = @ColumnNamesExpt 
                                 , @DelimBcpOutputField     = @DelimBcpOutputField
                                 , @DelimBcpOutputRow       = @DelimBcpOutputRow
                                 , @ExportIdentityCols      = @ExportIdentityCols
                                 , @ExportColumnHeaders     = @ExportColumnHeaders
GO

USE [ColumnStoreMigrationDemo_Src]
GO

DECLARE

  @InstanceNameSrc                    NVARCHAR(128)     = N'Inst2.docker.internal,1433'
, @InstanceNameTgt                    NVARCHAR(128)     = N'Inst1.docker.internal,1433'
, @SqlAuthentication                  BIT               = 1
, @DbNameSrc                          SYSNAME           = N'ColumnStoreMigrationDemo_Src'
, @DbNameTgt                          SYSNAME           = N'ColumnStoreMigrationDemo_Src_Clone'
, @OutputDirectoryPsXml               NVARCHAR(MAX)     = N'C:\MSSQL\Backup\BCP\'
, @OutputDirectoryCsv                 NVARCHAR(MAX)     = N'D:\DOCKER_SHARE\Windows\BackupCommon\BCP\'                                                        
, @SchemaNames                        NVARCHAR(MAX)     = N'dbo'
, @TableNames                         NVARCHAR(MAX)     = N'FactResellerSalesXL'
, @ColumnNamesExpt                    NVARCHAR(MAX)     = NULL --N'LargePhoto, Arabic*, Hebrew*, Thai*, Japanese*'
, @DelimBcpOutputField                VARCHAR(3)        = '^|^'
, @DelimBcpOutputRow                  VARCHAR(16)       = '~~~'
, @ExportIdentityCols                 BIT               = 1
, @ExportColumnHeaders                BIT               = 1

EXEC [dbo].[sp_BcpRunner]
                                   @InstanceNameSrc         = @InstanceNameSrc     
                                 , @InstanceNameTgt         = @InstanceNameTgt     
                                 , @SqlAuthentication       = @SqlAuthentication    
                                 , @DbNameSrc               = @DbNameSrc           
                                 , @DbNameTgt               = @DbNameTgt           
                                 , @OutputDirectoryPsXml    = @OutputDirectoryPsXml
                                 , @OutputDirectoryCsv      = @OutputDirectoryCsv  
                                 , @SchemaNames             = @SchemaNames         
                                 , @TableNames              = @TableNames
                                 , @ColumnNamesExpt         = @ColumnNamesExpt 
                                 , @DelimBcpOutputField     = @DelimBcpOutputField
                                 , @DelimBcpOutputRow       = @DelimBcpOutputRow
                                 , @ExportIdentityCols      = @ExportIdentityCols
                                 , @ExportColumnHeaders     = @ExportColumnHeaders
GO