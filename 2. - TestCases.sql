/* 
    Export and import all tables:
    1.  From Schemas: 'HumanResources, Production, Purchasing, Sales'
    2. Matching Table Name patterns: 'Product*, *Address, *Tax*, Employee*, Work*'
    Except for:
    1. Any table name that ends with 'History' or 'Model'
    2. Any Column Name 'LargePhoto'
    3. Any Column with data type xml
    4. Any Identity Columns
    Columns inside all output csv files will be delimited with: '|||'
*/
USE [AdventureWorks2019]
GO

DECLARE 

  @InstanceNameSrc                    NVARCHAR(128)     = N'Inst1.docker.internal'
, @InstanceNameTgt                    NVARCHAR(128)     = N'Inst2.docker.internal'
, @SqlAuthentication                  BIT               = 1       
, @SqlUserNameSrc                     SYSNAME           = 'sa'
, @SqlPasswordSrc                     NVARCHAR(128)     = N'Password1234$'
, @SqlUserNameTgt                     SYSNAME           = 'sa'
, @SqlPasswordTgt                     NVARCHAR(128)     = N'Password1234$'
, @DbNameSrc                          SYSNAME           = N'AdventureWorks2019'
, @DbNameTgt                          SYSNAME           = N'AdventureWorks2019_Target'
, @OutputDirectoryPsXml               NVARCHAR(MAX)     = N'C:\MSSQL\Backup\BCP\'
, @OutputDirectoryCsv                 NVARCHAR(MAX)     = N'C:\DOCKER_SHARE\Windows\BackupCommon\BCP\'                                                        
, @SchemaNames                        NVARCHAR(MAX)     = N'HumanResources, Production, Purchasing, Sales'
, @TableNames                         NVARCHAR(MAX)     = N'Product*, *Address, *Tax*, Employee*, Work*'
, @SchemaNamesExpt                    NVARCHAR(MAX)     = N'*'
, @TableNamesExpt                     NVARCHAR(MAX)     = N'*History, *Model'
, @ColumnNamesExpt                    NVARCHAR(MAX)     = N'LargePhoto'
, @DataTypesExpt                      NVARCHAR(MAX)     = N'xml'
, @DelimBcpOutputField                VARCHAR(3)        = '|||'
, @ExportIdentityCols                 BIT               = 0
       
EXEC [dbo].[sp_BcpRunner]
                                   @InstanceNameSrc         = @InstanceNameSrc     
                                 , @InstanceNameTgt         = @InstanceNameTgt     
                                 , @SqlAuthentication       = @SqlAuthentication   
                                 , @SqlUserNameSrc          = @SqlUserNameSrc      
                                 , @SqlPasswordSrc          = @SqlPasswordSrc      
                                 , @SqlUserNameTgt          = @SqlUserNameTgt      
                                 , @SqlPasswordTgt          = @SqlPasswordTgt      
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
                                 , @ExportIdentityCols      = @ExportIdentityCols
                                 
