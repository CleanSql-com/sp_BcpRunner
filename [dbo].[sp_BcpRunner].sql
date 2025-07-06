USE [master]
GO

IF (CAST(SUBSTRING(CAST(SERVERPROPERTY('ProductVersion') AS VARCHAR(20)), 1, 2) AS INT) < 14)
BEGIN
    RAISERROR('You can only install/run this sp on SQL Versions older than 14 (2017) if you modify the code in all sections where @DbEngineVersion is used', 18, 1)
END
GO

IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_NAME = 'sp_BcpRunner')
    EXEC ('CREATE PROC dbo.sp_BcpRunner AS SELECT ''stub version, to be replaced''')
GO

EXEC [sys].[sp_MS_marksystemobject] '[dbo].[sp_BcpRunner]';
GO

ALTER PROCEDURE [dbo].[sp_BcpRunner]

/* ==================================================================================================================== */
/* Author:      CleanSql.com © Copyright CleanSql.com                                                                   */
/* Create date: 2025-06-20                                                                                              */
/* Description: Automate Bcp Export/Import using customized column-list and field delimiter, specify source tables as   */
/*              input parameters: @SchemaNames/@TableNames, define custom field delimiter to separate columns in the    */
/*              output .csv files using @DelimBcpOutputField VARCHAR(3)                                                 */
/*              For each @SchemaNames/@TableNames the sp will create an xml FormatFile.Schema.TableName                 */ 
/*              additionally it will create 2 PowerShell scripts:                                                       */
/*              BcpExport.ps1 - run it first, to generate csv files from Source (@InstanceNameSrc/@DbNameSrc)            */
/*              BcpImport.ps1 - run it next, to import csv files generated above into @InstanceNameTgt/@DbNameTgt        */
/*              Running BcpExport.ps1 will produce Schema.TableName.csv file per each input TableName                   */
/*              Running BcpImport.ps1 will import data from Schema.TableName.csv into Target Instance/Db                */
/*              Both ps1 scripts will produce parallel multithreaded asynchronous Bcp runs. To create separate ps1      */ 
/*              files per Table (if you prefer to run them independently) set @CreateSeparatePwrShlFiles = 1            */
/* ==================================================================================================================== */
/* Change History:                                                                                                      */
/* -------------------------------------------------------------------------------------------------------------------- */
/* Date:       Version:  Change:                                                                                        */
/* -------------------------------------------------------------------------------------------------------------------- */
/* 2025-06-20  1.00      Created                                                                                        */
/* 2025-07-05  1.01      Added missing [#DataTypeMapping] float -> sqlflt8                                              */
/*                       Added thousand-comma-formatting to Job-Result numbers of PowerShell output                     */
/* -------------------------------------------------------------------------------------------------------------------- */
/* ==================================================================================================================== */
/* Example use: 

    Export and import all tables:
    1.  From Schemas: 'HumanResources, Production, Purchasing, Sales'
    2. Matching Table Name patterns: 'Product*, *Address, *Tax*, Employee*, Work*'
    Except for:
    1. Any table name that ends with 'History' or 'Model'
    2. Any Column Name 'LargePhoto'
    3. Any Column with data type xml
    4. Any Identity Columns
    Columns inside all output csv files will be delimited with: '|||'

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


    Export and import table [AdventureWorksDW2019].[dbo].[FactResellerSales]:
    Columns inside output csv file will be delimited with: '^|^'

USE [AdventureWorksDW2019]
GO

DECLARE 

  @InstanceNameSrc                    NVARCHAR(128)     = N'Inst1.docker.internal'
, @InstanceNameTgt                    NVARCHAR(128)     = N'Inst2.docker.internal'
, @SqlAuthentication                  BIT               = 1
, @SqlUserNameSrc                     SYSNAME           = 'sa'
, @SqlPasswordSrc                     NVARCHAR(128)     = N'Password1234$'
, @SqlUserNameTgt                     SYSNAME           = 'sa'
, @SqlPasswordTgt                     NVARCHAR(128)     = N'Password1234$'
, @DbNameSrc                          SYSNAME           = N'AdventureWorksDW2019'
, @DbNameTgt                          SYSNAME           = N'AdventureWorksDW2019_Target'
, @OutputDirectoryPsXml               NVARCHAR(MAX)     = N'C:\MSSQL\Backup\BCP\'
, @OutputDirectoryCsv                 NVARCHAR(MAX)     = N'D:\DOCKER_SHARE\Windows\BackupCommon\BCP\'
, @SchemaNames                        NVARCHAR(MAX)     = N'dbo'
, @TableNames                         NVARCHAR(MAX)     = N'FactResellerSales'
, @SchemaNamesExpt                    NVARCHAR(MAX)     = NULL
, @TableNamesExpt                     NVARCHAR(MAX)     = NULL
, @ColumnNamesExpt                    NVARCHAR(MAX)     = NULL
, @DataTypesExpt                      NVARCHAR(MAX)     = NULL
, @DelimBcpOutputField                VARCHAR(3)        = '^|^'
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

*/
/*THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO    */
/*THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE      */
/*AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, */
/*TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE      */
/*SOFTWARE.                                                                                                           */
/*LICENSE: https://github.com/CleanSql-com/sp_BcpRunner?tab=MIT-1-ov-file#readme                                      */
/* ===================================================================================================================*/

    /* Input parameters: */
        @InstanceNameSrc                    NVARCHAR(128) 
      , @InstanceNameTgt                    NVARCHAR(128) 
      , @SqlAuthentication                  BIT           
      , @SqlUserNameSrc                     SYSNAME       = NULL
      , @SqlPasswordSrc                     NVARCHAR(128) = NULL
      , @SqlUserNameTgt                     SYSNAME       = NULL
      , @SqlPasswordTgt                     NVARCHAR(128) = NULL
      , @DbNameSrc                          SYSNAME       
      , @DbNameTgt                          SYSNAME        /* if other than current DB it has to be a valid Target DB Name */            
      , @OutputDirectoryPsXml               NVARCHAR(MAX)  /* directory where SQL will create a PowerShell/XmlFormat files; has to be visible to SQL Server */
      , @OutputDirectoryCsv                 NVARCHAR(MAX)  /* directory where PowerShell script will create csv/zip files; has to be visible to PowerShell scripts that will run bcp export/import */
      
      , @SchemaNames                        NVARCHAR(MAX) 
      , @TableNames                         NVARCHAR(MAX) 
      , @SchemaNamesExpt                    NVARCHAR(MAX) 
      , @TableNamesExpt                     NVARCHAR(MAX) 
      , @ColumnNamesExpt                    NVARCHAR(MAX) /* list here any Column Names that you do not want exported */
      , @DataTypesExpt                      NVARCHAR(MAX) /* list here any Data Types that you do not want exported */

      , @DelimBcpOutputField                VARCHAR(3)    /* character(s) that will separate columns in the ouput csv files */
      , @DelimSrcObjList                    CHAR(1)       = ','   /* character used to delimit the list of Schema/Table names, supplied to @SchemaNames/@TableNames params above */
      , @WildcardChar                       CHAR(1)       = '*'   /* character used as a wildcard in the parameters above, if not used leave as NULL */
      , @ExportAllTablesPerDB               BIT           = 0     /* Set @ExportAllTablesPerDB to = 1 ONLY if you want to ignore the @SchemaNames/@TableNames specified above and export ALL TABLES IN THE ENTIRE DB */
      , @ExportComputedCols                 BIT           = 0     /* assuming computed cols on Target are defined identically as on Source (saves space in .csv), change to 1 if you want to export/import them */
      , @ExportIdentityCols                 BIT           = 1
      , @ExportColumnHeaders                BIT           = 0     /* set = 1 only if you want to see the Column Names in the csv files, not critical for Import to work, will slow down Export/Import with larger files */                                                                                              
                                                                  

      , @CreateXmlFormatFile                BIT           = 1
      , @CreatePwrShlFile                   BIT           = 1
      , @CreateSeparatePwrShlFiles          BIT           = 0
      , @AllowNotNullColumnsAsExceptions    BIT           = 0
            
      , @WhatIf                             BIT           = 0     /* 1 = only printout commands to be executed, without running them */
      , @KeepSourceCollation                BIT           = 0

AS
BEGIN
SET NOCOUNT ON;
SET XACT_ABORT ON;

DECLARE

/* ==================================================================================================================== */
/* ----------------------------------------- VARIABLE AND TEMP TABLE DECLARATIONS: ------------------------------------ */
/* ==================================================================================================================== */

  /* Internal parameters: */
    @SpCurrentVersion      CHAR(5) = '1.01'
  , @ObjectId              INT
  , @SchemaName            SYSNAME
  , @TableName             SYSNAME
  , @LineId                INT
  , @LineIdMax             INT
  , @LineOfCode            NVARCHAR(MAX)
  , @SelectedTableId       INT
  , @SelectedTableIdMax    INT
  , @CanBcpInDirect        BIT
  , @DbEngineVersion       INT
  , @DbCollation           VARCHAR(256)
  , @Id                    INT
  , @IdMax                 INT

  /* Table-Count Variables: */
  , @CountTablesSelected   INT           = 0
  , @CountColumnList       INT           = 0
  , @CountExceptionList    INT           = 0

  /* File variables: */
  , @OutputFileNameXmlFmt  NVARCHAR(128)
  , @OutputFileNamePwrShl  NVARCHAR(128)
  , @FileContentXmlFmt     NVARCHAR(MAX)
  , @PwrShlBcpOutHeader    NVARCHAR(MAX)
  , @PwrShlBcpOutFooter    NVARCHAR(MAX)
  , @PwrShlBcpOutSep       NVARCHAR(MAX)
  , @PwrShlBcpOutAll       NVARCHAR(MAX)
  , @PwrShlBcpOutFinal     NVARCHAR(MAX)
  , @PwrShlBcpInHeader     NVARCHAR(MAX)
  , @PwrShlBcpInFooter     NVARCHAR(MAX)
  , @PwrShlBcpInSep        NVARCHAR(MAX)
  , @PwrShlBcpInAll        NVARCHAR(MAX)
  , @PwrShlBcpInFinal      NVARCHAR(MAX)
  , @crlf                  CHAR(2)       = CHAR(13) + CHAR(10)


  /* Error handling varaibles: */
  , @ErrorSeverity11      INT           = 11 /* 11 changes the message color to red */
  , @ErrorSeverity18      INT           = 18 /* 16 and below does not break execution */
  , @ErrorState           INT           = 1
  , @ErrorMessage         NVARCHAR(MAX)

  /* Ole Automation variables: */
  , @ObjectToken          INT
  , @File                 INT
  , @FileExists           BIT           = 0;

  /* Table variables: */
  DECLARE @_SchemaNames TABLE
  (
      [Id]               INT     NOT NULL PRIMARY KEY CLUSTERED IDENTITY(1, 1)
    , [SchemaName]       SYSNAME NOT NULL
    , [ContainsWildcard] BIT     NULL
  );
  DECLARE @_TableNames TABLE
  (
      [Id]               INT     NOT NULL PRIMARY KEY CLUSTERED IDENTITY(1, 1)
    , [TableName]        SYSNAME NOT NULL
    , [ContainsWildcard] BIT     NULL
  );
  
  DECLARE @_SchemaNamesExpt TABLE
  (
      [Id]               INT     NOT NULL PRIMARY KEY CLUSTERED IDENTITY(1, 1)
    , [SchemaName]       SYSNAME NOT NULL
    , [ContainsWildcard] BIT     NULL
  );
  DECLARE @_TableNamesExpt TABLE
  (
      [Id]               INT     NOT NULL PRIMARY KEY CLUSTERED IDENTITY(1, 1)
    , [TableName]        SYSNAME NOT NULL
    , [ContainsWildcard] BIT     NULL
  );
    DECLARE @_ColumnNamesExpt TABLE
  (
      [Id]               INT     NOT NULL PRIMARY KEY CLUSTERED IDENTITY(1, 1)
    , [ColumnName]       SYSNAME NOT NULL
    , [ContainsWildcard] BIT     NULL
  );
  DECLARE @_DataTypesExpt TABLE
  (
      [Id]               INT     NOT NULL PRIMARY KEY CLUSTERED IDENTITY(1, 1)
    , [DataTypeName]     SYSNAME NOT NULL
    , [ContainsWildcard] BIT     NULL
  );   

PRINT(CONCAT('/* Current SP Version: ', @SpCurrentVersion, IIF(@WhatIf = 1, 'with @WhatIf = 1 - no actual changes will be made', ''), ' */'))

/* ==================================================================================================================== */
/* ----------------------------------------- VALIDATE INPUT PARAMETERS: ----------------------------------------------- */
/* ==================================================================================================================== */

SELECT @DbEngineVersion = CAST(SUBSTRING(CAST(SERVERPROPERTY('ProductVersion') AS VARCHAR(20)), 1, 2) AS INT);
IF (@DbEngineVersion < 14)
BEGIN
    SET @ErrorMessage = 'You can only install/run this sp on SQL Versions older than 14 (2017) if you modify the code in all sections where @DbEngineVersion is used'
    GOTO ERROR
END

IF (@ExportAllTablesPerDB = 0 OR @ExportAllTablesPerDB IS NULL) AND (@SchemaNames IS NULL OR @TableNames IS NULL OR LEN(@SchemaNames) = 0 OR LEN(@TableNames) = 0)
BEGIN
    SET @ErrorMessage = N'@SchemaNames AND @TableNames parameters can not be empty, unless you want to truncate ALL tables per DB by using @ExportAllTablesPerDB = 1';
    GOTO ERROR
END;

IF @ExportAllTablesPerDB = 1 AND (LEN(@SchemaNames) > 0 OR LEN(@TableNames) > 0)
BEGIN
    SET @ErrorMessage = N'If you want to truncate ALL tables per DB by using @ExportAllTablesPerDB = 1 then both @SchemaNames AND @TableNames must be empty.';
    GOTO ERROR
END;

IF (LEN(@SchemaNamesExpt) > 0 AND LEN(@TableNamesExpt) = 0) OR (LEN(@SchemaNamesExpt) = 0 AND LEN(@TableNamesExpt) > 0)
BEGIN
    SET @ErrorMessage = N'If you want to add any exceptions then both @SchemaNamesExpt and @TableNamesExpt must contain a value';
    GOTO ERROR
END;


INSERT INTO @_SchemaNames ([SchemaName])
SELECT DISTINCT
       TRIM([value])
FROM STRING_SPLIT(REPLACE(@SchemaNames, @crlf, ''), @DelimSrcObjList)
WHERE LEN(TRIM([value])) > 0;

INSERT INTO @_TableNames ([TableName])
SELECT DISTINCT
       TRIM([value])
FROM STRING_SPLIT(REPLACE(@TableNames, @crlf, ''), @DelimSrcObjList)
WHERE LEN(TRIM([value])) > 0;

INSERT INTO @_SchemaNamesExpt ([SchemaName])
SELECT DISTINCT
       TRIM([value])
FROM STRING_SPLIT(REPLACE(@SchemaNamesExpt, @crlf, ''), @DelimSrcObjList)
WHERE LEN(TRIM([value])) > 0;

INSERT INTO @_TableNamesExpt ([TableName])
SELECT DISTINCT
       TRIM([value])
FROM STRING_SPLIT(REPLACE(@TableNamesExpt, @crlf, ''), @DelimSrcObjList)
WHERE LEN(TRIM([value])) > 0;

INSERT INTO @_ColumnNamesExpt ([ColumnName])
SELECT DISTINCT
       TRIM([value])
FROM STRING_SPLIT(REPLACE(@ColumnNamesExpt, @crlf, ''), @DelimSrcObjList)
WHERE LEN(TRIM([value])) > 0;

INSERT INTO @_DataTypesExpt ([DataTypeName])
SELECT DISTINCT
       TRIM([value])
FROM STRING_SPLIT(REPLACE(@DataTypesExpt, @crlf, ''), @DelimSrcObjList)
WHERE LEN(TRIM([value])) > 0;

IF (@WildcardChar IS NOT NULL)
BEGIN
    UPDATE @_SchemaNames SET [ContainsWildcard] = IIF(CHARINDEX(@WildcardChar, [SchemaName], 0) > 0, 1, 0)
    UPDATE @_TableNames SET [ContainsWildcard] = IIF(CHARINDEX(@WildcardChar, [TableName], 0) > 0, 1, 0)
    UPDATE @_SchemaNamesExpt SET [ContainsWildcard] = IIF(CHARINDEX(@WildcardChar, [SchemaName], 0) > 0, 1, 0)
    UPDATE @_TableNamesExpt SET [ContainsWildcard] = IIF(CHARINDEX(@WildcardChar, [TableName], 0) > 0, 1, 0)
    UPDATE @_ColumnNamesExpt SET [ContainsWildcard] = IIF(CHARINDEX(@WildcardChar, [ColumnName], 0) > 0, 1, 0)
    UPDATE @_DataTypesExpt SET [ContainsWildcard] = IIF(CHARINDEX(@WildcardChar, [DataTypeName], 0) > 0, 1, 0)
END

/* Expand all wildcard character entries into table variables:  */
IF EXISTS (SELECT 1 FROM @_SchemaNames WHERE [ContainsWildcard] = 1)
BEGIN
    SELECT @Id = MIN([Id]), @IdMax = MAX([Id]) FROM @_SchemaNames WHERE [ContainsWildcard] = 1;
    WHILE (@Id <= @IdMax)
    BEGIN
        MERGE @_SchemaNames AS TARGET USING   
        (
            SELECT [name] AS [SchemaName] FROM sys.schemas WHERE [name] LIKE (SELECT REPLACE([SchemaName], @WildcardChar, '%') FROM @_SchemaNames WHERE [Id] = @Id) 
        ) AS SOURCE
        ON TARGET.[SchemaName] = SOURCE.[SchemaName]
        WHEN NOT MATCHED BY TARGET THEN
        INSERT ([SchemaName]) VALUES (SOURCE.[SchemaName]);
                
        DELETE FROM @_SchemaNames WHERE [Id] = @Id;
        SELECT @Id = COALESCE(MIN([Id]), @Id + 1) FROM @_SchemaNames WHERE [ContainsWildcard] = 1 AND [Id] > @Id;        
    END
END

IF EXISTS (SELECT 1 FROM @_TableNames WHERE [ContainsWildcard] = 1)
BEGIN
    SELECT @Id = MIN([Id]), @IdMax = MAX([Id]) FROM @_TableNames WHERE [ContainsWildcard] = 1;
    WHILE (@Id <= @IdMax)
    BEGIN
        MERGE @_TableNames AS TARGET USING   
        (
            SELECT [name] AS [TableName] FROM sys.tables WHERE [name] LIKE (SELECT REPLACE([TableName], @WildcardChar, '%') FROM @_TableNames WHERE [Id] = @Id) 
        ) AS SOURCE
        ON TARGET.[TableName] = SOURCE.[TableName]
        WHEN NOT MATCHED BY TARGET THEN
        INSERT ([TableName]) VALUES (SOURCE.[TableName]);
                
        DELETE FROM @_TableNames WHERE [Id] = @Id;
        SELECT @Id = COALESCE(MIN([Id]), @Id + 1) FROM @_TableNames WHERE [ContainsWildcard] = 1 AND [Id] > @Id;        
    END
END

IF EXISTS (SELECT 1 FROM @_SchemaNamesExpt WHERE [ContainsWildcard] = 1)
BEGIN
    SELECT @Id = MIN([Id]), @IdMax = MAX([Id]) FROM @_SchemaNamesExpt WHERE [ContainsWildcard] = 1;
    WHILE (@Id <= @IdMax)
    BEGIN
        MERGE @_SchemaNamesExpt AS TARGET USING   
        (
            SELECT [name] AS [SchemaName] FROM sys.schemas WHERE [name] LIKE (SELECT REPLACE([SchemaName], @WildcardChar, '%') FROM @_SchemaNamesExpt WHERE [Id] = @Id) 
        ) AS SOURCE
        ON TARGET.[SchemaName] = SOURCE.[SchemaName]
        WHEN NOT MATCHED BY TARGET THEN
        INSERT ([SchemaName]) VALUES (SOURCE.[SchemaName]);
                
        DELETE FROM @_SchemaNamesExpt WHERE [Id] = @Id;
        SELECT @Id = COALESCE(MIN([Id]), @Id + 1) FROM @_SchemaNamesExpt WHERE [ContainsWildcard] = 1 AND [Id] > @Id;        
    END
END

IF EXISTS (SELECT 1 FROM @_TableNamesExpt WHERE [ContainsWildcard] = 1)
BEGIN
    SELECT @Id = MIN([Id]), @IdMax = MAX([Id]) FROM @_TableNamesExpt WHERE [ContainsWildcard] = 1;
    WHILE (@Id <= @IdMax)
    BEGIN
        MERGE @_TableNamesExpt AS TARGET USING   
        (
            SELECT [name] AS [TableName] FROM sys.tables WHERE [name] LIKE (SELECT REPLACE([TableName], @WildcardChar, '%') FROM @_TableNamesExpt WHERE [Id] = @Id) 
        ) AS SOURCE
        ON TARGET.[TableName] = SOURCE.[TableName]
        WHEN NOT MATCHED BY TARGET THEN
        INSERT ([TableName]) VALUES (SOURCE.[TableName]);
                
        DELETE FROM @_TableNamesExpt WHERE [Id] = @Id;
        SELECT @Id = COALESCE(MIN([Id]), @Id + 1) FROM @_TableNamesExpt WHERE [ContainsWildcard] = 1 AND [Id] > @Id;        
    END
END

IF EXISTS (SELECT 1 FROM @_ColumnNamesExpt WHERE [ContainsWildcard] = 1)
BEGIN
    SELECT @Id = MIN([Id]), @IdMax = MAX([Id]) FROM @_ColumnNamesExpt WHERE [ContainsWildcard] = 1;
    WHILE (@Id <= @IdMax)
    BEGIN
        MERGE @_ColumnNamesExpt AS TARGET USING   
        (
            SELECT [sc].[name] AS [ColumnName]
            FROM sys.columns AS [sc]
            JOIN sys.tables AS [st]
                ON [st].[object_id] = [sc].[object_id]
            WHERE [st].[is_ms_shipped] = 0
            AND   [sc].[name] LIKE (SELECT REPLACE([ColumnName], @WildcardChar, '%') FROM @_ColumnNamesExpt WHERE [Id] = @Id) 
        ) AS SOURCE
        ON TARGET.[ColumnName] = SOURCE.[ColumnName]
        WHEN NOT MATCHED BY TARGET THEN
        INSERT ([ColumnName]) VALUES (SOURCE.[ColumnName]);
                
        DELETE FROM @_ColumnNamesExpt WHERE [Id] = @Id;
        SELECT @Id = COALESCE(MIN([Id]), @Id + 1) FROM @_ColumnNamesExpt WHERE [ContainsWildcard] = 1 AND [Id] > @Id;        
    END
END

IF EXISTS (SELECT 1 FROM @_DataTypesExpt WHERE [ContainsWildcard] = 1)
BEGIN
    SELECT @Id = MIN([Id]), @IdMax = MAX([Id]) FROM @_DataTypesExpt WHERE [ContainsWildcard] = 1;
    WHILE (@Id <= @IdMax)
    BEGIN
        MERGE @_DataTypesExpt AS TARGET USING   
        (
            SELECT [name] AS [DataTypeName] FROM sys.types WHERE [name] LIKE (SELECT REPLACE([DataTypeName], @WildcardChar, '%') FROM @_DataTypesExpt WHERE [Id] = @Id) 
        ) AS SOURCE
        ON TARGET.[DataTypeName] = SOURCE.[DataTypeName]
        WHEN NOT MATCHED BY TARGET THEN
        INSERT ([DataTypeName]) VALUES (SOURCE.[DataTypeName]);
                
        DELETE FROM @_DataTypesExpt WHERE [Id] = @Id;
        SELECT @Id = COALESCE(MIN([Id]), @Id + 1) FROM @_DataTypesExpt WHERE [ContainsWildcard] = 1 AND [Id] > @Id;        
    END
END

/* Verify all SchemaNames requested: */
IF EXISTS (
              SELECT 1
              FROM @_SchemaNames AS [sn]
              LEFT JOIN sys.schemas AS [ss]
                  ON [sn].[SchemaName] = [ss].[name]
              WHERE [ss].[name] IS NULL
          )
BEGIN
    SELECT @ErrorMessage = CONCAT('The following schmema names could not be found in ', DB_NAME(), ' database: [', STRING_AGG([sn].[SchemaName], ','), ']')
    FROM @_SchemaNames AS [sn]
    LEFT JOIN sys.schemas AS [ss]
        ON [sn].[SchemaName] = [ss].[name]
    WHERE [ss].[name] IS NULL;
    GOTO ERROR;
END

/* Verify all TableNames requested: */
IF EXISTS (
              SELECT 1
              FROM @_TableNames AS [tn]
              LEFT JOIN sys.tables AS [st]
                  ON [tn].[TableName] = [st].[name]
              WHERE [st].[name] IS NULL
          )
BEGIN
    SELECT @ErrorMessage = CONCAT('The following table names could not be found in ', DB_NAME(), ' database: [', STRING_AGG([tn].[TableName], ','), ']')
    FROM @_TableNames AS [tn]
    LEFT JOIN sys.tables AS [st]
        ON [tn].[TableName] = [st].[name]
    WHERE [st].[name] IS NULL;
    GOTO ERROR;
END

/* Verify all SchemaNamesExpt requested: */
IF EXISTS (
              SELECT 1
              FROM @_SchemaNamesExpt AS [sn]
              LEFT JOIN sys.schemas AS [ss]
                  ON [sn].[SchemaName] = [ss].[name]
              WHERE [ss].[name] IS NULL
          )
BEGIN
    SELECT @ErrorMessage = CONCAT('The following SchemaNames specified as Expt could not be found in ', DB_NAME(), ' database: [', STRING_AGG([sn].[SchemaName], ','), ']')
    FROM @_SchemaNamesExpt AS [sn]
    LEFT JOIN sys.schemas AS [ss]
        ON [sn].[SchemaName] = [ss].[name]
    WHERE [ss].[name] IS NULL;
    GOTO ERROR;
END

/* Verify all TableNamesExpt requested: */
IF EXISTS (
              SELECT 1
              FROM @_TableNamesExpt AS [tn]
              LEFT JOIN sys.tables AS [st]
                  ON [tn].[TableName] = [st].[name]
              WHERE [st].[name] IS NULL
          )
BEGIN
    SELECT @ErrorMessage = CONCAT('The following TableNames specified as Expt could not be found in ', DB_NAME(), ' database: [', STRING_AGG([tn].[TableName], ','), ']')
    FROM @_TableNamesExpt AS [tn]
    LEFT JOIN sys.tables AS [st]
        ON [tn].[TableName] = [st].[name]
    WHERE [st].[name] IS NULL;
    GOTO ERROR;
END

/* Verify all @_ColumnNamesExpt requested: */
IF EXISTS (
              SELECT 1
              FROM @_ColumnNamesExpt AS [cn]
              LEFT JOIN sys.columns AS [sc]
                  ON [cn].[ColumnName] = [sc].[name]
              LEFT JOIN sys.tables AS [st]
                ON [st].[object_id] = [sc].[object_id]
              WHERE [st].[is_ms_shipped] = 0 AND [sc].[name] IS NULL
          )
BEGIN
    SELECT @ErrorMessage = CONCAT('The following column names could not be found in ', DB_NAME(), ' database: [', STRING_AGG([cn].[ColumnName], ','), ']')
    FROM @_ColumnNamesExpt AS [cn]
    LEFT JOIN sys.columns AS [sc]
        ON [cn].[ColumnName] = [sc].[name]
    LEFT JOIN sys.tables AS [st]
      ON [st].[object_id] = [sc].[object_id]
    WHERE [st].[is_ms_shipped] = 0 AND [sc].[name] IS NULL
    GOTO ERROR;
END

/* Verify all @_DataTypesExpt requested: */
IF EXISTS (
              SELECT 1
              FROM @_DataTypesExpt AS [tn]
              LEFT JOIN sys.types AS [st]
                  ON [tn].[DataTypeName] = [st].[name]
              WHERE [st].[name] IS NULL
          )
BEGIN
    SELECT @ErrorMessage = CONCAT('The following datatypes could not be found in ', DB_NAME(), ' database: [', STRING_AGG([tn].[DataTypeName], ','), ']')
    FROM @_DataTypesExpt AS [tn]
    LEFT JOIN sys.types AS [st]
        ON [tn].[DataTypeName] = [st].[name]
    WHERE [st].[name] IS NULL;
    GOTO ERROR;
END

BEGIN TRANSACTION;

/* ==================================================================================================================== */
/* ----------------------------------------- DEFINE TEMP TABLES: ------------------------------------------------------ */
/* ==================================================================================================================== */

DROP TABLE IF EXISTS [#SelectedTables];
CREATE TABLE [#SelectedTables]
(
    [Id]                   INT           NOT NULL PRIMARY KEY CLUSTERED IDENTITY(1, 1)
  , [SchemaID]             INT           NOT NULL
  , [ObjectID]             BIGINT        NOT NULL UNIQUE
  , [SchemaName]           SYSNAME       NOT NULL
  , [TableName]            SYSNAME       NOT NULL
  , [PathBcpOutPowershell] SYSNAME       NULL
  , [PathBcpInPowershell]  SYSNAME       NULL
  , [PathFormatFileXml]    SYSNAME       NULL
  , [IsToBeExported]       BIT           NULL
  , [IsOnExceptionList]    BIT           NULL
  , [CanBcpInDirect]       BIT           NULL
  , [ErrorMessage]         NVARCHAR(MAX) NULL
);

DROP TABLE IF EXISTS [#ColumnList];
CREATE TABLE [#ColumnList]
(
    [ObjectId]                   INT           NOT NULL
  , [column_id]                  INT           NOT NULL
  , [system_type_id]             TINYINT       NOT NULL
  , [user_type_id]               INT           NOT NULL
  , [ColumnName]                 NVARCHAR(258) NOT NULL
  , [DataTypeOriginal]           SYSNAME       NOT NULL
  , [DataTypeTransalted]         SYSNAME       NULL
  , [FmtXsiType]                 SYSNAME       NULL
  , [MaxLength]                  INT           NULL
  , [IsComputed]                 BIT           NOT NULL
  , [IsIdentity]                 BIT           NOT NULL
  , [IsNullable]                 BIT           NOT NULL
  , [IsCharType]                 BIT           NOT NULL
  , [CollationName]              SYSNAME       NULL
  , [ColumnDefinition]           NVARCHAR(MAX) NOT NULL
  , [ColumnDefinitionTranslated] NVARCHAR(MAX) NULL
  , [IsToBeExported]             BIT           NULL
  , [IsOnExceptionList]          BIT           NULL
  , PRIMARY KEY CLUSTERED ([ObjectId], [column_id])
);

DROP TABLE IF EXISTS [#DataTypeMapping]
CREATE TABLE [#DataTypeMapping]
(
    [system_type_id]        TINYINT  NOT NULL
  , [user_type_id]          INT      NOT NULL
  , [SqlDataType]           SYSNAME  NOT NULL
  , [collation_name]        SYSNAME  NULL
  , [is_user_defined]       BIT      NOT NULL
  , [is_table_type]         BIT      NOT NULL
  , [max_length]            SMALLINT NOT NULL
  , [scale]                 TINYINT  NOT NULL
  , [precision]             TINYINT  NOT NULL
  , [FmtXsiType]            SYSNAME  NULL
  , [FmtMaxLength]          INT      NULL
  , PRIMARY KEY CLUSTERED ([system_type_id], [user_type_id])
)

INSERT INTO [#DataTypeMapping] ([system_type_id], [user_type_id], [SqlDataType], [collation_name], [is_user_defined], [is_table_type], [max_length], [scale], [precision])
SELECT DISTINCT
       [st].[system_type_id]
     , [st].[user_type_id]
     , [st].[name] AS [SqlDataType]
     , [st].[collation_name]
     , [st].[is_user_defined]
     , [st].[is_table_type]
     , [st].[max_length]
     , [st].[scale]
     , [st].[precision]
FROM sys.columns AS [sc]
JOIN sys.types AS [st]
    ON [sc].[user_type_id] = [st].[user_type_id]
ORDER BY [st].[system_type_id], [st].[name];

UPDATE [#DataTypeMapping]
SET [FmtXsiType] = 
       CASE [SqlDataType]
           WHEN LOWER('binary')             THEN 'sqlvariant'   -- *
           WHEN LOWER('geography')          THEN 'sqludt'       /* 'varybin' -- bcp format null -x -f: 'udt' (?) */
           WHEN LOWER('hierarchyid')        THEN 'sqludt'
           WHEN LOWER('image')              THEN 'sqlvarybin'   -- *
           WHEN LOWER('money')              THEN 'sqlmoney4'    -- ?
           WHEN LOWER('rowversion ')        THEN 'sqlbinary'    -- *
           WHEN LOWER('sql_variant')        THEN 'sqlvariant'   -- *           
           WHEN LOWER('smallmoney')         THEN 'sqlmoney4'    -- ?
           WHEN LOWER('sysname')            THEN 'sqlnvarchar'  -- *
           WHEN LOWER('text')               THEN 'sqltext'      -- *
           WHEN LOWER('timestamp ')         THEN 'sqlbinary'    -- *           
           WHEN LOWER('uniqueidentifier')   THEN 'sqluniqueid'
           WHEN LOWER('varbinary')          THEN 'sqlvarybin'
           WHEN LOWER('varchar')            THEN 'sqlvarychar'
           WHEN LOWER('xml')                THEN 'sqlnvarchar'      -- * /* bcp format null -x -f: sqlnvarchar (?) */
           WHEN LOWER('float')              THEN 'sqlflt8'
           ELSE CONCAT('sql', [SqlDataType])
       END,
    [FmtMaxLength] = 
       CASE
           /* https://learn.microsoft.com/en-us/sql/relational-databases/import-export/specify-field-length-by-using-bcp-sql-server?view=sql-server-ver16:  */

           WHEN [SqlDataType] IN ( 'binary' )                               THEN IIF([max_length] = -1, 4000, [max_length] * 2)                                -- *
           WHEN [SqlDataType] IN ( 'bigint' )                               THEN 19         /* bcp format null -x -f: 21 (?)  -- -9223372036854775808       */
           WHEN [SqlDataType] IN ( 'bit' )                                  THEN 1
           WHEN [SqlDataType] IN ( 'char', 'nchar', 'varchar', 'nvarchar' ) THEN IIF([max_length] = -1, 4000, [max_length])
           WHEN [SqlDataType] IN ( 'decimal', 'numeric' )                   THEN 41         /* [sc].[precision] + 2 -- +1 for decimal, +1 for sign          */
           WHEN [SqlDataType] IN ( 'date', 'time2' )                        THEN 11
           WHEN [SqlDataType] IN ( 'datetime', 'smalldatetime' )            THEN 24         /*  e.g., 2025-05-16 14:30:59.997                               */
           WHEN [SqlDataType] IN ( 'datetimeoffset' )                       THEN 33         /* (?)                                                          */
           WHEN [SqlDataType] IN ( 'datetime2' )                            THEN IIF([max_length] = 0, 19, 27)
           WHEN [SqlDataType] IN ( 'float' )                                THEN 30
           WHEN [SqlDataType] IN ( 'geography' )                            THEN 5000        /* bcp format null -x -f: none (?)                             */
           WHEN [SqlDataType] IN ( 'hierarchyid' )                          THEN 892
           WHEN [SqlDataType] IN ( 'image' )                                THEN 8000       /* deprecated, use (varbinary(max) instead                      */ -- *
           WHEN [SqlDataType] IN ( 'int' )                                  THEN 12         /* -2147483648                                                  */
           WHEN [SqlDataType] IN ( 'money' )                                THEN 30         /* -922,337,203,685,477.5808                                    */
           WHEN [SqlDataType] IN ( 'real' )                                 THEN 30
           WHEN [SqlDataType] IN ( 'smallint' )                             THEN 7          /* -32768                                                       */
           WHEN [SqlDataType] IN ( 'smallmoney' )                           THEN 30         /* -214,748.3648                                                */
           WHEN [SqlDataType] IN ( 'sql_variant' )                          THEN 8000       /*  (?)                                                         */ -- *
           WHEN [SqlDataType] IN ( 'sysname' )                              THEN 512        /*  internally an nvarchar(128)                                 */ -- *
           WHEN [SqlDataType] IN ( 'text' )                                 THEN 2147483647 /*                                                              */ -- *
           WHEN [SqlDataType] IN ( 'time' )                                 THEN 16         /* bcp format null -x -f: 19 (?)                                */
           WHEN [SqlDataType] IN ( 'timestamp', 'rowversion' )              THEN 18         /* 8-byte binary (16 hex digits + 0x prefix = 18); bcp format null -x -f: 16 (?) */           
           WHEN [SqlDataType] IN ( 'tinyint' )                              THEN 5          /* 255                                                          */
           WHEN [SqlDataType] IN ( 'uniqueidentifier' )                     THEN 37
           WHEN [SqlDataType] IN ( 'varbinary' )                            THEN IIF([max_length] = -1, 4000, [max_length] * 2)
           WHEN [SqlDataType] IN ( 'xml' )                                  THEN -1         /* -1 => remove MAX_LENGTH from format file for that data type  */
       END

DROP TABLE IF EXISTS [#BcpOutPwrShlHeader];
CREATE TABLE [#BcpOutPwrShlHeader]
(
    [ObjectId]   INT           NOT NULL
  , [LineId]     INT           IDENTITY(1, 1) NOT NULL
  , [LineOfCode] NVARCHAR(MAX) NOT NULL
  , PRIMARY KEY CLUSTERED ([ObjectId], [LineId])
);

DROP TABLE IF EXISTS [#BcpOutPwrShlBody];
CREATE TABLE [#BcpOutPwrShlBody]
(
    [ObjectId]   INT           NOT NULL
  , [LineId]     INT           IDENTITY(1, 1) NOT NULL
  , [LineOfCode] NVARCHAR(MAX) NOT NULL
  , PRIMARY KEY CLUSTERED ([ObjectId], [LineId])
);

DROP TABLE IF EXISTS [#BcpOutPwrShlFooter];
CREATE TABLE [#BcpOutPwrShlFooter]
(
    [ObjectId]   INT           NOT NULL
  , [LineId]     INT           IDENTITY(1, 1) NOT NULL
  , [LineOfCode] NVARCHAR(MAX) NOT NULL
  , PRIMARY KEY CLUSTERED ([ObjectId], [LineId])
);


DROP TABLE IF EXISTS [#BcpInPwrShlHeader];
CREATE TABLE [#BcpInPwrShlHeader]
(
    [ObjectId]   INT           NOT NULL
  , [LineId]     INT           IDENTITY(1, 1) NOT NULL
  , [LineOfCode] NVARCHAR(MAX) NOT NULL
  , PRIMARY KEY CLUSTERED ([ObjectId], [LineId])
);

DROP TABLE IF EXISTS [#BcpInPwrShlBody];
CREATE TABLE [#BcpInPwrShlBody]
(
    [ObjectId]   INT           NOT NULL
  , [LineId]     INT           IDENTITY(1, 1) NOT NULL
  , [LineOfCode] NVARCHAR(MAX) NOT NULL
  , PRIMARY KEY CLUSTERED ([ObjectId], [LineId])
);

DROP TABLE IF EXISTS [#BcpInPwrShlFooter];
CREATE TABLE [#BcpInPwrShlFooter]
(
    [ObjectId]   INT           NOT NULL
  , [LineId]     INT           IDENTITY(1, 1) NOT NULL
  , [LineOfCode] NVARCHAR(MAX) NOT NULL
  , PRIMARY KEY CLUSTERED ([ObjectId], [LineId])
);


DROP TABLE IF EXISTS [#BulkInsertFormatFile];
CREATE TABLE [#BulkInsertFormatFile]
(
    [ObjectId]   INT           NOT NULL
  , [LineId]     INT           IDENTITY(1, 1) NOT NULL
  , [LineOfCode] NVARCHAR(MAX) NOT NULL
  , PRIMARY KEY CLUSTERED ([ObjectId], [LineId])
)

DROP TABLE IF EXISTS [#ExceptionList];
CREATE TABLE [#ExceptionList] ([SchemaNameExpt] SYSNAME NOT NULL, [TableNameExpt] SYSNAME NOT NULL);

SELECT @DbCollation = [collation_name] FROM [master].[sys].[databases] WHERE [name] = DB_NAME(); -- <== to do: adjust to read from TgtDb

/* ==================================================================================================================== */
/* ----------------------------------------- COLLECTING METADATA: ----------------------------------------------------- */
/* ==================================================================================================================== */

IF (@WhatIf = 1 )
BEGIN
    PRINT(CONCAT('USE [', DB_NAME(), ']'));
    PRINT(CONCAT('GO', @crlf));
END;

PRINT ('/*--------------------------------------- COLLECTING [#SelectedTables]: ------------------------------------------*/');
IF (@ExportAllTablesPerDB = 1)
BEGIN
    PRINT (CONCAT(
                     N'/* Specified @ExportAllTablesPerDB = 1 so collecting list of all non-system tables in the database: '
                   , QUOTENAME(DB_NAME())
                   , ' */'
                 )
          );

    INSERT INTO [#SelectedTables] ([SchemaID], [ObjectID], [SchemaName], [TableName])
    SELECT [ss].[schema_id] AS [SchemaID]
         , [st].[object_id] AS [ObjectID]
         , [ss].[name] AS [SchemaName]
         , [st].[name] AS [TableName]
    FROM sys.tables AS [st]
    JOIN sys.schemas AS [ss]
        ON [st].[schema_id] = [ss].[schema_id]
    WHERE [st].[is_ms_shipped] <> 1;
END
ELSE 
BEGIN
    INSERT INTO [#SelectedTables] ([SchemaID], [ObjectID], [SchemaName], [TableName])
    SELECT 
      SCHEMA_ID([sn].[SchemaName]) AS [ScemaId]
    , OBJECT_ID(CONCAT(QUOTENAME([sn].[SchemaName]), '.', QUOTENAME([tn].[TableName]))) AS [ObjectId]
    , [sn].[SchemaName]
    , [tn].[TableName] 
    FROM @_SchemaNames AS [sn]
    CROSS JOIN @_TableNames AS [tn]
    WHERE OBJECT_ID(CONCAT(QUOTENAME([sn].[SchemaName]), '.', QUOTENAME([tn].[TableName]))) IS NOT NULL
END;

PRINT ('/*--------------------------------------- END OF COLLECTING [#SelectedTables] ------------------------------------*/');

IF NOT EXISTS (SELECT 1 FROM [#SelectedTables])
BEGIN
    BEGIN
        SET @ErrorMessage = CONCAT('Could not find any objects specified in the list of schemas: [', @SchemaNames, N'] and tables: [', @TableNames, N'] in the database: [', DB_NAME(DB_ID()), N'].');
        GOTO ERROR;
    END;
END;

UPDATE [st]
SET [st].[IsOnExceptionList] = IIF([expt].[IsExpt] = 1, 1, 0)
  , [st].[IsToBeExported] = IIF([expt].[IsExpt] = 1, 0, 1)
FROM [#SelectedTables] AS [st]
OUTER APPLY 
(
    SELECT 1 AS [IsExpt]
    FROM @_SchemaNamesExpt AS [snx]
    CROSS JOIN @_TableNamesExpt AS [tnx]
    WHERE OBJECT_ID(CONCAT(QUOTENAME([snx].[SchemaName]), '.', QUOTENAME([tnx].[TableName]))) IS NOT NULL
    AND SCHEMA_ID([snx].[SchemaName]) = [st].[SchemaID]
    AND OBJECT_ID(CONCAT(QUOTENAME([snx].[SchemaName]), '.', QUOTENAME([tnx].[TableName]))) = [st].[ObjectID]
) AS [expt]

SELECT @CountExceptionList = @@ROWCOUNT
IF (@CountExceptionList > 0) PRINT (CONCAT('/* Flagged ', @CountExceptionList, ' Records in [#SelectedTables] as Exceptions and Updated [IsToBeExported] = 0 */'));

SELECT @CountTablesSelected = COUNT([Id]) FROM [#SelectedTables] WHERE [IsToBeExported] = 1;
PRINT (CONCAT('/* [#SelectedTables] has a total of: ', @CountTablesSelected, ' Records WHERE [IsToBeExported] = 1 */'));

/* ==================================================================================================================== */
/* ----------------------------------------- COLLECT AND SAVE EACH TABLE'S COLUMN LIST: ------------------------------- */
/* ==================================================================================================================== */

INSERT INTO [#ColumnList]
    (
        [ObjectId]
      , [column_id]
      , [system_type_id]
      , [user_type_id]  
      , [ColumnName]
      , [DataTypeOriginal]
      , [DataTypeTransalted]
      , [FmtXsiType]
      , [MaxLength]
      , [IsComputed]
      , [IsIdentity]
      , [IsNullable]
      , [IsCharType]
      , [CollationName]
      , [ColumnDefinition]
      , [ColumnDefinitionTranslated]
    )
SELECT [st].[ObjectID]
     , [sc].[column_id]
     , [sc].[system_type_id]
     , [sc].[user_type_id]
     , [sc].[name] AS [ColumnName]
     , [sdt].[name] [DataTypeOriginal]
     , [udt].[SystemTypeName] AS [DataTypeTransalted]
     , [map].[FmtXsiType]
     , [map].[FmtMaxLength] AS [MaxLength]
     , [sc].[is_computed] AS [IsComputed]
     , [sc].[is_identity] AS [IsIdentity]
     , [sc].[is_nullable] AS [IsNullable]
     , IIF(COALESCE([udt].[collation_name] COLLATE DATABASE_DEFAULT, [map].[collation_name] COLLATE DATABASE_DEFAULT) IS NOT NULL, 1, 0) AS [IsCharType]
     , COALESCE([udt].[collation_name] COLLATE DATABASE_DEFAULT, [map].[collation_name] COLLATE DATABASE_DEFAULT) AS [CollationName]
     , CASE
           WHEN [sc].[is_computed] = 1 THEN 'AS ' + [cc].[definition]
           ELSE
               UPPER([sdt].[name]) + CASE
                                         WHEN [sdt].[name] IN ( 'varchar', 'char', 'varbinary', 'binary', 'text' ) THEN '(' + CASE
                                                                                                                                  WHEN [sc].[max_length] = -1 THEN 'MAX'
                                                                                                                                  ELSE CAST([sc].[max_length] AS VARCHAR(5))
                                                                                                                              END + ')'
                                         WHEN [sdt].[name] IN ( 'nvarchar', 'nchar', 'ntext' ) THEN '(' + CASE
                                                                                                              WHEN [sc].[max_length] = -1 THEN 'MAX'
                                                                                                              ELSE CAST([sc].[max_length] / 2 AS VARCHAR(5))
                                                                                                          END + ')'
                                         WHEN [sdt].[name] IN ( 'datetime2', 'time', 'time2', 'datetimeoffset' ) THEN '(' + CAST([sc].[scale] AS VARCHAR(5)) + ')'
                                         WHEN [sdt].[name] = 'decimal' THEN '(' + CAST([sc].[precision] AS VARCHAR(5)) + ',' + CAST([sc].[scale] AS VARCHAR(5)) + ')'
                                         ELSE ''
                                     END + CASE
                                               WHEN [sdt].[is_user_defined] = 0
                                               AND  [sc].[collation_name] <> @DbCollation
                                               AND  @KeepSourceCollation = 1 THEN ' COLLATE ' + [sc].[collation_name]
                                               ELSE ''
                                           END + CASE WHEN [sc].[is_nullable] = 1 THEN ' NULL' ELSE ' NOT NULL' END
               + CASE
                     WHEN [ic].[is_identity] = 1 THEN ' IDENTITY(' + CAST(ISNULL([ic].[seed_value], '0') AS CHAR(1)) + ',' + CAST(ISNULL([ic].[increment_value], '1') AS CHAR(1)) + ')'
                     ELSE ''
                 END
       END AS [ColumnDefinition]
     , CASE
           WHEN [sc].[is_computed] = 0 THEN
               UPPER([udt].[SystemTypeName]) + CASE
                                                   WHEN [udt].[SystemTypeName] IN ( 'varchar', 'char', 'varbinary', 'binary', 'text' ) THEN '(' + CASE
                                                                                                                                                      WHEN [udt].[max_length] = -1 THEN 'MAX'
                                                                                                                                                      ELSE CAST([udt].[max_length] AS VARCHAR(5))
                                                                                                                                                  END + ')'
                                                   WHEN [udt].[SystemTypeName] IN ( 'nvarchar', 'nchar', 'ntext' ) THEN '(' + CASE
                                                                                                                                  WHEN [udt].[max_length] = -1 THEN 'MAX'
                                                                                                                                  ELSE CAST([udt].[max_length] / 2 AS VARCHAR(5))
                                                                                                                              END + ')'
                                                   WHEN [udt].[SystemTypeName] IN ( 'datetime2', 'time', 'time2', 'datetimeoffset' ) THEN '(' + CAST([udt].[scale] AS VARCHAR(5)) + ')'
                                                   WHEN [udt].[SystemTypeName] = 'decimal' THEN '(' + CAST([udt].[precision] AS VARCHAR(5)) + ',' + CAST([udt].[scale] AS VARCHAR(5)) + ')'
                                                   ELSE ''
                                               END + CASE
                                                         WHEN [sc].[collation_name] <> @DbCollation
                                                         AND  @KeepSourceCollation = 1 THEN ' COLLATE ' + [sc].[collation_name]
                                                         ELSE ''
                                                     END + CASE WHEN [sc].[is_nullable] = 1 THEN ' NULL' ELSE ' NOT NULL' END
               + CASE
                     WHEN [ic].[is_identity] = 1 THEN ' IDENTITY(' + CAST(ISNULL([ic].[seed_value], '0') AS CHAR(1)) + ',' + CAST(ISNULL([ic].[increment_value], '1') AS CHAR(1)) + ')'
                     ELSE ''
                 END
           ELSE NULL
       END AS [ColumnDefinitionTranslated]
FROM sys.columns AS [sc]
JOIN [#SelectedTables] AS [st]
    ON [st].[ObjectID] = [sc].[object_id]
JOIN sys.types AS [sdt]
    ON [sc].[user_type_id] = [sdt].[user_type_id]
LEFT JOIN sys.computed_columns AS [cc]
    ON  [sc].[object_id] = [cc].[object_id]
    AND [sc].[column_id] = [cc].[column_id]
LEFT JOIN sys.identity_columns AS [ic]
    ON  [sc].[is_identity] = 1
    AND [sc].[object_id] = [ic].[object_id]
    AND [sc].[column_id] = [ic].[column_id]
OUTER APPLY (
                SELECT TYPE_NAME([st].[system_type_id]) AS [SystemTypeName]
                     , [st].[max_length]
                     , [st].[precision]
                     , [st].[scale]
                     , [st].[collation_name]
                     , [st].[is_nullable]
                FROM sys.types AS [st]
                WHERE [st].[is_user_defined] = 1
                AND   [st].[user_type_id] = [sc].[user_type_id]
                AND   [st].[system_type_id] = [sdt].[system_type_id]
            ) AS [udt]
OUTER APPLY (
                SELECT DISTINCT 
                    [dtm].[SqlDataType]
                  , [dtm].[FmtXsiType]
                  , [dtm].[FmtMaxLength]
                  , [dtm].[collation_name]
                FROM [#DataTypeMapping] AS [dtm]
                WHERE [sdt].[is_user_defined] = 0 AND [sc].[system_type_id] = [dtm].[system_type_id] AND [sc].[user_type_id] = [dtm].[user_type_id]
                UNION 
                SELECT DISTINCT 
                    [dtm].[SqlDataType]
                  , [dtm].[FmtXsiType]
                  , [dtm].[FmtMaxLength]
                  , [dtm].[collation_name]
                FROM [#DataTypeMapping] AS [dtm]
                WHERE [sdt].[is_user_defined] = 1 AND [sc].[system_type_id] = [dtm].[system_type_id] AND [dtm].[SqlDataType] COLLATE DATABASE_DEFAULT = [udt].[SystemTypeName] COLLATE DATABASE_DEFAULT
            ) [map]
WHERE 1 = 1
AND   [st].[IsToBeExported] = 1;

IF NOT EXISTS (SELECT 1 FROM [#ColumnList])
BEGIN
    SET @ErrorMessage = CONCAT('Could not find any columns for schemas: [', @SchemaNames, N'] and tables: [', @TableNames, N'] in database: [', DB_NAME(DB_ID()), N'].');
    GOTO ERROR;
END;

UPDATE [cl]
SET [cl].[IsOnExceptionList] = IIF([expt].[IsExpt] = 1, 1, 0)
  , [cl].[IsToBeExported] = IIF([expt].[IsExpt] = 1 OR ([cl].[IsComputed] = 1 AND @ExportComputedCols = 0) OR ([cl].[IsIdentity] = 1 AND @ExportIdentityCols = 0), 0, 1)
FROM [#ColumnList] AS [cl]
OUTER APPLY (
                SELECT 1 AS [IsExpt]
                FROM  @_ColumnNamesExpt AS [xc]
                WHERE [xc].[ColumnName] COLLATE DATABASE_DEFAULT = [cl].[ColumnName] COLLATE DATABASE_DEFAULT
                UNION
                SELECT 1 AS [IsExpt]
                FROM  @_DataTypesExpt AS [xt]
                WHERE [xt].[DataTypeName] COLLATE DATABASE_DEFAULT = COALESCE([cl].[DataTypeTransalted], [cl].[DataTypeOriginal]) COLLATE DATABASE_DEFAULT                                        
            ) AS [expt]

IF EXISTS (    
            SELECT 1 FROM [#ColumnList] AS [cl]
            JOIN [#SelectedTables] AS [st] ON [st].[ObjectID] = [cl].[ObjectId]
            WHERE [cl].[IsOnExceptionList] = 0 AND [cl].[MaxLength] IS NULL
          )
BEGIN
    SELECT @ErrorMessage = CONCAT(@crlf, 'The following [Schemas].[Tables].[Columns] have data types missing from [#DataTypeMapping] ', @crlf,
                                  STRING_AGG(CONCAT(QUOTENAME([st].[SchemaName]), '.', QUOTENAME([st].[TableName]), '.', QUOTENAME([cl].[ColumnName])
                                  , ' datatype: ', COALESCE([cl].[DataTypeTransalted], [cl].[DataTypeOriginal])), @crlf), @crlf,
                                  'Eiter obtain the right values from a manually created xml file by running: ', @crlf, 'bcp [', @DbNameSrc, '].[SchemaName].[TableName] format nul -x -f "', @OutputDirectoryCsv, 'FormatFile.xml" -c -t "', @DelimBcpOutputField, '" -r"\r\n" -S ', @InstanceNameSrc, IIF(@SqlAuthentication = 1, CONCAT(' -U ', @SqlUserNameSrc, ' -P ', @SqlPasswordSrc), ' -T'), @crlf,
                                  'Or add that data type to the list of exceptions in parameter: @DataTypesExpt'
                                 )
    FROM [#ColumnList] AS [cl]
    JOIN [#SelectedTables] AS [st]
        ON [st].[ObjectID] = [cl].[ObjectId]
    WHERE [cl].[IsOnExceptionList] = 0 AND [cl].[MaxLength] IS NULL
    GOTO ERROR;
END;

IF (@AllowNotNullColumnsAsExceptions = 0 OR @AllowNotNullColumnsAsExceptions IS NULL) 
AND EXISTS (    
             SELECT 1 FROM [#ColumnList] AS [cl]
             JOIN [#SelectedTables] AS [st] ON [st].[ObjectID] = [cl].[ObjectId]
             WHERE [cl].[IsOnExceptionList] = 1 AND [cl].[IsNullable] = 0
           )
BEGIN
    SELECT @ErrorMessage = CONCAT(@crlf, 'You flagged the following [Schemas].[Tables].[Columns] as [IsOnExceptionList]: ', @crlf,
                                  STRING_AGG(CONCAT(QUOTENAME([st].[SchemaName]), '.', QUOTENAME([st].[TableName]), '.', QUOTENAME([cl].[ColumnName])
                                  , ' datatype: ', COALESCE([cl].[DataTypeTransalted], [cl].[DataTypeOriginal])), @crlf), @crlf,
                                  'But in the Source Database: ', @DbNameSrc, ' these columns are defined as NOT NULL ie: if the TargetTable definition matches the Source your BcpImport on these Tables will fail', @crlf,
                                  'To override this warning and proceed with the Export anyway, using current Column Exceptions set @AllowNotNullColumnsAsExceptions = 1'
                                 )
    FROM [#ColumnList] AS [cl]
    JOIN [#SelectedTables] AS [st]
        ON [st].[ObjectID] = [cl].[ObjectId]
    WHERE [cl].[IsOnExceptionList] = 1 AND [cl].[IsNullable] = 0
    GOTO ERROR;
END;

BEGIN
    UPDATE [st]
    SET [st].[CanBcpInDirect] = IIF([cle].[ColExpt] = 1 OR [clc].[ColCmp] = 1, 0, 1)
    FROM [#SelectedTables] AS [st]
    OUTER APPLY
    (
        SELECT 1 AS [ColExpt] FROM [#ColumnList] AS [cl]
        WHERE [st].[ObjectID] = [cl].[ObjectId] AND [cl].[IsToBeExported] = 0
    ) AS [cle]
    OUTER APPLY 
    (
        SELECT 1 AS [ColCmp] FROM [#ColumnList] AS [cl]
        WHERE [st].[ObjectID] = [cl].[ObjectId] AND [cl].[IsComputed] = 1
    ) AS [clc]
    WHERE [st].[IsToBeExported] = 1
END;

/* ==================================================================================================================== */
/* ----------------------------------------- POPULATE [#BcpOutPwrShl - Files]: ---------------------------------------- */
/* ==================================================================================================================== */

SELECT @ObjectId = -1
INSERT INTO [#BcpOutPwrShlHeader] ([ObjectId], [LineOfCode])
VALUES 
  (@ObjectId, ' ################################################  Common Parameters: ################################################ ')
, (@ObjectId, CONCAT('$outputDir = "', @OutputDirectoryCsv, '"'))
, (@ObjectId, CONCAT('$server = "', @InstanceNameSrc, '"'))
, (@ObjectId, IIF(@SqlAuthentication = 1, CONCAT('$username = "', @SqlUserNameSrc, '"'), ''))
, (@ObjectId, IIF(@SqlAuthentication = 1, CONCAT('$password = "', @SqlPasswordSrc, '"'), ''))
, (@ObjectId, 'if (-not (Test-Path $outputDir)) {')
, (@ObjectId, CONCAT('throw "ERROR: The folder path: ', @OutputDirectoryCsv, ' does not exist. Please create it before running this script."'))
, (@ObjectId, '}')
, (@ObjectId, '$tables = @(')
SELECT @PwrShlBcpOutHeader = STRING_AGG([LineOfCode], @crlf) FROM [#BcpOutPwrShlHeader];

INSERT INTO [#BcpInPwrShlHeader] ([ObjectId], [LineOfCode])
VALUES 
  (@ObjectId, ' ################################################  Common Parameters: ################################################ ')
, (@ObjectId, CONCAT('$inputDir = "', @OutputDirectoryCsv, '"'))
, (@ObjectId, CONCAT('$server = "', @InstanceNameTgt, '"'))
, (@ObjectId, IIF(@SqlAuthentication = 1, CONCAT('$username = "', @SqlUserNameTgt, '"'), ''))
, (@ObjectId, IIF(@SqlAuthentication = 1, CONCAT('$password = "', @SqlPasswordTgt, '"'), ''))
, (@ObjectId, 'if (-not (Test-Path $inputDir)) {')
, (@ObjectId, CONCAT('throw "ERROR: The folder path: ', @OutputDirectoryCsv, ' does not exist. Please create it before running this script."'))
, (@ObjectId, '}')
, (@ObjectId, '$tables = @(')
SELECT @PwrShlBcpInHeader = STRING_AGG([LineOfCode], @crlf) FROM [#BcpInPwrShlHeader];

INSERT INTO [#BcpOutPwrShlFooter] ([ObjectId], [LineOfCode])
VALUES 
  (@ObjectId, CONCAT(@crlf, ' ################################################  Driver Section: ################################################ '))
, (@ObjectId, 		 '$totalJobs = $tables.Count')
, (@ObjectId, 		 '$jobs = @()')
, (@ObjectId, 		 'Write-Host "`n ========================================== Starting Bcp-Export Jobs ================================================================ "')
, (@ObjectId, 		 'for ($i = 0; $i -lt $totalJobs; $i++) {')
, (@ObjectId, 		 '    $table = $tables[$i]')
, (@ObjectId, 		 '    $job = Start-Job -ScriptBlock {')
, (@ObjectId, 		 '        param($table, $outputDir, $server, $username, $password)')
, (@ObjectId, 		 '        $startTime = Get-Date')
, (@ObjectId, 		 '        $query = $table.Query')
, (@ObjectId, 		 '        $formatFile = Join-Path $outputDir ("FormatFile." + $table.TableName + ".xml")')
, (@ObjectId, 		 '        $csvFile = Join-Path $outputDir ($table.TableName + ".csv")')
, (@ObjectId, CONCAT('        $args = @("`"$query`"", "queryout", "`"$csvFile`"", "-S", $server, ', IIF(@SqlAuthentication = 1, ('"-U", $username, "-P", $password,'), '-T'), ' "-C", "1252", "-t", "', @DelimBcpOutputField, '", "-f", "`"$formatFile`"")'))
, (@ObjectId, 		 '        $output = & bcp.exe @args 2>&1 | Out-String')
, (@ObjectId, 		 '        $endTime = Get-Date')
, (@ObjectId, 		 '        $rowsCopied = if ($output -match "(\d+)\s+rows copied") { $matches[1] } else { "?" }')
, (@ObjectId, 		 '        $duration   = if ($output -match "Clock Time.*?:\s+(.+?)\s") { $matches[1] } else { "?" }')
, (@ObjectId, 		 '        $speed      = if ($output -match ":\s+\((.*?)\s+rows per sec\.\)") { $matches[1] } else { "?" }')
, (@ObjectId, 		 '        $errorLines = $output -split "`r?`n" | Where-Object {')
, (@ObjectId, 		 '            ($_ -match "(?i)\b(error|failed)\b") -and')
, (@ObjectId, 		 '            ($_ -notmatch "BCP import with a format file will convert empty strings in delimited columns to NULL")')
, (@ObjectId, 		 '        }')
, (@ObjectId, 		 '        $errorText = if ($errorLines) { $errorLines -join "`n" } else { "None" }')
, (@ObjectId,        '        $durationFormatted   = if ($duration) { "{0:N0}" -f [int]$duration } else { "?" }')
, (@ObjectId,        '        $rowsCopiedFormatted = if ($rowsCopied) { "{0:N0}" -f [int]$rowsCopied } else { "?" }')
, (@ObjectId,        '        $speedFormatted      = if ($speed) { "{0:N0}" -f [int]$speed } else { "?" }')
, (@ObjectId,        '        [pscustomobject]@{')
, (@ObjectId,        '            Table      = $table.TableName')
, (@ObjectId,        '            StartTime  = $startTime')
, (@ObjectId,        '            EndTime    = $endTime')
, (@ObjectId,        '            Duration   = $durationFormatted')
, (@ObjectId,        '            RowsCopied = $rowsCopiedFormatted')
, (@ObjectId,        '            Speed      = $speedFormatted')
, (@ObjectId, 		 '            Error      = $errorText')
, (@ObjectId, 		 '        }')
, (@ObjectId, 		 '    } -ArgumentList $table, $outputDir, $server, $username, $password')
, (@ObjectId, 		 '    $jobs += $job')
, (@ObjectId, 		 '    Write-Progress -Activity "Starting Bcp-Export Jobs" -Status "$($i + 1) of $totalJobs started." -PercentComplete (($i + 1) / $totalJobs * 100)')
, (@ObjectId, 		 '}')
, (@ObjectId, 		 'Write-Host "`n ========================================== Waiting for Bcp-Export Jobs to Complete ================================================= "')
, (@ObjectId, 		 '$results = @()')
, (@ObjectId, 		 '$completed = 0')
, (@ObjectId, 		 'while ($completed -lt $totalJobs) {')
, (@ObjectId, 		 '    foreach ($job in $jobs) {')
, (@ObjectId, 		 '        if ($job.State -eq "Running") { continue }')
, (@ObjectId, 		 '        if (-not $job.HasMoreData) { continue }')
, (@ObjectId, 		 '        $results += Receive-Job -Job $job')
, (@ObjectId, 		 '        Remove-Job -Job $job')
, (@ObjectId, 		 '        $completed++')
, (@ObjectId, 		 '        Write-Progress -Activity "Waiting for Bcp-Export Job Results" -Status "$completed of $totalJobs completed" -PercentComplete (($completed / $totalJobs) * 100)')
, (@ObjectId, 		 '    }')
, (@ObjectId, 		 '    Start-Sleep -Milliseconds 250')
, (@ObjectId, 		 '}')
, (@ObjectId, 		 'Write-Host "`n ========================================== Bcp-Export Job Results: ================================================================= "')
, (@ObjectId, 		 '$results | Select-Object Table, StartTime, EndTime, @{Name = "Duration [ms]"; Expression = { $_.Duration }}, RowsCopied, @{Name = "Speed [rows/s]"; Expression = { $_.Speed }}, Error | Format-Table -AutoSize')

INSERT INTO [#BcpInPwrShlFooter] ([ObjectId], [LineOfCode])
VALUES 
  (@ObjectId, CONCAT(@crlf, ' ################################################  Driver Section: ################################################ '))
, (@ObjectId,       '$totalJobs = $tables.Count')
, (@ObjectId,       '$jobs = @()')
, (@ObjectId, CONCAT('$targetDb = "', @DbNameTgt, '"'))
, (@ObjectId,        'Write-Host "`n ========================================== Starting Bcp-Import Jobs ================================================================ "')
, (@ObjectId,        'for ($i = 0; $i -lt $totalJobs; $i++) {')
, (@ObjectId,        '    $table = $tables[$i]')
, (@ObjectId,        '        $job = Start-Job -ScriptBlock {')
, (@ObjectId,        '    param($table, $inputDir, $server, $username, $password, $targetDb)')
                     
, (@ObjectId,        '    function Execute-Sql {')
, (@ObjectId,        '        param (')
, (@ObjectId,        '            [string]$sql,')
, (@ObjectId,        '            [string]$server,')
, (@ObjectId,        '            [string]$database,')
, (@ObjectId,        '            [string]$username,')
, (@ObjectId,        '            [string]$password')
, (@ObjectId,        '        )')
, (@ObjectId,        '        $connStr = "Server=$server;Database=$database;User ID=$username;Password=$password;TrustServerCertificate=True"')
, (@ObjectId,        '        $conn = New-Object System.Data.SqlClient.SqlConnection $connStr')
, (@ObjectId,        '        $cmd = $conn.CreateCommand()')
, (@ObjectId,        '        $cmd.CommandText = $sql')
, (@ObjectId,        '        try {')
, (@ObjectId,        '            $conn.Open()')
, (@ObjectId,        '            $cmd.ExecuteNonQuery() | Out-Null')
, (@ObjectId,        '            $true')
, (@ObjectId,        '        } catch {')
, (@ObjectId,        '            Write-Host "ERROR executing SQL: $($_.Exception.Message)"')
, (@ObjectId,        '            $false')
, (@ObjectId,        '        } finally {')
, (@ObjectId,        '            $conn.Close()')
, (@ObjectId,        '        }')
, (@ObjectId,        '    }')
                     
, (@ObjectId,        '    $startTime = Get-Date')
, (@ObjectId,        '    $formatFile = Join-Path $inputDir ("FormatFile." + $table["TableName"] + ".xml")')
, (@ObjectId,        '    $csvFile = Join-Path $inputDir ($table["TableName"] + ".csv")')
, (@ObjectId,        IIF(@ExportColumnHeaders = 1, '    $csvFileNoHeader = Join-Path $inputDir ($table["TableName"] + ".NoHeader.csv")', ''))
, (@ObjectId,        IIF(@ExportColumnHeaders = 1, '    Get-Content $csvFile | Where-Object { $_.Trim() -ne "" } | Select-Object -Skip 1 | Out-File -FilePath $csvFileNoHeader -Encoding utf8', ''))

, (@ObjectId,        '    $targetView = "$targetDb." + $table["ViewName"]')
, (@ObjectId,        '    $viewSql = $table["ViewDefinition"]')
                     
, (@ObjectId,        '    # Step 1: Create or update the view:')
, (@ObjectId,        '    $viewCreated = Execute-Sql -sql $viewSql -server $server -database $targetDb -username $username -password $password')                     
, (@ObjectId,        '    if (-not $viewCreated) {')
, (@ObjectId,        '        return [pscustomobject]@{')
, (@ObjectId,        '            Table      = $table["TableName"]')
, (@ObjectId,        '            RowsCopied = 0')
, (@ObjectId,        '            Duration   = "N/A"')
, (@ObjectId,        '            Speed      = "N/A"')
, (@ObjectId,        '            StartTime  = $startTime')
, (@ObjectId,        '            EndTime    = Get-Date')
, (@ObjectId,        '            Error      = "View creation failed for $targetView"')
, (@ObjectId,        '        }')
, (@ObjectId,        '    }')
                     
, (@ObjectId,        '    # Step 2: If view creation succeeded proceed with BCP into that view:')
, (@ObjectId, CONCAT('    $args = @($targetView, "in", "`"', IIF(@ExportColumnHeaders = 1, '$csvFileNoHeader', '$csvFile'), '`"", "-S", $server, "-q", "-E", ', IIF(@SqlAuthentication = 1, ('"-U", $username, "-P", $password,'), '" -T "'),' "-f", "`"$formatFile`"")'))
, (@ObjectId,        '    $output = & bcp.exe @args 2>&1 | Out-String')
, (@ObjectId,        '    $endTime = Get-Date')
, (@ObjectId,        '    $rowsCopied = if ($output -match "(\d+)\s+rows copied") { $matches[1] } else { "?" }')
, (@ObjectId,        '    $duration   = if ($output -match "Clock Time.*?:\s+(.+?)\s") { $matches[1] } else { "?" }')
, (@ObjectId,        '    $speed      = if ($output -match ":\s+\((.*?)\s+rows per sec\.\)") { $matches[1] } else { "?" }')
, (@ObjectId,        '    $errorLines = $output -split "`r?`n" | Where-Object {')
, (@ObjectId,        '        ($_ -match "(?i)\b(error|failed)\b") -and')
, (@ObjectId,        '        ($_ -notmatch "BCP import with a format file will convert empty strings in delimited columns to NULL")')
, (@ObjectId,        '    }')
, (@ObjectId,        '    $errorText = if ($errorLines) { $errorLines -join "`n" } else { "None" }')

, (@ObjectId,       '	  # Step 3: Drop the view if everything succeeded:')
, (@ObjectId,       '	  if ($viewCreated -and -not $errorLines) {')
, (@ObjectId,       '	      $dropSql = "DROP VIEW [$($table["ViewName"].Split(''.'')[-2])].[$($table["ViewName"].Split(''.'')[-1])]"')
, (@ObjectId,       '	      $dropSuccess = Execute-Sql -sql $dropSql -server $server -database $targetDb -username $username -password $password')
, (@ObjectId,       '	      if (-not $dropSuccess) {')
, (@ObjectId,       '	          $errorText += "`nWarning: Failed to drop view $($table["ViewName"])"')
, (@ObjectId,       '	      }')
, (@ObjectId,       '	  }')
, (@ObjectId,        '    $durationFormatted   = if ($duration) { "{0:N0}" -f [int]$duration } else { "?" }')
, (@ObjectId,        '    $rowsCopiedFormatted = if ($rowsCopied) { "{0:N0}" -f [int]$rowsCopied } else { "?" }')
, (@ObjectId,        '    $speedFormatted      = if ($speed) { "{0:N0}" -f [int]$speed } else { "?" }')
, (@ObjectId,        '    [pscustomobject]@{')
, (@ObjectId,        '        Table      = $table.TableName')
, (@ObjectId,        '        StartTime  = $startTime')
, (@ObjectId,        '        EndTime    = $endTime')
, (@ObjectId,        '        Duration   = $durationFormatted')
, (@ObjectId,        '        RowsCopied = $rowsCopiedFormatted')
, (@ObjectId,        '        Speed      = $speedFormatted')
, (@ObjectId, 		 '        Error      = $errorText')
, (@ObjectId,        '    }')
, (@ObjectId,        '')
, (@ObjectId,        '} -ArgumentList $table, $inputDir, $server, $username, $password, $targetDb')
                     
, (@ObjectId,        '    $jobs += $job')
, (@ObjectId,        '    Write-Progress -Activity "Starting Bcp-Import Jobs" -Status "$($i + 1) of $totalJobs started." -PercentComplete (($i + 1) / $totalJobs * 100)')
, (@ObjectId,        '}')
, (@ObjectId,        'Write-Host "`n ========================================== Waiting for Bcp-Import Jobs to Complete ================================================= "')
, (@ObjectId,        '$results = @()')
, (@ObjectId,        '$completed = 0')
, (@ObjectId,        'while ($completed -lt $totalJobs) {')
, (@ObjectId,        '    foreach ($job in $jobs) {')
, (@ObjectId,        '        if ($job.State -eq "Running") { continue }')
, (@ObjectId,        '        if (-not $job.HasMoreData) { continue }')
, (@ObjectId,        '        $results += Receive-Job -Job $job')
, (@ObjectId,        '        Remove-Job -Job $job')
, (@ObjectId,        '        $completed++')
, (@ObjectId,        '        Write-Progress -Activity "Waiting for Bcp-Import Job Results" -Status "$completed of $totalJobs completed" -PercentComplete (($completed / $totalJobs) * 100)')
, (@ObjectId,        '    }')
, (@ObjectId,        '    Start-Sleep -Milliseconds 250')
, (@ObjectId,        '}')
, (@ObjectId,        'Write-Host "`n ========================================== Bcp-Import Job Results: ================================================================= "')
, (@ObjectId,        '$results | Select-Object Table, StartTime, EndTime, @{Name = "Duration [ms]"; Expression = { $_.Duration }}, RowsCopied, @{Name = "Speed [rows/s]"; Expression = { $_.Speed }}, Error | Format-Table -AutoSize')

IF (@ExportColumnHeaders = 1)
BEGIN
INSERT INTO [#BcpOutPwrShlFooter] ([ObjectId], [LineOfCode])
VALUES
  (@ObjectId, 'function Wait-ForFileUnlock {')
, (@ObjectId, '    param (')
, (@ObjectId, '        [string]$Path,')
, (@ObjectId, '        [int]$TimeoutSeconds = 10')
, (@ObjectId, '    )')
, (@ObjectId, '    $stopwatch = [System.Diagnostics.Stopwatch]::StartNew()')
, (@ObjectId, '    if ($Verbose) { Write-Host "[$(Get-Date -Format HH:mm:ss)] [WAIT] Checking file: $Path if ready to insert column headers" }')
, (@ObjectId, '')
, (@ObjectId, '    while ($stopwatch.Elapsed.TotalSeconds -lt $TimeoutSeconds) {')
, (@ObjectId, '        if (Test-Path $Path) {')
, (@ObjectId, '            try {')
, (@ObjectId, '                $fs = [System.IO.File]::Open($Path, ''Open'', ''ReadWrite'', ''None'')')
, (@ObjectId, '                $fs.Close()')
, (@ObjectId, '                if ($Verbose) { Write-Host "[$(Get-Date -Format HH:mm:ss)] [READY] File unlocked: $Path" }')
, (@ObjectId, '                return $true')
, (@ObjectId, '            } catch {')
, (@ObjectId, '                Write-Host "[$(Get-Date -Format HH:mm:ss)] [LOCKED] File is still locked: $Path"')
, (@ObjectId, '                Start-Sleep -Milliseconds 100')
, (@ObjectId, '            }')
, (@ObjectId, '        } else {')
, (@ObjectId, '            Write-Host "[$(Get-Date -Format HH:mm:ss)] [MISSING] File does not exist yet: $Path"')
, (@ObjectId, '            Start-Sleep -Milliseconds 100')
, (@ObjectId, '        }')
, (@ObjectId, '    }')
, (@ObjectId, '    Write-Host "[$(Get-Date -Format HH:mm:ss)] [TIMEOUT] File still locked or missing after $TimeoutSeconds seconds: $Path"')
, (@ObjectId, '    return $false')
, (@ObjectId, '}')

, (@ObjectId, '$Verbose = $false') /* needs to be parametrized in sql */
, (@ObjectId, 'Write-Host "`n ========================================== Inserting Headers into CSV Files ================================================= "')
, (@ObjectId, 'foreach ($table in $tables) {')
, (@ObjectId, '    if ($table.HeaderColumnNames -and $table.HeaderColumnNames.Trim() -ne '''') {')
, (@ObjectId, '        $csvFile = Join-Path $outputDir ($table.TableName + ".csv")')
, (@ObjectId, '        $headerColumnNames = $table.HeaderColumnNames')
, (@ObjectId, '        if (-not (Wait-ForFileUnlock -Path $csvFile)) {')
, (@ObjectId, '            Write-Warning "[$(Get-Date -Format HH:mm:ss)] File locked or inaccessible: $csvFile"')
, (@ObjectId, '            continue')
, (@ObjectId, '        }')
, (@ObjectId, '        $maxAttempts = 10')
, (@ObjectId, '        $attempt = 0')
, (@ObjectId, '        $success = $false')
, (@ObjectId, '        while (-not $success -and $attempt -lt $maxAttempts) {')
, (@ObjectId, '            try {                ')
, (@ObjectId, '                if (-not (Test-Path $csvFile)) {')
, (@ObjectId, '                    throw "File not found: $csvFile"')
, (@ObjectId, '                }                ')
, (@ObjectId, '                $temp = Get-Content -Raw $csvFile -ErrorAction Stop')
, (@ObjectId, '                if ([string]::IsNullOrWhiteSpace($temp)) {')
, (@ObjectId, '                    # File exists but is empty — write header only')
, (@ObjectId, '                    Set-Content -Path $csvFile -Value "$headerColumnNames`r`n"')
, (@ObjectId, '                } else {')
, (@ObjectId, '                    # File has content — write header and append content')
, (@ObjectId, '                    Set-Content -Path $csvFile -Value "$headerColumnNames`r`n$temp"')
, (@ObjectId, '                }                ')
, (@ObjectId, '                $success = $true')
, (@ObjectId, '                Write-Host "[$(Get-Date -Format HH:mm:ss)] [SUCCESS] Header written to: $csvFile (attempt $attempt)"')
, (@ObjectId, '            } catch {')
, (@ObjectId, '                Write-Warning "[$(Get-Date -Format HH:mm:ss)] [RETRY $attempt] Failed to add header to $csvFile ${($_.Exception.Message)}"')
, (@ObjectId, '                Start-Sleep -Milliseconds 300')
, (@ObjectId, '            }')
, (@ObjectId, '        }')
, (@ObjectId, '        if (-not $success) {')
, (@ObjectId, '            Write-Warning "[$(Get-Date -Format HH:mm:ss)] Failed to write header to $csvFile after $maxAttempts attempts"')
, (@ObjectId, '        }')
, (@ObjectId, '    }')
, (@ObjectId, '}')
END

SELECT @PwrShlBcpOutFooter = STRING_AGG([LineOfCode], @crlf) FROM [#BcpOutPwrShlFooter];
SELECT @PwrShlBcpInFooter = STRING_AGG([LineOfCode], @crlf) FROM [#BcpInPwrShlFooter];

SELECT @SelectedTableId = MIN([Id]), @SelectedTableIdMax = MAX([Id]) FROM [#SelectedTables] WHERE [IsToBeExported] = 1;
WHILE (@SelectedTableId <= @SelectedTableIdMax)
BEGIN

    TRUNCATE TABLE [#BcpOutPwrShlBody]
    TRUNCATE TABLE [#BcpInPwrShlBody]

    SELECT @ObjectId = [ObjectID]
         , @SchemaName = [SchemaName]
         , @TableName = [TableName]
         , @CanBcpInDirect = [CanBcpInDirect]
    FROM [#SelectedTables]
    WHERE [Id] = @SelectedTableId AND [IsToBeExported] = 1
    
    INSERT INTO [#BcpOutPwrShlBody] ([ObjectId], [LineOfCode])
    VALUES            
           (@ObjectId, CONCAT(' ################################################ Parameters for: ', QUOTENAME(@SchemaName), '.', QUOTENAME(@TableName), ' ################################################ '))
         , (@ObjectId, '@{') 
         , (@ObjectId, CONCAT('TableName = "', @SchemaName, '.', @TableName, '"'))
         , (@ObjectId,        'Query = "SELECT ');


    INSERT INTO [#BcpInPwrShlBody] ([ObjectId], [LineOfCode])
    VALUES            
           (@ObjectId, CONCAT(' ################################################ Parameters for: ', QUOTENAME(@SchemaName), '.', QUOTENAME(@TableName), ' ################################################ '))
         , (@ObjectId, '@{') 
         , (@ObjectId, CONCAT('TableName = "', @SchemaName, '.', @TableName, '"'))
         , (@ObjectId, CONCAT('CanBcpInDirect = ', IIF(@CanBcpInDirect = 1, '$true', '$false')))
         , (@ObjectId, CONCAT('ViewName = "', (@SchemaName), '.', CONCAT(@TableName, '_BcpIn"')))
         , (@ObjectId, CONCAT('ViewDefinition = "CREATE OR ALTER VIEW ', QUOTENAME(@SchemaName), '.', QUOTENAME(CONCAT(@TableName, '_BcpIn')), ' AS SELECT '));    

    ; WITH [cte] AS (
    SELECT ROW_NUMBER() OVER (PARTITION BY [cl].[ObjectId] ORDER BY [cl].[column_id]) AS [Rn] /* this is in case the 1st column has been flagged as [IsToBeExported] = 0 */
         , [ColumnName]
    FROM [#ColumnList] AS [cl]
    WHERE [ObjectId] = @ObjectId AND [IsToBeExported] = 1
    )    
    INSERT INTO [#BcpOutPwrShlBody] ([ObjectId], [LineOfCode])
    SELECT
          @ObjectId AS [ObjectId]
        , CONCAT(IIF([Rn] > 1, ', ', '  '), QUOTENAME([ColumnName]), ' ')
    FROM [cte]
    ORDER BY [Rn];

    ; WITH [cte] AS (
    SELECT ROW_NUMBER() OVER (PARTITION BY [cl].[ObjectId] ORDER BY [cl].[column_id]) AS [Rn] /* this is in case the 1st column has been flagged as [IsToBeExported] = 0 */
         , [ColumnName]
    FROM [#ColumnList] AS [cl]
    WHERE [ObjectId] = @ObjectId AND [IsToBeExported] = 1
    )    
    INSERT INTO [#BcpInPwrShlBody] ([ObjectId], [LineOfCode])
    SELECT
          @ObjectId AS [ObjectId]
        , CONCAT(IIF([Rn] > 1, ', ', '  '), QUOTENAME([ColumnName]), ' ')
    FROM [cte]
    ORDER BY [Rn];

    INSERT INTO [#BcpOutPwrShlBody] ([ObjectId], [LineOfCode])
    VALUES (@ObjectId, CONCAT(' FROM ', QUOTENAME(@DbNameSrc), '.', QUOTENAME(@SchemaName), '.', QUOTENAME(@TableName), '"'))

    INSERT INTO [#BcpInPwrShlBody] ([ObjectId], [LineOfCode])
    VALUES (@ObjectId, CONCAT(' FROM ', QUOTENAME(@DbNameTgt), '.', QUOTENAME(@SchemaName), '.', QUOTENAME(@TableName), '"'))

    IF (@ExportColumnHeaders = 1)
    BEGIN
        INSERT INTO [#BcpOutPwrShlBody] ([ObjectId], [LineOfCode])
        SELECT @ObjectId
             , CONCAT('HeaderColumnNames = "', STRING_AGG([ColumnName], @DelimBcpOutputField), '"')
        FROM [#ColumnList]
        WHERE [ObjectId] = @ObjectId AND [IsToBeExported] = 1 
        
        INSERT INTO [#BcpInPwrShlBody] ([ObjectId], [LineOfCode])
        SELECT @ObjectId
             , CONCAT('HeaderColumnNames = "', STRING_AGG([ColumnName], @DelimBcpOutputField), '"')
        FROM [#ColumnList]
        WHERE [ObjectId] = @ObjectId AND [IsToBeExported] = 1 
    END
    
    INSERT INTO [#BcpOutPwrShlBody] ([ObjectId], [LineOfCode])
    VALUES (@ObjectId, IIF(@SelectedTableId < @SelectedTableIdMax AND @CreateSeparatePwrShlFiles = 0, '},', CONCAT('}', @crlf, ')')))

    INSERT INTO [#BcpInPwrShlBody] ([ObjectId], [LineOfCode])
    VALUES (@ObjectId, IIF(@SelectedTableId < @SelectedTableIdMax AND @CreateSeparatePwrShlFiles = 0, '},', CONCAT('}', @crlf, ')')))


/* ==================================================================================================================== */
/* ----------------------------------------- POPULATE [#BulkInsertFormatFile]: ---------------------------------------- */
/* ==================================================================================================================== */
    

    TRUNCATE TABLE [#BulkInsertFormatFile]
    INSERT INTO [#BulkInsertFormatFile] ([ObjectId], [LineOfCode])
    VALUES          
           (@ObjectId, '<?xml version="1.0"?>')
         , (@ObjectId, '<BCPFORMAT xmlns="http://schemas.microsoft.com/sqlserver/2004/bulkload/format" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">')
         , (@ObjectId, '<RECORD>');

    ;WITH [XmlFieldNum]
    AS (SELECT [column_id]
             , ROW_NUMBER() OVER (PARTITION BY NULL ORDER BY [column_id]) AS [Rn]
             , [IsCharType]
        FROM [#ColumnList]
        WHERE [ObjectId] = @ObjectId AND [IsToBeExported] = 1)
    INSERT INTO [#BulkInsertFormatFile] ([ObjectId], [LineOfCode])
    SELECT @ObjectId AS [ObjectId]
         , CONCAT(
                     CHAR(9)
                   , '<FIELD ID="'
                   , [xfn].[Rn]
                   , '" xsi:type="CharTerm" TERMINATOR="'
                   , IIF(LEAD([xfn].[Rn]) OVER (PARTITION BY NULL ORDER BY [xfn].[Rn]) IS NULL, '\r\n', @DelimBcpOutputField) /* if this is the last field place double quote after FieldDelimiter */
                   , '"'
                   , IIF([cl].[MaxLength] <> -1, CONCAT(' MAX_LENGTH="', [cl].[MaxLength], '"'), '')
                   , IIF([xfn].[IsCharType] = 1, ' COLLATION="' + [cl].[CollationName] + '"', '')
                   , '/>'
                   , CONCAT(' <!-- ', QUOTENAME([cl].[ColumnName]), ' ', [cl].[FmtXsiType], ' -->')
                 ) AS [LineOfCode]
    FROM [#ColumnList] AS [cl]
    JOIN [XmlFieldNum] AS [xfn]
        ON [cl].[column_id] = [xfn].[column_id]
    WHERE [cl].[ObjectId] = @ObjectId AND [cl].[IsToBeExported] = 1
    ORDER BY [cl].[column_id]

    INSERT INTO [#BulkInsertFormatFile] ([ObjectId], [LineOfCode])
    VALUES (@ObjectId, '</RECORD>')
         , (@ObjectId, '<ROW>');

    ;WITH [XmlFieldNum]
    AS (SELECT [column_id]
             , ROW_NUMBER() OVER (PARTITION BY NULL ORDER BY [column_id]) AS [Rn]
             , [IsCharType]
        FROM [#ColumnList]
        WHERE [ObjectId] = @ObjectId AND [IsToBeExported] = 1)
    INSERT INTO [#BulkInsertFormatFile] ([ObjectId], [LineOfCode])
    SELECT @ObjectId AS [ObjectId]
         , CONCAT(CHAR(9), '<COLUMN SOURCE="', [xfn].[Rn], '" NAME="', [cl].[ColumnName], '" xsi:type="', UPPER([cl].[FmtXsiType]), '"/>')
    FROM [#ColumnList] AS [cl]
    JOIN [XmlFieldNum] AS [xfn]
        ON [cl].[column_id] = [xfn].[column_id]
    WHERE [cl].[ObjectId] = @ObjectId AND [cl].[IsToBeExported] = 1
    ORDER BY [cl].[column_id];

    INSERT INTO [#BulkInsertFormatFile] ([ObjectId], [LineOfCode])
    VALUES (@ObjectId, '</ROW>')
         , (@ObjectId, '</BCPFORMAT>');

/* ==================================================================================================================== */
/* ----------------------------------------- CREATE POWERSHELL FILE(S): ----------------------------------------------- */
/* ==================================================================================================================== */

IF (@CreatePwrShlFile = 1) AND (@WhatIf <> 1)
    BEGIN        
        
        IF (@CreateSeparatePwrShlFiles = 1)
        BEGIN 
            
            /* ======================================= PwrShl Export: ======================================= */

            /* Step 1: Concatenate all rows into a single PwrShl content */
            SELECT @PwrShlBcpOutSep = STRING_AGG([LineOfCode], @crlf) FROM [#BcpOutPwrShlBody];
            SELECT @PwrShlBcpOutFinal = CONCAT(@PwrShlBcpOutHeader, @PwrShlBcpOutSep, @PwrShlBcpOutFooter)

            /* Step 2: Variables for OLE Automation */
            SET @OutputFileNamePwrShl = CONCAT(@OutputDirectoryPsXml, 'BcpExport.', @SchemaName, '.', @TableName, '.ps1');
        
            /* Step 3: Create FileSystemObject */
            EXEC sp_OACreate 'Scripting.FileSystemObject', @ObjectToken OUTPUT;
        
            /* Step 4: Create (or Overwrite) File */
            EXEC sp_OAMethod @ObjectToken, 'CreateTextFile', @File OUTPUT, @OutputFileNamePwrShl, 2, True;  /* 2 = overwrite, True = Unicode */
        
            /* Step 5: Write content to file */
            EXEC sp_OAMethod @File, 'Write', NULL, @PwrShlBcpOutFinal;
        
            /* Step 6: Close file */
            EXEC sp_OAMethod @File, 'Close';
        
            /* Step 7: Check if file exists */
            EXEC sp_OAMethod @ObjectToken, 'FileExists', @FileExists OUTPUT, @OutputFileNamePwrShl;
            
            IF @FileExists = 1
            BEGIN 
                UPDATE [#SelectedTables] SET [PathBcpOutPowershell] = @OutputFileNamePwrShl WHERE [Id] = @SelectedTableId
                PRINT(CONCAT('BcpOut PowerShell File: ', @OutputFileNamePwrShl, ' created successfully'));
            END
            ELSE
            BEGIN
                SET @ErrorMessage = CONCAT('Failed to create the PowerShell File: ', @OutputFileNamePwrShl);
                GOTO ERROR; 
            END   
            
            /* Step 8: Clean up */
            EXEC sp_OADestroy @File;
            EXEC sp_OADestroy @ObjectToken;

           /* ======================================= PwrShl Import: ======================================= */

            /* Step 1: Concatenate all rows into a single PwrShl content */
            SELECT @PwrShlBcpInSep = STRING_AGG([LineOfCode], @crlf) FROM [#BcpInPwrShlBody];
            SELECT @PwrShlBcpInFinal = CONCAT(@PwrShlBcpInHeader, @PwrShlBcpInSep, @PwrShlBcpInFooter)

            /* Step 2: Variables for OLE Automation */
            SET @OutputFileNamePwrShl = CONCAT(@OutputDirectoryPsXml, 'BcpImport.', @SchemaName, '.', @TableName, '.ps1');
        
            /* Step 3: Create FileSystemObject */
            EXEC sp_OACreate 'Scripting.FileSystemObject', @ObjectToken OUTPUT;
        
            /* Step 4: Create (or Overwrite) File */
            EXEC sp_OAMethod @ObjectToken, 'CreateTextFile', @File OUTPUT, @OutputFileNamePwrShl, 2, True;  /* 2 = overwrite, True = Unicode */
        
            /* Step 5: Write content to file */
            EXEC sp_OAMethod @File, 'Write', NULL, @PwrShlBcpInFinal;
        
            /* Step 6: Close file */
            EXEC sp_OAMethod @File, 'Close';
        
            /* Step 7: Check if file exists */
            EXEC sp_OAMethod @ObjectToken, 'FileExists', @FileExists OUTPUT, @OutputFileNamePwrShl;
            
            IF @FileExists = 1
            BEGIN 
                UPDATE [#SelectedTables] SET [PathBcpInPowershell] = @OutputFileNamePwrShl WHERE [Id] = @SelectedTableId
                PRINT(CONCAT('BcpOut PowerShell File: ', @OutputFileNamePwrShl, ' created successfully'));
            END
            ELSE
            BEGIN
                SET @ErrorMessage = CONCAT('Failed to create the PowerShell File: ', @OutputFileNamePwrShl);
                GOTO ERROR; 
            END   
            
            /* Step 8: Clean up */
            EXEC sp_OADestroy @File;
            EXEC sp_OADestroy @ObjectToken;
        END        
        ELSE
        BEGIN
        

            /* Step 1: Accumulate all rows per each table into a single PwrShl content: */
            SELECT @PwrShlBcpOutAll = CONCAT(@PwrShlBcpOutAll, @crlf, STRING_AGG([LineOfCode], @crlf)) FROM [#BcpOutPwrShlBody];
            SELECT @PwrShlBcpInAll = CONCAT(@PwrShlBcpInAll, @crlf, STRING_AGG([LineOfCode], @crlf)) FROM [#BcpInPwrShlBody];

            /* On last while-loop iteration create a single PwrShl BCP Out File: */
            IF (@SelectedTableId = @SelectedTableIdMax)
            BEGIN            
                
                /* ======================================= PwrShl Export: ======================================= */

                /* Step 2: Variables for OLE Automation */
                SET @OutputFileNamePwrShl = CONCAT(@OutputDirectoryPsXml, 'BcpExport.ps1');
            
                /* Step 3: Create FileSystemObject */
                EXEC sp_OACreate 'Scripting.FileSystemObject', @ObjectToken OUTPUT;
            
                /* Step 4: Create (or Overwrite) File */
                EXEC sp_OAMethod @ObjectToken, 'CreateTextFile', @File OUTPUT, @OutputFileNamePwrShl, 2, True;  /* 2 = overwrite, True = Unicode */
            
                SELECT @PwrShlBcpOutFinal = CONCAT(@PwrShlBcpOutHeader, @PwrShlBcpOutAll, @PwrShlBcpOutFooter)
                
                /* Step 5: Write content to file */
                EXEC sp_OAMethod @File, 'Write', NULL, @PwrShlBcpOutFinal;
            
                /* Step 6: Close file */
                EXEC sp_OAMethod @File, 'Close';
            
                /* Step 7: Check if file exists */
                EXEC sp_OAMethod @ObjectToken, 'FileExists', @FileExists OUTPUT, @OutputFileNamePwrShl;
 
                IF @FileExists = 1
                BEGIN 
                    UPDATE [#SelectedTables] SET [PathBcpOutPowershell] = @OutputFileNamePwrShl WHERE [IsToBeExported] = 1
                    PRINT(CONCAT('BcpOut PowerShell File: ', @OutputFileNamePwrShl, ' created successfully'));
                END
                ELSE
                BEGIN
                    SET @ErrorMessage = CONCAT('Failed to create the PowerShell File: ', @OutputFileNamePwrShl);
                    GOTO ERROR; 
                END   
                
                /* Step 8: Clean up */
                EXEC sp_OADestroy @File;
                EXEC sp_OADestroy @ObjectToken;

                /* ======================================= PwrShl Import: ======================================= */

                /* Step 2: Variables for OLE Automation */
                SET @OutputFileNamePwrShl = CONCAT(@OutputDirectoryPsXml, 'BcpImport.ps1');
            
                /* Step 3: Create FileSystemObject */
                EXEC sp_OACreate 'Scripting.FileSystemObject', @ObjectToken OUTPUT;
            
                /* Step 4: Create (or Overwrite) File */
                EXEC sp_OAMethod @ObjectToken, 'CreateTextFile', @File OUTPUT, @OutputFileNamePwrShl, 2, True;  /* 2 = overwrite, True = Unicode */
            
                SELECT @PwrShlBcpInFinal = CONCAT(@PwrShlBcpInHeader, @PwrShlBcpInAll, @PwrShlBcpInFooter)
                
                /* Step 5: Write content to file */
                EXEC sp_OAMethod @File, 'Write', NULL, @PwrShlBcpInFinal;
            
                /* Step 6: Close file */
                EXEC sp_OAMethod @File, 'Close';
            
                /* Step 7: Check if file exists */
                EXEC sp_OAMethod @ObjectToken, 'FileExists', @FileExists OUTPUT, @OutputFileNamePwrShl;
 
                IF @FileExists = 1
                BEGIN 
                    UPDATE [#SelectedTables] SET [PathBcpInPowershell] = @OutputFileNamePwrShl WHERE [IsToBeExported] = 1
                    PRINT(CONCAT('BcpOut PowerShell File: ', @OutputFileNamePwrShl, ' created successfully'));
                END
                ELSE
                BEGIN
                    SET @ErrorMessage = CONCAT('Failed to create the PowerShell File: ', @OutputFileNamePwrShl);
                    GOTO ERROR; 
                END   
                
                /* Step 8: Clean up */
                EXEC sp_OADestroy @File;
                EXEC sp_OADestroy @ObjectToken;

            END
        END
    END
/* ==================================================================================================================== */
/* ----------------------------------------- CREATE XML FORMAT FILE(S): ----------------------------------------------- */
/* ==================================================================================================================== */

IF (@CreateXmlFormatFile = 1) AND (@WhatIf <> 1)
    BEGIN
        
        /* Step 1: Concatenate all rows into a single XML content */
        SELECT @FileContentXmlFmt = STRING_AGG([LineOfCode], @crlf)
        FROM [#BulkInsertFormatFile];
        
        /* Step 2: Variables for OLE Automation */
        SET @OutputFileNameXmlFmt = CONCAT(@OutputDirectoryPsXml, 'FormatFile.', @SchemaName, '.', @TableName, '.xml');
        
        /* Step 3: Create FileSystemObject */
        EXEC sp_OACreate 'Scripting.FileSystemObject', @ObjectToken OUTPUT;
        
        /* Step 4: Create (or Overwrite) File */
        EXEC sp_OAMethod @ObjectToken, 'CreateTextFile', @File OUTPUT, @OutputFileNameXmlFmt, 2, True;  /* 2 = overwrite, True = Unicode */
        
        /* Step 5: Write content to file */
        EXEC sp_OAMethod @File, 'Write', NULL, @FileContentXmlFmt;
        
        /* Step 6: Close file */
        EXEC sp_OAMethod @File, 'Close';

        /* Step 7: Check if file exists */
        EXEC sp_OAMethod @ObjectToken, 'FileExists', @FileExists OUTPUT, @OutputFileNameXmlFmt;

        IF @FileExists = 1
        BEGIN 
            UPDATE [#SelectedTables] SET [PathFormatFileXml] = @OutputFileNameXmlFmt WHERE [Id] = @SelectedTableId
            PRINT(CONCAT('Format File: ', @OutputFileNameXmlFmt, ' created successfully'));
        END
        ELSE
        BEGIN
            SET @ErrorMessage = CONCAT('Failed to create Format File: ', @OutputFileNameXmlFmt);
            GOTO ERROR; 
        END   
        
        /* Step 8: Clean up */
        EXEC sp_OADestroy @File;
        EXEC sp_OADestroy @ObjectToken;         
    END

/* ==================================================================================================================== */
/* ----------------------------------------- PRINTING SUMMARY OUTPUT TABLE: ------------------------------------------- */
/* ==================================================================================================================== */

    IF (@WhatIf = 1)
    BEGIN
        SELECT @LineId = MIN([LineId]), @LineIdMax = MAX([LineId]) FROM [#BcpOutPwrShlBody];
        WHILE (@LineId <= @LineIdMax)
        BEGIN
            SELECT @LineOfCode = [LineOfCode] FROM [#BcpOutPwrShlBody] WHERE [LineId] = @LineId
            PRINT(@LineOfCode)
            SELECT @LineId = COALESCE(MIN([LineId]), @LineId + 1) FROM [#BcpOutPwrShlBody] WHERE [LineId] > @LineId;
        END

        SELECT @LineId = MIN([LineId]), @LineIdMax = MAX([LineId]) FROM [#BulkInsertFormatFile];
        WHILE (@LineId <= @LineIdMax)
        BEGIN
            SELECT @LineOfCode = [LineOfCode] FROM [#BulkInsertFormatFile] WHERE [LineId] = @LineId
            PRINT(@LineOfCode)
            SELECT @LineId = COALESCE(MIN([LineId]), @LineId + 1) FROM [#BulkInsertFormatFile] WHERE [LineId] > @LineId;
        END
    END
    
    SELECT @SelectedTableId = MIN([Id]) FROM [#SelectedTables] WHERE [Id] > @SelectedTableId AND [IsToBeExported] = 1;
END;

/* ==================================================================================================================== */
/* ----------------------------------------- COMMIT OR ROLLBACK: ------------------------------------------------------ */
/* ==================================================================================================================== */

IF (XACT_STATE() <> 0 AND @@TRANCOUNT > 0 AND @@ERROR = 0)
BEGIN
    IF (@WhatIf <> 1)
        PRINT ('/* Committing the transaction */');
    COMMIT TRANSACTION;
END;
GOTO SUMMARY;

ERROR:
BEGIN
    IF (@ErrorMessage IS NOT NULL AND XACT_STATE() <> 0 AND @@TRANCOUNT > 0)
    BEGIN
        ROLLBACK TRANSACTION;
        SET @ErrorMessage = CONCAT('/* Rolling back transaction: */ ', @ErrorMessage);
    END;
    RAISERROR(@ErrorMessage, @ErrorSeverity18, @ErrorState) WITH NOWAIT;
    GOTO FINISH;
END;

/* ==================================================================================================================== */
/* ----------------------------------------- OUTPUT SUMMARY: ---------------------------------------------------------- */
/* ==================================================================================================================== */

SUMMARY:
BEGIN

    SELECT [Id]
         , [ObjectID]
         , [SchemaName]
         , [TableName]
         , [PathBcpOutPowershell]
         , [PathFormatFileXml]
         , [IsOnExceptionList]
         , [IsToBeExported]    
         , [CanBcpInDirect]
    FROM [#SelectedTables];

    SELECT
        [st].[SchemaName]
      , [st].[TableName]      
      , [cl].[ObjectId]
      , [cl].[column_id]
      , [cl].[system_type_id]
      , [cl].[user_type_id]  
      , [cl].[ColumnName]
      , [cl].[DataTypeOriginal]
      , [cl].[DataTypeTransalted]
      , [cl].[IsOnExceptionList]
      , [cl].[IsComputed]
      , [cl].[IsIdentity]
      , [cl].[IsToBeExported]
      , [cl].[IsNullable]
      , [cl].[ColumnDefinition]
      , [cl].[ColumnDefinitionTranslated]
      , [cl].[FmtXsiType]
      , [cl].[MaxLength]
      , [cl].[IsCharType]
      , [cl].[CollationName]
    FROM [#ColumnList] AS [cl]
    JOIN [#SelectedTables] AS [st]
        ON [st].[ObjectID] = [cl].[ObjectId]
    --WHERE [cl].[IsOnExceptionList] = 1
    ORDER BY [st].[Id]
           , [cl].[column_id];
    
END;
FINISH:
END
