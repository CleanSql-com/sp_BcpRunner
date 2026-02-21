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
/*              additionally if @ImportTarget parameter is left as default (MSSQL)it will create 2 PowerShell scripts:  */
/*              BcpExport.ps1 - run it first, to generate csv files from Source (@InstanceNameSrc/@DbNameSrc)           */
/*              BcpImport.ps1 - run it next, to import csv files generated above into @InstanceNameTgt/@DbNameTgt       */
/*              Running BcpExport.ps1 will produce Schema.TableName.csv file per each input TableName                   */
/*              Running BcpImport.ps1 will import data from Schema.TableName.csv into Target Instance/Db                */
/*              Both ps1 scripts will produce parallel multithreaded asynchronous Bcp runs. To create separate ps1      */ 
/*              files per Table (if you prefer to run them independently) set @CreateSeparatePwrShlFiles = 1.           */
/*              If @ImportTarget parameter is set to 'SNOWLFAKE' instead of 2nd PowerShell script the procedure will    */
/*              create a SnowSql script (named as in @SnflImpFileNam, default: 'SnowflakeImport.sql').                  */
/*              Running that SnowSql script on your Snowflake instance will generate commands needed to transfer .csv   */
/*              files from you local drive into a new Snowflake Internal Stage and import them into new Snowflake       */
/*              Tables by inferring schema                                                                              */
/* ==================================================================================================================== */
/* Change History:                                                                                                      */
/* -------------------------------------------------------------------------------------------------------------------- */
/* Date:       Version:  Change:                                                                                        */
/* -------------------------------------------------------------------------------------------------------------------- */
/* 2025-06-20  1.00      Created                                                                                        */
/* 2025-07-05  1.01      Added missing [#DataTypeMapping] float -> sqlflt8                                              */
/*                       Added thousand-comma-formatting to Job-Result numbers of PowerShell output                     */
/* 2025-10-25  1.02      Powershell adjustments that tolerate SQL 2022 bcp output changes                               */
/*                       Support for Snowflake as Target Import Platform                                                */
/* 2026-02-22  1.03      Added [#SqlCodePgToSnflEncMapping] to automatically set PowerShell/Snowflake encoding params   */
/* -------------------------------------------------------------------------------------------------------------------- */
/* ==================================================================================================================== */
/* 
Example 1: 

    Export and import all data from SQL Instance 'Inst1.docker.internal,1433':
        1. From Database AdventureWorks2022 Schemas: 'HumanResources, Production, Purchasing, Sales'
        2. Matching Table Name patterns: 'Product*, *Address, *Tax*, Employee*, Work*'
    Except for:
        1. Any Table Name in any Schema that ends with 'History' or 'Model'
        2. Any Column Name 'LargePhoto'
        3. Any Column with data type xml
        4. Any Identity Columns
    Into SQL Instance 'Inst2.docker.internal,1433' Database 'AdventureWorks2022_Clone'
    using SQL Authentication (Powershell will promt for User name and password)
    Columns inside all output csv files will be delimited with: '^|^'
    Rows inside all output csv files will be delimited with: '~~~' + newline 
    
    All PowerShell/XmlFormat files will be created by this SP in @OutputDirectoryPsXml (C:\MSSQL\Backup\BCP\ as visible by SQL Server)
    All csv exports generated by running the Powershell Export in step 2 will land in @OutputDirectoryCsv 
    (D:\DOCKER_SHARE\Windows\BackupCommon\BCP\ as visible by machine where PowerShell is run)
    If PowerShell is to be run from a client other than SQL Server then for easiest management,
    to make both the SP and PowerShell operate on the same directory map them together or use a common network share accessible to both

USE [AdventureWorks2022]
GO

DECLARE

  @InstanceNameSrc                    NVARCHAR(128)     = N'Inst1.docker.internal,1433'
, @InstanceNameTgt                    NVARCHAR(128)     = N'Inst2.docker.internal,1433'
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
                                 , @ExportColumnHeaders     = @ExportColumnHeaders;
Example 2: 

    Export and import all data from Inst2 to Inst1:
        1. From Database AdventureWorksDW2022 Schema: 'dbo'
        2. From All Tables
    Except for:
        1. Any Table Name beginning with 'Fact*' in Schema 'dbo'
        2. Any Column Name beginning with 'Arabic*'

    Into SQL Instance 'Inst1' Database 'AdventureWorksDW2022_Clone'
    using Trusted Windows Authentication (no promt for User name or password)
    Columns inside all output csv files will be delimited with: '^_^'
    Rows inside all output csv files will be delimited with: '~~~' + newline 
    
    All PowerShell/XmlFormat files will be created by this SP in @OutputDirectoryPsXml (C:\MSSQL\Backup\BCP\ as visible by SQL Server)
    All csv exports generated by running the Powershell Export in step 2 will land in @OutputDirectoryCsv 
    (C:\MSSQL\Backup\BCP\ as visible by machine where PowerShell is run)
    If PowerShell is to be run from a client other than SQL Server then for easiest management,
    to make both the SP and PowerShell operate on the same directory map them together or use a common network share accessible to both

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

Example 3: 

    Export and import all data from SQL Instance 'Inst2.docker.internal,1433':
    using SQL Authentication (Powershell will promt for User name and password)
        1. From Database AdventureWorks2022 Schema: 'Sales'
        2. Tables: 'SalesOrderHeader, SalesOrderDetail, SalesPerson'
    Except for:
        1. Any Columns with datatype 'uniqueidentifier'
        2. Any Identity Columns
    Into SNOWFLAKE Instance where SnowflakeImport.sql will need to be executed to script out the import steps
    Snowflake's target database is specified as 'AdventureWorks2022_Snow'
    Columns inside all output csv files will be delimited with: '^|^'
    Rows inside all output csv files will be delimited with: '~~~' + newline 

    All PowerShell/XmlFormat files will be created by this SP in @OutputDirectoryPsXml (C:\MSSQL\Backup\BCP\ as visible by SQL Server)
    All csv exports generated by running the Powershell Export in step 2 will land in @OutputDirectoryCsv 
    (D:\DOCKER_SHARE\Windows\BackupCommon\BCP\ as visible by machine where PowerShell is run)
    If PowerShell is to be run from a client other than SQL Server then for easiest management,
    to make both the SP and PowerShell operate on the same directory map them together or use a common network share accessible to both


USE [AdventureWorks2022]
GO

DECLARE

  @InstanceNameSrc                    NVARCHAR(128)     = N'Inst2.docker.internal,1433'
, @SqlAuthentication                  BIT               = 1
, @DbNameSrc                          SYSNAME           = N'AdventureWorks2022'
, @DbNameTgt                          SYSNAME           = N'AdventureWorks2022_Snow'
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
      , @InstanceNameTgt                    NVARCHAR(128) = NULL
      , @SqlAuthentication                  BIT
      , @DbNameSrc                          SYSNAME       
      , @DbNameTgt                          SYSNAME        /* if other than current DB it has to be a valid Target DB Name */            
      , @OutputDirectoryPsXml               NVARCHAR(MAX)  /* directory where SQL will create a PowerShell/XmlFormat files; has to be visible to SQL Server */
      , @OutputDirectoryCsv                 NVARCHAR(MAX)  /* directory where PowerShell script will create csv/zip files; has to be visible to PowerShell scripts that will run bcp export/import */
      
      , @SchemaNames                        NVARCHAR(MAX) = NULL
      , @TableNames                         NVARCHAR(MAX) = NULL
      , @SchemaNamesExpt                    NVARCHAR(MAX) = NULL 
      , @TableNamesExpt                     NVARCHAR(MAX) = NULL 
      , @ColumnNamesExpt                    NVARCHAR(MAX) = NULL /* list here any Column Names that you do NOT want exported */
      , @DataTypesExpt                      NVARCHAR(MAX) = NULL /* list here any Data Types that you do NOT want exported */

      , @DelimBcpOutputField                VARCHAR(3)    /* character(s) separating columns in the ouput csv files */
      , @DelimBcpOutputRow                  VARCHAR(16)   /* character(s) separating rows in the ouput csv files in addition to CRLF (which bcp will append automatically)
                                                             they are needed because CRLF may be present in table data fields like xml or varchar and such field could hapen to be the last column */
      , @DelimSrcObjList                    CHAR(1)       = ','   /* character used to delimit the list of Schema/Table names, supplied to @SchemaNames/@TableNames params above */
      , @WildcardChar                       CHAR(1)       = '*'   /* character used as a wildcard in the parameters above, if not used leave as NULL */
      , @ExportAllTablesPerDB               BIT           = 0     /* Set @ExportAllTablesPerDB to = 1 ONLY if you want to ignore the @SchemaNames/@TableNames specified above and export ALL TABLES IN THE ENTIRE DB */
      , @ExportComputedCols                 BIT           = 0     /* assuming computed cols on Target are defined identically as on Source (saves space in .csv), change to 1 if you want to export/import them */
      , @ExportIdentityCols                 BIT           = 1
      , @ExportColumnHeaders                BIT           = 0     /* set = 1 only if you want to see the Column Names in the csv files or if your Target is SNOWFLAKE, not critical for Import into MSSQL to work */                                                                                              
                                                                  

      , @CreateXmlFormatFile                BIT           = 1
      , @CreatePwrShlFile                   BIT           = 1
      , @CreateSeparatePwrShlFiles          BIT           = 0
      , @AllowNotNullColumnsAsExceptions    BIT           = 0
      , @ImportTarget                       VARCHAR(16)   = 'MSSQL'            
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
    @SpCurrentVersion      CHAR(5) = '1.03'
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

  
  
  /* Snowflake import variables: */
  , @SnowflakeSqlImport    NVARCHAR(MAX)
  , @SnflSchemaUtil        NVARCHAR(255) = 'BcpImport'
  , @SnflIntrnStage        NVARCHAR(255) = 'CsvFiles'
  , @SnflImpCtrlTbl        NVARCHAR(255) = 'ImportControlTable'
  , @SnflImpFileFmt        NVARCHAR(255) = 'CsvFormat'
  , @SnflImpFileNam        NVARCHAR(255) = 'SnowflakeImport.sql'
  , @OutputFileNameSnowSql NVARCHAR(128)
  , @newln                 CHAR(6)       = '''\n'''
  , @BackSlsh              CHAR(1)       = CHAR(92)
  , @FwdSlsh               CHAR(1)       = CHAR(47)
  
  /* SqlCodePage/Snowflake Format File Encoding parameters: */
  , @SqlCodePage           SQL_VARIANT
  , @PwrShlEnc             INT
  , @SnflkEnc              VARCHAR(32)


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
    SET @ErrorMessage = N'If you want to export ALL tables per DB by using @ExportAllTablesPerDB = 1 then both @SchemaNames AND @TableNames must be empty.';
    GOTO ERROR
END;

IF (LEN(@SchemaNamesExpt) > 0 AND LEN(COALESCE(@TableNamesExpt, '')) = 0) OR (LEN(COALESCE(@SchemaNamesExpt, '')) = 0 AND LEN(@TableNamesExpt) > 0)
BEGIN
    SET @ErrorMessage = N'If you want to add any exceptions then both @SchemaNamesExpt and @TableNamesExpt must contain a value';
    GOTO ERROR
END;

IF (CHARINDEX(@BackSlsh, @DelimBcpOutputRow, 1) > 0) OR (CHARINDEX(@FwdSlsh, @DelimBcpOutputRow, 1) > 0) 
BEGIN
    SET @ErrorMessage = CONCAT(N'Do not include ', @BackSlsh, ' or ', @FwdSlsh, ' in your @DelimBcpOutputRow parameter, it will confuse a living shit out of bcp.exe');
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

/* Verify @ImportTarget requested: */
SET @ImportTarget = UPPER(@ImportTarget)
IF NOT (@ImportTarget = 'MSSQL' OR @ImportTarget = 'SNOWFLAKE')
BEGIN
    SET @ErrorMessage = CONCAT(N'@ImportTarget Parameter provided: ', @ImportTarget, ' does not match any currently accepted Target Systems: MSSQL/SNOWLFAKE');
    GOTO ERROR
END;

IF UPPER(@ImportTarget) = 'MSSQL' AND @InstanceNameTgt IS NULL
BEGIN
    SET @ErrorMessage = 'Parameter @InstanceNameTgt is required when @ImportTarget is set (or left as default) to MSSQL'
    GOTO ERROR
END;

IF UPPER(@ImportTarget) = 'SNOWFLAKE' AND COALESCE(@ExportColumnHeaders, 0) = 0
BEGIN
    SET @ErrorMessage = 'If your @ImportTarget is SNOWFLAKE then your csv files have to include Column Headers for INFER SCHEMA to work, set @ExportColumnHeaders = 1'
    GOTO ERROR
END;

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
  , [PathImportFile]       SYSNAME       NULL
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
           WHEN LOWER('float')              THEN 'sqlflt8'
           WHEN LOWER('geography')          THEN 'sqludt'       /* 'varybin' -- bcp format null -x -f: 'udt' (?) */
           WHEN LOWER('hierarchyid')        THEN 'sqludt'
           WHEN LOWER('image')              THEN 'sqlvarybin'   -- *
           WHEN LOWER('money')              THEN 'sqlmoney4'    -- ?
           WHEN LOWER('real')               THEN 'sqlflt4'
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

IF (@ImportTarget = 'MSSQL')
BEGIN
    DROP TABLE IF EXISTS [#BcpInPwrShlHeader];
    CREATE TABLE [#BcpInPwrShlHeader]
    (
        [ObjectId] INT NOT NULL
      , [LineId] INT IDENTITY(1, 1) NOT NULL
      , [LineOfCode] NVARCHAR(MAX) NOT NULL
      , PRIMARY KEY CLUSTERED ([ObjectId], [LineId])
    );

    DROP TABLE IF EXISTS [#BcpInPwrShlBody];
    CREATE TABLE [#BcpInPwrShlBody]
    (
        [ObjectId] INT NOT NULL
      , [LineId] INT IDENTITY(1, 1) NOT NULL
      , [LineOfCode] NVARCHAR(MAX) NOT NULL
      , PRIMARY KEY CLUSTERED ([ObjectId], [LineId])
    );

    DROP TABLE IF EXISTS [#BcpInPwrShlFooter];
    CREATE TABLE [#BcpInPwrShlFooter]
    (
        [ObjectId] INT NOT NULL
      , [LineId] INT IDENTITY(1, 1) NOT NULL
      , [LineOfCode] NVARCHAR(MAX) NOT NULL
      , PRIMARY KEY CLUSTERED ([ObjectId], [LineId])
    );
END
ELSE IF (@ImportTarget = 'SNOWFLAKE')
BEGIN
    
    DROP TABLE IF EXISTS [#SnowflakeInSql];
    CREATE TABLE [#SnowflakeInSql]
    (
        [ObjectId] INT NOT NULL
      , [LineId] INT IDENTITY(1, 1) NOT NULL
      , [LineOfCode] NVARCHAR(MAX) NOT NULL
      , PRIMARY KEY CLUSTERED ([ObjectId], [LineId])
    );
END

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

DROP TABLE IF EXISTS [#SqlCodePgToSnflEncMapping];
CREATE TABLE [#SqlCodePgToSnflEncMapping]
(
	[SqlCodePage] SQL_VARIANT	NOT NULL PRIMARY KEY CLUSTERED
  , [PwrShlEnc]   INT			NOT NULL
  , [SnflkEnc]	  VARCHAR(32)	NOT NULL
  , [RegionLang]  VARCHAR(128)	NOT NULL
)

INSERT INTO [#SqlCodePgToSnflEncMapping]
(
  [SqlCodePage] 
, [PwrShlEnc] 
, [SnflkEnc]	  
, [RegionLang]  
)
VALUES
  ( 0		, 1252	  , 'WINDOWS1252'	  , 'System Default (usually Western)')
, ( 437		, 437	  , 'CP437'			  , 'US-ASCII (Original OEM)')
, ( 850		, 850	  , 'CP850'			  , 'Western Europe (OEM)')
, ( 874		, 874	  , 'ISO-8859-11'	  , 'Thai')
, ( 932		, 932	  , 'SJIS'			  , 'Japanese (Shift-JIS)')
, ( 936		, 936	  , 'GB18030'		  , 'Chinese Simplified')
, ( 949		, 949	  , 'UHC'			  , 'Korean')
, ( 950		, 950	  , 'BIG5'			  , 'Chinese Traditional')
, ( 1250	, 1250	  , 'WINDOWS1250'	  , 'Central Europe')
, ( 1251	, 1251	  , 'WINDOWS1251'	  , 'Cyrillic')
, ( 1252	, 1252	  , 'WINDOWS1252'	  , 'Western Europe (Latin 1)')
, ( 1253	, 1253	  , 'WINDOWS1253'	  , 'Greek')
, ( 1254	, 1254	  , 'WINDOWS1254'	  , 'Turkish')
, ( 1255	, 1255	  , 'WINDOWS1255'	  , 'Hebrew')
, ( 1256	, 1256	  , 'WINDOWS1256'	  , 'Arabic')
, ( 1257	, 1257	  , 'WINDOWS1257'	  , 'Baltic')
, ( 1258	, 1258	  , 'WINDOWS1258'	  , 'Vietnamese')
, ( 65001	, 65001	  , 'UTF8'			  , 'Unicode (UTF-8)')

SELECT @SqlCodePage = COALESCE(COLLATIONPROPERTY(CAST(DATABASEPROPERTYEX(DB_NAME(), 'Collation') AS VARCHAR(128)), 'CodePage'), 0)
SELECT @PwrShlEnc = [PwrShlEnc], @SnflkEnc = [SnflkEnc] FROM [#SqlCodePgToSnflEncMapping] WHERE [SqlCodePage] = @SqlCodePage

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

UPDATE  [st]
SET     [st].[IsOnExceptionList] = IIF([expt].[IsExpt] = 1, 1, 0)
      , [st].[IsToBeExported]    = IIF([expt].[IsExpt] = 1, 0, 1)
FROM    [#SelectedTables] AS [st]
OUTER APPLY 
(
    SELECT      1 AS [IsExpt]
    FROM        @_SchemaNamesExpt AS [snx]
    CROSS JOIN  @_TableNamesExpt AS [tnx]
    WHERE       OBJECT_ID(CONCAT(QUOTENAME([snx].[SchemaName]), '.', QUOTENAME([tnx].[TableName]))) IS NOT NULL
    AND         SCHEMA_ID([snx].[SchemaName]) = [st].[SchemaID]
    AND         OBJECT_ID(CONCAT(QUOTENAME([snx].[SchemaName]), '.', QUOTENAME([tnx].[TableName]))) = [st].[ObjectID]
) AS [expt]

SELECT @CountExceptionList = COUNT([Id]) FROM [#SelectedTables] where [IsOnExceptionList] = 1;
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
                                  'Eiter obtain the right values from a manually created xml file by running: ', @crlf, 'bcp [', @DbNameSrc, '].[SchemaName].[TableName] format nul -x -f "', @OutputDirectoryCsv, 'FormatFile.xml" -c -t "', @DelimBcpOutputField, '" -r"\r\n" -S ', @InstanceNameSrc, IIF(@SqlAuthentication = 1, CONCAT(' -U ', 'EnterYourUserNameHere', ' -P ', 'EnterYourPasswordHere'), ' -T'), @crlf,
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
                                  'But in the Source Database: ', @DbNameSrc, ' these columns are defined as NOT NULL ie: if the TargetTable definition matches the Source your Bcp Import on these Tables will fail', @crlf,
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
/* ----------------------------------------- POPULATE [#BcpOutPwrShl/#BcpInPwrShl - Files]: --------------------------- */
/* ==================================================================================================================== */

SELECT @ObjectId = -1
INSERT INTO [#BcpOutPwrShlHeader] ([ObjectId], [LineOfCode])
VALUES 
  (@ObjectId, ' ################################################  Common Parameters: ################################################ ')
, (@ObjectId, CONCAT('$outputDir = "', @OutputDirectoryCsv, '"'))
, (@ObjectId, CONCAT('$server = "', @InstanceNameSrc, '"'))
, (@ObjectId, IIF(@SqlAuthentication = 1, '$username = Read-Host "Enter username"', ''))
, (@ObjectId, IIF(@SqlAuthentication = 1, '$passwordSecure = Read-Host "Enter password" -AsSecureString', ''))
, (@ObjectId, IIF(@SqlAuthentication = 1, '$pwdBstr = [Runtime.InteropServices.Marshal]::SecureStringToBSTR($passwordSecure)', ''))
, (@ObjectId, IIF(@SqlAuthentication = 1, '# bcp.exe cannot accept a SecureString, with SQL Authentication $passwordSecure must be converted into plaintext before calling it', ''))
, (@ObjectId, IIF(@SqlAuthentication = 1, '$password = [Runtime.InteropServices.Marshal]::PtrToStringAuto($pwdBstr)', ''))
, (@ObjectId, 'if (-not (Test-Path $outputDir)) {')
, (@ObjectId, CONCAT('throw "ERROR: The folder path: ', @OutputDirectoryCsv, ' does not exist. Please create it before running this script."'))
, (@ObjectId, '}')
, (@ObjectId, '$tables = @(')
SELECT @PwrShlBcpOutHeader = STRING_AGG([LineOfCode], @crlf) FROM [#BcpOutPwrShlHeader];

IF (@ImportTarget = 'MSSQL')
BEGIN
    INSERT INTO [#BcpInPwrShlHeader] ([ObjectId], [LineOfCode])
    VALUES 
      (@ObjectId, ' ################################################  Common Parameters: ################################################ ')
    , (@ObjectId, CONCAT('$inputDir = "', @OutputDirectoryCsv, '"'))
    , (@ObjectId, CONCAT('$server = "', @InstanceNameTgt, '"'))
    , (@ObjectId, CONCAT('$targetDb = "', @DbNameTgt, '"'))
    , (@ObjectId, IIF(@SqlAuthentication = 1, '$username = Read-Host "Enter username"', ''))
    , (@ObjectId, IIF(@SqlAuthentication = 1, '$passwordSecure = Read-Host "Enter password" -AsSecureString', ''))
    , (@ObjectId, IIF(@SqlAuthentication = 1, '$pwdBstr = [Runtime.InteropServices.Marshal]::SecureStringToBSTR($passwordSecure)', ''))
    , (@ObjectId, IIF(@SqlAuthentication = 1, '# bcp.exe cannot accept a SecureString, with SQL Authentication $passwordSecure must be converted into plaintext before calling it', ''))
    , (@ObjectId, IIF(@SqlAuthentication = 1, '$password = [Runtime.InteropServices.Marshal]::PtrToStringAuto($pwdBstr)', ''))
    , (@ObjectId, 'if (-not (Test-Path $inputDir)) {')
    , (@ObjectId, CONCAT('throw "ERROR: The folder path: ', @OutputDirectoryCsv, ' does not exist. Please create it before running this script."'))
    , (@ObjectId, '}')
    , (@ObjectId, '$tables = @(')
    SELECT @PwrShlBcpInHeader = STRING_AGG([LineOfCode], @crlf) FROM [#BcpInPwrShlHeader];
END
ELSE IF (@ImportTarget = 'SNOWFLAKE')
BEGIN
    INSERT INTO [#SnowflakeInSql] ([ObjectId], [LineOfCode])
    VALUES 
      (@ObjectId, '/* ##############################################  RUN ALL BELOW ON YOUR SNOWFLAKE INSTANCE ############################################## */')
    , (@ObjectId, '/* ##############################################  File Format Definition:   ############################################## */ ')
    , (@ObjectId, CONCAT('CREATE OR REPLACE SCHEMA ', @DbNameTgt, '.', @SnflSchemaUtil, ';'))
    , (@ObjectId, CONCAT('CREATE OR REPLACE FILE FORMAT ', @DbNameTgt, '.', @SnflSchemaUtil, '.', @SnflImpFileFmt))
    , (@ObjectId,        '     TYPE = CSV')
    , (@ObjectId, CONCAT('     PARSE_HEADER = ', IIF(@ExportColumnHeaders = 1, 'TRUE', 'FALSE')))
    , (@ObjectId, CONCAT('     FIELD_DELIMITER = ''', @DelimBcpOutputField, ''''))
    , (@ObjectId, CONCAT('     RECORD_DELIMITER = ''', @DelimBcpOutputRow, '\r\n'''))
    , (@ObjectId,        '     NULL_IF = (''NULL'', ''null'')')
    , (@ObjectId, CONCAT('     ENCODING = ''', @SnflkEnc, ''''))
    , (@ObjectId,        '     EMPTY_FIELD_AS_NULL = TRUE;')
    , (@ObjectId, '')
    
    , (@ObjectId, '/* ##############################################  Control Table Definition: ############################################## */ ')
    , (@ObjectId, CONCAT('CREATE OR REPLACE TABLE ', @DbNameTgt, '.', @SnflSchemaUtil, '.', @SnflImpCtrlTbl))
    , (@ObjectId, '(SCHEMA_NAME NVARCHAR(256) NOT NULL, TABLE_NAME NVARCHAR(256) NOT NULL, PRIMARY KEY (SCHEMA_NAME, TABLE_NAME));')
    , (@ObjectId, CONCAT('INSERT INTO ', @DbNameTgt, '.', @SnflSchemaUtil, '.', @SnflImpCtrlTbl, ' (SCHEMA_NAME, TABLE_NAME) VALUES '))

    ; WITH [cte] AS (
    SELECT ROW_NUMBER() OVER (PARTITION BY NULL ORDER BY [tl].[ObjectId]) AS [Rn] /* this is in case the 1st column has been flagged as [IsToBeExported] = 0 */
         , [SchemaName]
         , [TableName]
    FROM [#SelectedTables] AS [tl] WHERE [IsToBeExported] = 1
    )    
    INSERT INTO [#SnowflakeInSql] ([ObjectId], [LineOfCode])
    SELECT
          @ObjectId AS [ObjectId]
        , CONCAT(IIF([Rn] > 1, ', ', '  '), '(''', [SchemaName], ''',''', [TableName], ''')')
    FROM [cte]
    ORDER BY [Rn];
    INSERT INTO [#SnowflakeInSql] ([ObjectId], [LineOfCode]) VALUES (@ObjectId, ';')

    INSERT INTO [#SnowflakeInSql] ([ObjectId], [LineOfCode])
    VALUES 
      (@ObjectId, '')
    , (@ObjectId, '/* ##############################################  Internal Stage Definition:   ############################################## */ ')
    , (@ObjectId, CONCAT('CREATE OR REPLACE STAGE ', @DbNameTgt, '.', @SnflSchemaUtil, '.', @SnflIntrnStage, ';'))
    , (@ObjectId, '')
    , (@ObjectId, '/* ##############################################  Script Out Snowflake''s PUT FILE Commands:   ########################################### */ ')
    , (@ObjectId, 'EXECUTE IMMEDIATE')
    , (@ObjectId, '$$')
    , (@ObjectId,        'DECLARE')
    , (@ObjectId,        '     data RESULTSET;')
    , (@ObjectId,        '     sch NVARCHAR(256);')
    , (@ObjectId,        '     tbl NVARCHAR(256);')
    , (@ObjectId,        '     put_commands NVARCHAR(4096) DEFAULT '''';')
    , (@ObjectId, CONCAT('     file_path_prefix VARCHAR DEFAULT ''file://', REPLACE(@OutputDirectoryCsv, @BackSlsh, @FwdSlsh), ''';'))
    , (@ObjectId, CONCAT('     stage_name VARCHAR DEFAULT ''@', @DbNameTgt, '.', @SnflSchemaUtil, '.', @SnflIntrnStage, ''';'))
    , (@ObjectId,        'BEGIN')
    , (@ObjectId, CONCAT('     data := (SELECT SCHEMA_NAME, TABLE_NAME FROM ', @DbNameTgt, '.', @SnflSchemaUtil, '.', @SnflImpCtrlTbl, ');'))
    , (@ObjectId,        '     FOR rec IN data DO')
    , (@ObjectId,        '          sch := TO_VARCHAR(rec.SCHEMA_NAME);')
    , (@ObjectId,        '          tbl := TO_VARCHAR(rec.TABLE_NAME);')
    , (@ObjectId,        '          put_commands := CONCAT(put_commands, ''PUT '', file_path_prefix, sch, ''.'', tbl, ''.csv '', stage_name, '';\n'');')
    , (@ObjectId,        '     END FOR;')
    , (@ObjectId,        '     RETURN put_commands;')
    , (@ObjectId,        'END;')
    , (@ObjectId, '$$')
    , (@ObjectId, '')

    INSERT INTO [#SnowflakeInSql] ([ObjectId], [LineOfCode])
    VALUES
      (@ObjectId, '/* ##############################################  Script Out Snowflake''s CREATE TABLE Commands:   ########################################### */ ')
    , (@ObjectId, 'EXECUTE IMMEDIATE')
    , (@ObjectId, '$$')
    , (@ObjectId, 'DECLARE')
    , (@ObjectId, '    data RESULTSET;')
    , (@ObjectId, '    sch VARCHAR;')
    , (@ObjectId, '    tbl VARCHAR;')
    , (@ObjectId, '    create_table_commands STRING DEFAULT '''';')
    , (@ObjectId, CONCAT('    stage_name STRING DEFAULT ''@', @DbNameTgt, '.', @SnflSchemaUtil, '.', @SnflIntrnStage, ''';'))
    , (@ObjectId, 'BEGIN')
    , (@ObjectId, CONCAT('    data := (SELECT SCHEMA_NAME, TABLE_NAME FROM ', @DbNameTgt, '.', @SnflSchemaUtil, '.', @SnflImpCtrlTbl, ');'))
    , (@ObjectId, '    FOR rec IN data DO')
    , (@ObjectId, '        sch := rec.SCHEMA_NAME;')
    , (@ObjectId, '        tbl := rec.TABLE_NAME;')
    , (@ObjectId, '        create_table_commands := CONCAT(')
    , (@ObjectId, '          create_table_commands')
    , (@ObjectId, CONCAT('        ,', @newln, ', ''/* ############################################## Creating Table: '', sch, ''.'', tbl, '' ############################################## */'''))
    , (@ObjectId, CONCAT('        ,', @newln, ', ''CREATE OR REPLACE TABLE ', @DbNameTgt, '.'', sch, ''.'', tbl'))
    , (@ObjectId, CONCAT('        ,', @newln, ', ''USING TEMPLATE (SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*)) FROM TABLE(INFER_SCHEMA('''))
    , (@ObjectId, CONCAT('        ,', @newln, ', ''  LOCATION => ''''@', @DbNameTgt, '.', @SnflSchemaUtil, '.', @SnflIntrnStage, '/'', sch, ''.'', tbl, ''.csv.gz'''''''))
    , (@ObjectId, CONCAT('        ,', @newln, ', '', FILE_FORMAT => ''''', @DbNameTgt, '.', @SnflSchemaUtil, '.', @SnflImpFileFmt, ''''')));'''))
    , (@ObjectId, CONCAT('        ,', @newln, ''))
    , (@ObjectId, CONCAT('        ,', @newln, ', ''COPY INTO ', @DbNameTgt, '.'', sch, ''.'', tbl'))
    , (@ObjectId, CONCAT('        ,', @newln, ', ''FROM ''''@', @DbNameTgt, '.', @SnflSchemaUtil, '.', @SnflIntrnStage, '/'', sch, ''.'', tbl, ''.csv.gz'''''''))
    , (@ObjectId, CONCAT('        ,', @newln, ', ''FILE_FORMAT = (format_name = ''''', @DbNameTgt, '.', @SnflSchemaUtil, '.', @SnflImpFileFmt, ''''')'''))
    , (@ObjectId, CONCAT('        ,', @newln, ', ''MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;'''))
    , (@ObjectId, CONCAT('        ,', @newln, ');'))
    , (@ObjectId, '    END FOR;')
    , (@ObjectId, '    RETURN create_table_commands;')
    , (@ObjectId, 'END;')
    , (@ObjectId, '$$') 
 

    SELECT @SnowflakeSqlImport = STRING_AGG([LineOfCode], @crlf) FROM [#SnowflakeInSql];
END

INSERT INTO [#BcpOutPwrShlFooter] ([ObjectId], [LineOfCode])
VALUES 
  (@ObjectId, CONCAT(@crlf, ' ################################################  Driver Section: ################################################ '))
, (@ObjectId,         '$totalJobs = $tables.Count')
, (@ObjectId,         '$jobs = @()')
, (@ObjectId,         'Write-Host "`n ========================================== Starting Bcp-Export Jobs ================================================================ "')
, (@ObjectId,         'for ($i = 0; $i -lt $totalJobs; $i++) {')
, (@ObjectId,         '    $table = $tables[$i]')
, (@ObjectId,         '    $job = Start-Job -ScriptBlock {')
, (@ObjectId, CONCAT( '        param($table, $outputDir, $server', IIF(@SqlAuthentication = 1, ' , $username, $password)', ')')))
, (@ObjectId,         '        function TryInt($value) {')
, (@ObjectId,         '            if ($value -match ''^[0-9,]+$'') { return [int]($value.Replace('','', '''')) }')
, (@ObjectId,         '            return $null')
, (@ObjectId,         '        }')
, (@ObjectId,         '')
, (@ObjectId,         '        $startTime = Get-Date')
, (@ObjectId,         '        $query = $table.Query')
, (@ObjectId,         '        $formatFile = Join-Path $outputDir ("FormatFile." + $table.TableName + ".xml")')
, (@ObjectId,         '        $csvFile = Join-Path $outputDir ($table.TableName + ".csv")')
, (@ObjectId, CONCAT( '        $args = @("`"$query`"", "queryout", "`"$csvFile`"", "-S", $server, ', 
                     IIF(@SqlAuthentication = 1, ('"-U", $username, "-P", $password,'), '"-T",'), ' "-N", "-t", "', @DelimBcpOutputField, '", "-r", "', @DelimBcpOutputRow, '", "-f", "`"$formatFile`"")'))
, (@ObjectId,         '        $output = & bcp.exe @args 2>&1 | Out-String')
, (@ObjectId,         '        $endTime = Get-Date')
, (@ObjectId,         '')
, (@ObjectId,         '        $rowsCopied = if ($output -match ''([0-9][0-9,]*)\s+rows copied'') { $matches[1].Replace('','', '''') } else { $null }')
, (@ObjectId,         '        $duration   = if ($output -match ''Clock Time.*?:\s*([0-9,]+)'') { $matches[1].Replace('','', '''') } else { $null }')
, (@ObjectId,         '        $avgMatch = [regex]::Match($output, ''Average\s*:\s*\(([^)]*)\)'')')
, (@ObjectId,         '        $speed = if ($avgMatch.Value -match ''\(([0-9]+)\.'' ) { $matches[1] } else { $null }')

, (@ObjectId,         '        $rowsCopiedInt = TryInt $rowsCopied')
, (@ObjectId,         '        $durationInt   = TryInt $duration')
, (@ObjectId,         '        $speedInt      = TryInt $speed')
, (@ObjectId,         '')
, (@ObjectId,         '        $rowsCopiedFormatted = if ($rowsCopiedInt -ne $null) { "{0:N0}" -f $rowsCopiedInt } else { "?" }')
, (@ObjectId,         '        $durationFormatted   = if ($durationInt -ne $null)   { "{0:N0}" -f $durationInt } else { "?" }')
, (@ObjectId,         '        $speedFormatted      = if ($speedInt -ne $null)      { "{0:N0}" -f $speedInt } else { "?" }')
, (@ObjectId,         '')
, (@ObjectId,         '        $errorLines = $output -split "`r?`n" | Where-Object {')
, (@ObjectId,         '            ($_ -match "(?i)\b(error|failed)\b") -and')
, (@ObjectId,         '            ($_ -notmatch "BCP import with a format file will convert empty strings in delimited columns to NULL")')
, (@ObjectId,         '        }')
, (@ObjectId,         '        $errorText = if ($errorLines) { $errorLines -join "`n" } else { "None" }')
, (@ObjectId,         '')
, (@ObjectId,         '        [pscustomobject]@{')
, (@ObjectId,         '            Table      = $table.TableName')
, (@ObjectId,         '            StartTime  = $startTime')
, (@ObjectId,         '            EndTime    = $endTime')
, (@ObjectId,         '            Duration   = $durationFormatted')
, (@ObjectId,         '            RowsCopied = $rowsCopiedFormatted')
, (@ObjectId,         '            Speed      = $speedFormatted')
, (@ObjectId,         '            Error      = $errorText')
, (@ObjectId,         '        }')
, (@ObjectId,         '')
, (@ObjectId, CONCAT( '    } -ArgumentList $table, $outputDir, $server ', IIF(@SqlAuthentication = 1, ', $username, $password', '')))
, (@ObjectId,         '    $jobs += $job')
, (@ObjectId,         '    Write-Progress -Activity "Starting Bcp-Export Jobs" -Status "$($i + 1) of $totalJobs started." -PercentComplete (($i + 1) / $totalJobs * 100)')
, (@ObjectId,         '}')
, (@ObjectId,         'Write-Host "`n ========================================== Waiting for Bcp-Export Jobs to Complete ================================================= "')
, (@ObjectId,         '$results = @()')
, (@ObjectId,         '$completed = 0')
, (@ObjectId,         'while ($completed -lt $totalJobs) {')
, (@ObjectId,         '    foreach ($job in $jobs) {')
, (@ObjectId,         '        if ($job.State -eq "Running") { continue }')
, (@ObjectId,         '        if (-not $job.HasMoreData) { continue }')
, (@ObjectId,         '        $results += Receive-Job -Job $job')
, (@ObjectId,         '        Remove-Job -Job $job')
, (@ObjectId,         '        $completed++')
, (@ObjectId,         '        Write-Progress -Activity "Waiting for Bcp-Export Job Results" -Status "$completed of $totalJobs completed" -PercentComplete (($completed / $totalJobs) * 100)')
, (@ObjectId,         '    }')
, (@ObjectId,         '    Start-Sleep -Milliseconds 250')
, (@ObjectId,         '}')
, (@ObjectId,         'Write-Host "`n ========================================== Bcp-Export Job Results: ================================================================= "')
, (@ObjectId,         '$results | Sort-Object Table | Select-Object Table, StartTime, EndTime, @{Name = "Duration [ms]"; Expression = { $_.Duration }}, RowsCopied, @{Name = "Speed [rows/s]"; Expression = { $_.Speed }}, Error | Format-Table -AutoSize -Wrap');

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
, (@ObjectId, 'Write-Host "`n ========================================== Creating CSV Files With Headers ================================================= "')
, (@ObjectId, 'foreach ($table in $tables) {')
, (@ObjectId, '    if ($table.HeaderColumnNames -and $table.HeaderColumnNames.Trim() -ne '''') {')
, (@ObjectId, '        $csvFile       = Join-Path $outputDir ($table.TableName + ".csv")')
, (@ObjectId, '        $csvFileWHdr   = Join-Path $outputDir ($table.TableName + ".wHeader.csv")')
, (@ObjectId, '        $headerColumns = $table.HeaderColumnNames')
, (@ObjectId, '        if (-not (Test-Path $csvFile)) {')
, (@ObjectId, '            Write-Warning "[$(Get-Date -Format HH:mm:ss)] Original file not found: $csvFile"')
, (@ObjectId, '            continue')
, (@ObjectId, '        }')
, (@ObjectId, '        if (-not (Wait-ForFileUnlock -Path $csvFile)) {')
, (@ObjectId, '            Write-Warning "[$(Get-Date -Format HH:mm:ss)] File locked or inaccessible: $csvFile"')
, (@ObjectId, '            continue')
, (@ObjectId, '        }')
, (@ObjectId, '        try {')
, (@ObjectId, '            ')
, (@ObjectId, CONCAT('            $ExplicitEncoding = [System.Text.Encoding]::GetEncoding(', @PwrShlEnc, ') # try different value if running on non-Windows system'))
, (@ObjectId,        '            # Stream write: first write the header')
, (@ObjectId, CONCAT('            Set-Content -Path $csvFileWHdr -Value ($headerColumns + ''', @DelimBcpOutputRow, ''')'))
, (@ObjectId, '            ')
, (@ObjectId, '            # Then append the rest of the CSV, line-by-line')
, (@ObjectId, '            $reader = [System.IO.StreamReader]::new($csvFile, $ExplicitEncoding)')
, (@ObjectId, '            try {')
, (@ObjectId, '                $writer = [System.IO.StreamWriter]::new($csvFileWHdr, $true, $ExplicitEncoding)')
, (@ObjectId, '                try {')
, (@ObjectId, '                    while (-not $reader.EndOfStream) {')
, (@ObjectId, '                        $writer.WriteLine($reader.ReadLine())')
, (@ObjectId, '                    }')
, (@ObjectId, '                } finally {')
, (@ObjectId, '                    $writer.Dispose()')
, (@ObjectId, '                }')
, (@ObjectId, '            } finally {')
, (@ObjectId, '                $reader.Dispose()')
, (@ObjectId, '            }')
--, (@ObjectId, '            Write-Host "[$(Get-Date -Format HH:mm:ss)] [SUCCESS] Created: $csvFileWHdr"')
, (@ObjectId, '        } catch {')
, (@ObjectId, '            Write-Warning "[$(Get-Date -Format HH:mm:ss)] Failed creating $csvFileWHdr : $($_.Exception.Message)"')
, (@ObjectId, '            continue')
, (@ObjectId, '        }')
, (@ObjectId, '    }')
, (@ObjectId, '}')

, (@ObjectId, 'Write-Host "`n ========================================== Cleaning Up and Renaming CSV Files ================================================= "')
, (@ObjectId, 'foreach ($table in $tables) {')
, (@ObjectId, '    $csvFile     = Join-Path $outputDir ($table.TableName + ".csv")')
, (@ObjectId, '    $csvFileWHdr = Join-Path $outputDir ($table.TableName + ".wHeader.csv")')
, (@ObjectId, '    if (Test-Path $csvFileWHdr) {')
, (@ObjectId, '        $fileInfo = Get-Item $csvFileWHdr')
, (@ObjectId, '        if ($fileInfo.Length -gt 0) {')
, (@ObjectId, '            if (Test-Path $csvFile) {')
, (@ObjectId, '                try {')
, (@ObjectId, '                    Remove-Item -Path $csvFile -Force')
--, (@ObjectId, '                    Write-Host "[$(Get-Date -Format HH:mm:ss)] Removed original file: $csvFile"')
, (@ObjectId, '                } catch {')
, (@ObjectId, '                    Write-Warning "[$(Get-Date -Format HH:mm:ss)] Could not remove $csvFile : $($_.Exception.Message)"')
, (@ObjectId, '                    continue')
, (@ObjectId, '                }')
, (@ObjectId, '            }')
, (@ObjectId, '            try {')
, (@ObjectId, '                Rename-Item -Path $csvFileWHdr -NewName ($table.TableName + ".csv") -Force')
--, (@ObjectId, '                Write-Host "[$(Get-Date -Format HH:mm:ss)] [SUCCESS] Renamed to: $csvFile"')
, (@ObjectId, '            } catch {')
, (@ObjectId, '                Write-Warning "[$(Get-Date -Format HH:mm:ss)] Failed renaming $csvFileWHdr : $($_.Exception.Message)"')
, (@ObjectId, '            }')
, (@ObjectId, '        } else {')
, (@ObjectId, '            Write-Warning "[$(Get-Date -Format HH:mm:ss)] New file $csvFileWHdr looks empty. Skipping cleanup and rename."')
, (@ObjectId, '        }')
, (@ObjectId, '    } else {')
, (@ObjectId, '        Write-Warning "[$(Get-Date -Format HH:mm:ss)] Missing $csvFileWHdr. Cannot remove or rename original file."')
, (@ObjectId, '    }')
, (@ObjectId, '}')
END

IF (@ImportTarget = 'MSSQL')
BEGIN
    INSERT INTO [#BcpInPwrShlFooter] ([ObjectId], [LineOfCode])
    VALUES
       (@ObjectId, CONCAT(@crlf, ' ################################################  Driver Section: ################################################ '))
    ,  (@ObjectId, '$totalJobs = $tables.Count')
    ,  (@ObjectId, '$jobs = @()')
    ,  (@ObjectId, 'Write-Host "`n ========================================== Starting Bcp-Import Jobs ================================================================ "')
    ,  (@ObjectId, 'for ($i = 0; $i -lt $totalJobs; $i++) {')
    ,  (@ObjectId, '    $table = $tables[$i]')
    ,  (@ObjectId, '    $job = Start-Job -ScriptBlock {')
    ,  (@ObjectId, CONCAT( '        param($table, $inputDir, $server, $targetDb', IIF(@SqlAuthentication = 1, ' , $username, $password)', ')')))
    ,  (@ObjectId, '')
    ,  (@ObjectId, '        function Execute-Sql {')
    ,  (@ObjectId, '            param (')
    ,  (@ObjectId, '                [string]$sql')
    ,  (@ObjectId, '               ,[string]$server')
    ,  (@ObjectId, '               ,[string]$database')
    ,  (@ObjectId, IIF(@SqlAuthentication = 1, '               ,[string]$username', ''))
    ,  (@ObjectId, IIF(@SqlAuthentication = 1, '               ,[string]$password', ''))
    ,  (@ObjectId, '            )')
    ,  (@ObjectId, CONCAT('            $connStr = "Server=$server;Database=$database;', IIF(@SqlAuthentication = 1, 'User ID=$username;Password=$password;', 'Integrated Security=SSPI;'), 'TrustServerCertificate=True"'))
    ,  (@ObjectId, '            $conn = New-Object System.Data.SqlClient.SqlConnection $connStr')
    ,  (@ObjectId, '            $cmd = $conn.CreateCommand()')
    ,  (@ObjectId, '            $cmd.CommandText = $sql')
    ,  (@ObjectId, '            try { $conn.Open(); $cmd.ExecuteNonQuery() | Out-Null; $true }')
    ,  (@ObjectId, '            catch { Write-Host "ERROR executing SQL: $($_.Exception.Message)"; $false }')
    ,  (@ObjectId, '            finally { $conn.Close() }')
    ,  (@ObjectId, '        }')
    ,  (@ObjectId, '')
    ,  (@ObjectId, '        function TryInt($value) {')
    ,  (@ObjectId, '            if ($value -match ''^[0-9,\.]+$'') { return [int]($value.Replace('','' , '''').Split(''.'')[0]) }')
    ,  (@ObjectId, '            return $null')
    ,  (@ObjectId, '        }')
    ,  (@ObjectId, '')
    ,  (@ObjectId, '        $startTime = Get-Date')
    ,  (@ObjectId, '        $formatFile = Join-Path $inputDir ("FormatFile." + $table["TableName"] + ".xml")')
    ,  (@ObjectId, '        $csvFile = Join-Path $inputDir ($table["TableName"] + ".csv")')
    ,  (@ObjectId, '        $targetView = "$targetDb." + $table["ViewName"]')
    ,  (@ObjectId, '        $viewSql = $table["ViewDefinition"]')
    ,  (@ObjectId, '        $viewCreated = Execute-Sql -sql $viewSql -server $server -database $targetDb -username $username -password $password')
    ,  (@ObjectId, '        if (-not $viewCreated) { return [pscustomobject]@{ Table = $table["TableName"]; RowsCopied = 0; Duration = "N/A"; Speed = "N/A"; StartTime = $startTime; EndTime = Get-Date; Error = "View creation failed for $targetView" } }')
    ,  (@ObjectId, CONCAT('        $args = @($targetView, "in", "`"$csvFile`"", "-S", $server, "-q", "-E", ', IIF(@SqlAuthentication = 1, '"-U", $username, "-P", $password,', '"-T",'), IIF(@ExportColumnHeaders = 1, ' "-F", "2",', ''),' "-r", "', @DelimBcpOutputRow, '", "-f", "`"$formatFile`"")'))
    ,  (@ObjectId, '        $output = & bcp.exe @args 2>&1 | Out-String')
    ,  (@ObjectId, '        $endTime = Get-Date')
    ,  (@ObjectId, '        $rowsCopied = if ($output -match ''([0-9,]+)\s+rows copied'') { $matches[1] } else { $null }')
    ,  (@ObjectId, '        $duration   = if ($output -match ''Clock Time.*?:\s*([0-9,]+)'') { $matches[1] } else { $null }')
    ,  (@ObjectId, '        $avgMatch   = [regex]::Match($output, ''Average\s*:\s*\(([0-9,\.]+)'' )')
    ,  (@ObjectId, '        $speed      = if ($avgMatch.Success) { $avgMatch.Groups[1].Value } else { $null }')
    ,  (@ObjectId, '        $rowsCopiedInt = TryInt $rowsCopied')
    ,  (@ObjectId, '        $durationInt   = TryInt $duration')
    ,  (@ObjectId, '        $speedInt      = TryInt $speed')
    ,  (@ObjectId, '        $rowsCopiedFormatted = if ($rowsCopiedInt -ne $null) { "{0:N0}" -f $rowsCopiedInt } else { "?" }')
    ,  (@ObjectId, '        $durationFormatted   = if ($durationInt -ne $null)   { "{0:N0}" -f $durationInt } else { "?" }')
    ,  (@ObjectId, '        $speedFormatted      = if ($speedInt -ne $null)      { "{0:N0}" -f $speedInt } else { "?" }')
    ,  (@ObjectId, '        $errorLines = $output -split "`r?`n" | Where-Object { ($_ -match "(?i)\b(error|failed)\b") -and ($_ -notmatch "BCP import with a format file will convert empty strings in delimited columns to NULL") }')
    ,  (@ObjectId, '        $errorText = if ($errorLines) { $errorLines -join "`n" } else { "None" }')
    ,  (@ObjectId, '        if ($viewCreated -and -not $errorLines) {')
    ,  (@ObjectId, '            $dropSql = "DROP VIEW [$($table["ViewName"].Split(''.'')[-2])].[$($table["ViewName"].Split(''.'')[-1])]"')
    ,  (@ObjectId, '            $dropSuccess = Execute-Sql -sql $dropSql -server $server -database $targetDb -username $username -password $password')
    ,  (@ObjectId, '            if (-not $dropSuccess) { $errorText += "`nWarning: Failed to drop view $($table["ViewName"])" }')
    ,  (@ObjectId, '        }')
    ,  (@ObjectId, '        [pscustomobject]@{ Table = $table.TableName; StartTime = $startTime; EndTime = $endTime; Duration = $durationFormatted; RowsCopied = $rowsCopiedFormatted; Speed = $speedFormatted; Error = $errorText }')
    ,  (@ObjectId, '    } -ArgumentList $table, $inputDir, $server, $targetDb, $username, $password')
    ,  (@ObjectId, '    $jobs += $job')
    ,  (@ObjectId, '    Write-Progress -Activity "Starting Bcp-Import Jobs" -Status "$($i + 1) of $totalJobs started." -PercentComplete (($i + 1) / $totalJobs * 100)')
    ,  (@ObjectId, '}')
    ,  (@ObjectId, 'Write-Host "`n ========================================== Waiting for Bcp-Import Jobs to Complete ================================================= "')
    ,  (@ObjectId, '$results = @()')
    ,  (@ObjectId, '$completed = 0')
    ,  (@ObjectId, 'while ($completed -lt $totalJobs) {')
    ,  (@ObjectId, '    foreach ($job in $jobs) {')
    ,  (@ObjectId, '        if ($job.State -eq "Running") { continue }')
    ,  (@ObjectId, '        if (-not $job.HasMoreData) { continue }')
    ,  (@ObjectId, '        $results += Receive-Job -Job $job')
    ,  (@ObjectId, '        Remove-Job -Job $job')
    ,  (@ObjectId, '        $completed++')
    ,  (@ObjectId, '        Write-Progress -Activity "Waiting for Bcp-Import Job Results" -Status "$completed of $totalJobs completed" -PercentComplete (($completed / $totalJobs) * 100)')
    ,  (@ObjectId, '    }')
    ,  (@ObjectId, '    Start-Sleep -Milliseconds 250')
    ,  (@ObjectId, '}')
    ,  (@ObjectId, 'Write-Host "`n ========================================== Bcp-Import Job Results: ================================================================= "')
    ,  (@ObjectId, '$results | Sort-Object Table | Select-Object Table, StartTime, EndTime, @{Name = "Duration [ms]"; Expression = { $_.Duration }}, RowsCopied, @{Name = "Speed [rows/s]"; Expression = { $_.Speed }}, Error | Format-Table -AutoSize -Wrap');

    SELECT @PwrShlBcpInFooter = STRING_AGG([LineOfCode], @crlf) FROM [#BcpInPwrShlFooter];
END

SELECT @PwrShlBcpOutFooter = STRING_AGG([LineOfCode], @crlf) FROM [#BcpOutPwrShlFooter];

SELECT @SelectedTableId = MIN([Id]), @SelectedTableIdMax = MAX([Id]) FROM [#SelectedTables] WHERE [IsToBeExported] = 1;
WHILE (@SelectedTableId <= @SelectedTableIdMax)
BEGIN

    TRUNCATE TABLE [#BcpOutPwrShlBody]
    IF (@ImportTarget = 'MSSQL')
    BEGIN
        TRUNCATE TABLE [#BcpInPwrShlBody];
    END
    ELSE IF (@ImportTarget = 'SNOWFLAKE')
    BEGIN
        TRUNCATE TABLE [#SnowflakeInSql];
    END

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


    IF (@ImportTarget = 'MSSQL')
    BEGIN
        INSERT INTO [#BcpInPwrShlBody] ([ObjectId], [LineOfCode])
        VALUES            
               (@ObjectId, CONCAT(' ################################################ Parameters for: ', QUOTENAME(@SchemaName), '.', QUOTENAME(@TableName), ' ################################################ '))
             , (@ObjectId, '@{') 
             , (@ObjectId, CONCAT('TableName = "', @SchemaName, '.', @TableName, '"'))
             , (@ObjectId, CONCAT('CanBcpInDirect = ', IIF(@CanBcpInDirect = 1, '$true', '$false')))
             , (@ObjectId, CONCAT('ViewName = "', (@SchemaName), '.', CONCAT(@TableName, '_BcpIn"')))
             , (@ObjectId, CONCAT('ViewDefinition = "CREATE OR ALTER VIEW ', QUOTENAME(@SchemaName), '.', QUOTENAME(CONCAT(@TableName, '_BcpIn')), ' AS SELECT '));    
    END

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

    
    IF (@ImportTarget = 'MSSQL')
    BEGIN
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
    END

    INSERT INTO [#BcpOutPwrShlBody] ([ObjectId], [LineOfCode])
    VALUES (@ObjectId, CONCAT(' FROM ', QUOTENAME(@DbNameSrc), '.', QUOTENAME(@SchemaName), '.', QUOTENAME(@TableName), '"'))

    IF (@ImportTarget = 'MSSQL')
    BEGIN
        INSERT INTO [#BcpInPwrShlBody] ([ObjectId], [LineOfCode])
        VALUES (@ObjectId, CONCAT(' FROM ', QUOTENAME(@DbNameTgt), '.', QUOTENAME(@SchemaName), '.', QUOTENAME(@TableName), '"'))
    END

    IF (@ExportColumnHeaders = 1)
    BEGIN
        INSERT INTO [#BcpOutPwrShlBody] ([ObjectId], [LineOfCode])
        SELECT @ObjectId
             , CONCAT('HeaderColumnNames = "', STRING_AGG([ColumnName], @DelimBcpOutputField), '"')
        FROM [#ColumnList]
        WHERE [ObjectId] = @ObjectId AND [IsToBeExported] = 1 
        
        IF (@ImportTarget = 'MSSQL')
        BEGIN
            INSERT INTO [#BcpInPwrShlBody] ([ObjectId], [LineOfCode])
            SELECT @ObjectId
                 , CONCAT('HeaderColumnNames = "', STRING_AGG([ColumnName], @DelimBcpOutputField), '"')
            FROM [#ColumnList]
            WHERE [ObjectId] = @ObjectId AND [IsToBeExported] = 1 
        END
    END
    
    INSERT INTO [#BcpOutPwrShlBody] ([ObjectId], [LineOfCode])
    VALUES (@ObjectId, IIF(@SelectedTableId < @SelectedTableIdMax AND @CreateSeparatePwrShlFiles = 0, '},', CONCAT('}', @crlf, ')')))

    IF (@ImportTarget = 'MSSQL')
    BEGIN
        INSERT INTO [#BcpInPwrShlBody] ([ObjectId], [LineOfCode])
        VALUES (@ObjectId, IIF(@SelectedTableId < @SelectedTableIdMax AND @CreateSeparatePwrShlFiles = 0, '},', CONCAT('}', @crlf, ')')))
    END

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
                   , IIF(LEAD([xfn].[Rn]) OVER (PARTITION BY NULL ORDER BY [xfn].[Rn]) IS NULL, CONCAT(@DelimBcpOutputRow, '\r\n'), @DelimBcpOutputField) /* if this is the last field place double quote after FieldDelimiter */
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
            SET @OutputFileNamePwrShl = CONCAT(@OutputDirectoryPsXml, @SnflSchemaUtil, '.', @SchemaName, '.', @TableName, '.ps1');
        
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
                UPDATE [#SelectedTables] SET [PathImportFile] = @OutputFileNamePwrShl WHERE [Id] = @SelectedTableId
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

            IF (@ImportTarget = 'MSSQL')
            BEGIN
                SELECT @PwrShlBcpInAll = CONCAT(@PwrShlBcpInAll, @crlf, STRING_AGG([LineOfCode], @crlf)) FROM [#BcpInPwrShlBody];
            END
            ELSE IF (@ImportTarget = 'SNOWFLAKE')
            BEGIN
                SELECT @SnowflakeSqlImport = CONCAT(@SnowflakeSqlImport, @crlf, STRING_AGG([LineOfCode], @crlf)) FROM [#SnowflakeInSql];
            END

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

                IF (@ImportTarget = 'MSSQL')
                BEGIN
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
                        UPDATE [#SelectedTables] SET [PathImportFile] = @OutputFileNamePwrShl WHERE [IsToBeExported] = 1
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
                ELSE IF (@ImportTarget = 'SNOWFLAKE')
                BEGIN
                    /* ======================================= SnowflakeSql Import: ======================================= */

                    /* Step 2: Variables for OLE Automation */
                    SET @OutputFileNameSnowSql = CONCAT(@OutputDirectoryPsXml, @SnflImpFileNam);
            
                    /* Step 3: Create FileSystemObject */
                    EXEC sp_OACreate 'Scripting.FileSystemObject', @ObjectToken OUTPUT;
            
                    /* Step 4: Create (or Overwrite) File */
                    EXEC sp_OAMethod @ObjectToken, 'CreateTextFile', @File OUTPUT, @OutputFileNameSnowSql, 2, True;  /* 2 = overwrite, True = Unicode */
                
                    /* Step 5: Write content to file */
                    EXEC sp_OAMethod @File, 'Write', NULL, @SnowflakeSqlImport;
            
                    /* Step 6: Close file */
                    EXEC sp_OAMethod @File, 'Close';
            
                    /* Step 7: Check if file exists */
                    EXEC sp_OAMethod @ObjectToken, 'FileExists', @FileExists OUTPUT, @OutputFileNameSnowSql;
 
                    IF @FileExists = 1
                    BEGIN 
                        UPDATE [#SelectedTables] SET [PathImportFile] = @OutputFileNameSnowSql WHERE [IsToBeExported] = 1
                        PRINT(CONCAT('Snowflake Sql Import File: ', @OutputFileNameSnowSql, ' created successfully'));
                    END
                    ELSE
                    BEGIN
                        SET @ErrorMessage = CONCAT('Failed to create the Snowflake Sql File: ', @OutputFileNameSnowSql);
                        GOTO ERROR; 
                    END   
                
                    /* Step 8: Clean up */
                    EXEC sp_OADestroy @File;
                    EXEC sp_OADestroy @ObjectToken;
                END
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
         , [PathImportFile]
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
