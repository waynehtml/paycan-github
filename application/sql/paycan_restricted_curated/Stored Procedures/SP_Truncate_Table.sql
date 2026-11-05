CREATE   procedure [paycan_restricted_curated].[SP_Truncate_Table]  
(  
 @table_name varchar(250)  
)  
as  
begin  
  
declare @sqlScript NVARCHAR(500)  
  
 set @sqlScript = N'TRUNCATE TABLE ' + @table_name  
 EXECUTE sp_executesql @sqlScript  
  
end