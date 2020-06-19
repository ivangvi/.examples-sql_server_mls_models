/*
Automated time-series aggregation by specifying in a table with 1/0 what kind of aggregations to extract from each of the columns, while also specifying an alternative data source (norm) when applicable. 
The script then generates the query with dynamically, without the need to manually specify every single column. This allows for the code to be easy to modify or reuse with any other data source.
*/

CREATE OR ALTER PROCEDURE prc_abt_clientes_xm(@fec_datos date
	,@meses_historia int
	
) AS BEGIN

-- declarar agregaciones a hacer, variable por variable, y tambi�n finalmente declarar si requiere normalizaci�n o no
insert into #agregaciones (avg,sum,min,max,stdev,ran,variable,norm) values
	 (1,0,0,0,0,0,'column_x',0)
	,(1,0,0,0,0,0,'column_x',0)
	,(1,1,1,1,1,1,'column_x',0)
	,(1,1,1,1,0,1,'column_x',1)
	,(1,1,1,1,1,1,'column_x',1)
	,(1,0,0,0,0,0,'column_x',0)
	,(1,1,0,0,0,0,'column_x',1)
	,(1,1,0,0,0,0,'column_x',1)
	,(1,1,0,0,0,0,'column_x',0)
	,(1,0,0,0,0,0,'column_x',0)
	,(0,0,0,0,0,1,'column_x',1)
	,(0,0,0,0,0,1,'column_x',0)
	,(0,0,0,0,0,1,'column_x',0)
	,(1,1,1,1,1,1,'column_x',0)
	,(1,1,1,1,1,1,'column_x',1)
	,(1,1,0,0,0,1,'column_x',1)
	,(1,1,0,0,0,1,'column_x',1)
	,(1,1,0,0,0,1,'column_x',0)
	,(0,0,0,0,0,1,'column_x',0)
	,(1,1,1,1,1,1,'column_x',1)
	,(1,0,0,0,0,1,'column_x',0)
	,(1,0,0,0,0,1,'column_x',1)
	,(1,0,0,0,0,1,'column_x',0)
    -- scale as needed

declare @fec_historia date = eomonth(dateadd(month, - @meses_historia + 1, @fec_datos))

-- critical error no hay datos en el ultimo mes
if not exists (select id from abt where fec_datos = @fec_historia)
	begin
		print('CRITICAL ERROR! No hay datos en @fec_historia, abortando procedure.')
		return
	end
else
	print('Chequeo datos en @fec_historia OK..')

-- crear tabla de agregaciones
create table #agregaciones(variable nvarchar(max) 
	,avg nvarchar(max)
	,sum nvarchar(max)
	,min nvarchar(max)
	,max nvarchar(max)
	,stdev nvarchar(max)
	,ran nvarchar(max) 
	,norm smallint
	,nonz nvarchar(max) -- se relaciona con norm, es para pasar cu�ntas veces la variable tuvo valor distinto de 0, sirve para conservar info de var <= 1, que se pierde en la norm
)

-- hacer agregaciones: si la variable requiere normalizacion se busca la fuente normalizada, si no, la original
update #agregaciones set avg = case avg when 1 then case norm
		when 0 then concat('avg(abt.',variable,')      as ',variable,'_',cast(@meses_historia as varchar),'m_avg')
		else        concat('avg(abt_norm.',variable,') as ',variable,'_',cast(@meses_historia as varchar),'m_avg')
		end else null end
update #agregaciones set sum = case sum when 1 then case norm
		when 0 then concat('sum(abt.',variable,')      as ',variable,'_',cast(@meses_historia as varchar),'m_sum')
		else        concat('sum(abt_norm.',variable,') as ',variable,'_',cast(@meses_historia as varchar),'m_sum')
		end else null end
update #agregaciones set min = case min when 1 then case norm
		when 0 then concat('min(abt.',variable,')      as ',variable,'_',cast(@meses_historia as varchar),'m_min')
		else        concat('min(abt_norm.',variable,') as ',variable,'_',cast(@meses_historia as varchar),'m_min')
		end else null end
update #agregaciones set max = case max when 1 then case norm
		when 0 then concat('max(abt.',variable,')      as ',variable,'_',cast(@meses_historia as varchar),'m_max')
		else        concat('max(abt_norm.',variable,') as ',variable,'_',cast(@meses_historia as varchar),'m_max')
		end else null end
update #agregaciones set stdev = case stdev when 1 then case norm
		when 0 then concat('stdev(abt.',variable,')      as ',variable,'_',cast(@meses_historia as varchar),'m_stdev')
		else        concat('stdev(abt_norm.',variable,') as ',variable,'_',cast(@meses_historia as varchar),'m_stdev')
		end else null end
update #agregaciones set ran = case ran when 1 then case norm
		when 0 then concat('max(abt.',variable,') - ','min(abt.',variable,')           as ',variable,'_',cast(@meses_historia as varchar),'m_ran')
		else        concat('max(abt_norm.',variable,') - ','min(abt_norm.',variable,') as ',variable,'_',cast(@meses_historia as varchar),'m_ran')
		end else null end
update #agregaciones set nonz = case norm when 1 then concat('count(abt_norm.',variable,') as ',variable,'_',cast(@meses_historia as varchar),'m_nonz')		
		else null end

-- decisi�n INSERT/INTO encapsulsa a la ejecuci�n del sql dinamico
declare @sql_1 nvarchar(max) = N'select abt.id_cli_persona, max(abt.fec_datos) as fec_datos,' + char(13)
+ 'count(abt.id_cli_persona) as abt_count_' + cast(@meses_historia as varchar) +'m'
+ ',' + char(13)
+ (select string_agg(avg,', ' + char(13)) from #agregaciones)
+ ',' + char(13)
+ (select string_agg(sum,', ' + char(13)) from #agregaciones)
+ ',' + char(13)
+ (select string_agg(min,', ' + char(13)) from #agregaciones)
+ ',' + char(13)
+ (select string_agg(max,', ' + char(13)) from #agregaciones)
+ ',' + char(13)
+ (select string_agg(stdev,', ' + char(13)) from #agregaciones)
+ ',' + char(13)
+ (select string_agg(ran,', ' + char(13)) from #agregaciones)
+ ',' + char(13)
+ (select string_agg(nonz,', ' + char(13)) from #agregaciones)
+ char(13)
+ 'into #temp from abt join abt_norm on abt.id_cli_persona = abt_norm.id_cli_persona and abt.fec_datos = abt_norm.fec_datos'
+ char(13)
+ 'where (abt.fec_datos between eomonth(''' + cast(@fec_historia AS NVARCHAR) + ''') and eomonth(''' + cast(@fec_datos AS NVARCHAR) + '''))'
+ char(13)
+ 'and   (abt_norm.fec_datos between eomonth(''' + cast(@fec_historia AS NVARCHAR) + ''') and eomonth(''' + cast(@fec_datos AS NVARCHAR) + '''))'
+ char(13)
+ 'group by abt.id_cli_persona'
+ char(13) -- ahora viene insert/into a la tabla final, excluyendo a quienes  no figuren en el �ltimo mes
+ 'select * into abt' + cast(@meses_historia as varchar) + 'm from #temp where fec_datos = ''' + cast(@fec_datos AS NVARCHAR) + ''''
-- exec sql
exec sp_executesql @sql_1
END