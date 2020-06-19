/*
Batch-scores an arbitrary list of lightgbm json_models with a specific structure, i.e. must contain lightgbm string in mdl_string. This allows the client to escalate this scoring procedure to any number of models and control memory limits with @batch_size.
*/

CREATE OR ALTER PROCEDURE prc_mdl_score (@fec_datos AS DATE
	,@batch_size AS INT
	,@json_modelos AS NVARCHAR(MAX)
	,@comentarios AS NVARCHAR(MAX)
) AS
BEGIN

-- scoring python script
DECLARE @script AS NVARCHAR(MAX) =
	N'

# ---------------------------------------------------
# librerias
# ---------------------------------------------------
import pickle
import pandas as pd
from pandas.io.json import json_normalize
import numpy as np
import datetime
import lightgbm as lgb
from dateutil.relativedelta import relativedelta

# -----------------------------------------
# df_modelos y scoring
# -----------------------------------------
# armo la df contenedora de modelos
df_modelos = pd.read_json(json_modelos)

# en el caso de que se haya pasado mas de un modelo por target, me quedo con el ultimo
df_modelos = df_modelos.sort_values(["mdl_target","mdl_timestamp"], ascending = False).drop_duplicates(subset = "mdl_target").reset_index(drop = True)

# scoring modelo por modelo con string lightgbm
OutputDataSet = pd.DataFrame()
for mod_row in df_modelos.iterrows():
	mod_index = mod_row[0]
	# calculo de ventana y dep_fec_target
	fec_datos = datetime.datetime.strptime(df_modelos["mdl_fec_datos"][mod_index], "%Y-%m-%d")
	fec_target = datetime.datetime.strptime(df_modelos["mdl_fec_target"][mod_index], "%Y-%m-%d")
	ventana = ((fec_target.year - fec_datos.year) * 12) + fec_target.month - fec_datos.month
	dep_fec_datos = datetime.datetime.strptime("' + CAST(@fec_datos AS NVARCHAR) + '", "%Y-%m-%d") # esto se llama por variable de la proc porque quiz�s en alg�n momento se quiera scorear un modelo de otra fecha
	dep_fec_target = dep_fec_datos + relativedelta(months = + ventana)

	# scoring
	lgb_model = lgb.Booster(model_str = df_modelos["mdl_string"][mod_index])
	variables = lgb_model.feature_name()
	OutputDataSet = OutputDataSet.append(pd.DataFrame(data = {
		"id"                 : df["id"],
		"fec_datos"          : dep_fec_datos,
		"fec_target"         : dep_fec_target,
		"sco_timestamp"      : datetime.datetime.now(),
		"sco_mdl_nombre"     : df_modelos["mdl_nombre"][mod_index],
		"sco_mdl_timestamp"  : df_modelos["mdl_timestamp"][mod_index],
		"sco_mdl_fec_target" : df_modelos["mdl_fec_target"][mod_index],
		"sco_target"         : df_modelos["mdl_target"][mod_index],
		"sco_label"          : "1", # se puede usar para multiclass, en binario es siempre positivo en lightgbm
		"sco_score"          : lgb_model.predict(df[variables].values),
		"sco_comentarios"    : "' + @comentarios + '",
	}))

	'
-- determinar cantidad de batches en funci�n de @batch_size establecido y @row_count de la tabla maestra cargar
DECLARE @row_count AS INT = (SELECT COUNT(*) FROM clus_abt WHERE per_fec_carga = EOMONTH(@fec_datos))
DECLARE @batch_count AS INT
IF @row_count % @batch_size = 0 -- si es divisible
	SET @batch_count = @row_count / @batch_size     -- si es divisible (muy raro que pase), division en integer
ELSE
	SET @batch_count = @row_count / @batch_size + 1 -- si no es divisible, sumar +1 batch para agarrar al resto

-- declarar variables para batch
DECLARE @input_data_1 AS NVARCHAR(MAX)
DECLARE @batch INT = 1
DECLARE @batch_ini INT = 1
DECLARE @batch_fin INT

-- inicio batch scoring
WHILE (@batch <= @batch_count) 
BEGIN
	-- prep current batch
	SET @batch_fin = @batch_ini + @batch_size
	SET @input_data_1 = N'
        -- here goes your input data query
        -- it must work with @batch_ini and @batch_fin to select batch rows
        -- the final build solution does this step with dynamic sql to be able to add an arbitrary number of input tables without the need to declare every column
        '
	INSERT INTO mdl_score
	EXECUTE sp_execute_external_script @language = N'Python'
		, @script = @script
		, @input_data_1 = @input_data_1
		, @input_data_1_name = N'df'
		, @params = N'@json_modelos NVARCHAR(MAX)'
		, @json_modelos = @json_modelos
	
	-- prep para el pr�ximo batch
	SET @batch_ini = @batch_fin
	SET @batch = @batch + 1
END
END