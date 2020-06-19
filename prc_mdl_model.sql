/*
Allows building of binary models by passing on specified lightgbm parameters and an input data set. This belongs to a bigger project with a defined SQL Server architecture where the table gt_targets contains a list of customers and dated labels.
*/

CREATE OR ALTER PROCEDURE prc_mdl_modelo_flg_devo1 (@target NVARCHAR(32)
	,@fec_target DATE
	,@meses_eventos INT
	,@rows_t_max INT = 99999
	,@rows_f_t_rat INT = 3
	,@lgb_leaves INT
	,@lgb_min_data INT
	,@lgb_bag_frac REAL
	,@lgb_feat_frac REAL
	,@lgb_rate REAL
	,@comentarios NVARCHAR(MAX)
) AS
BEGIN

-- fec_target_ini: se define seg�n la cantidad de meses de eventos que mira el modelo en tgt_targets
DECLARE @fec_target_ini DATE = DATEADD(month, - @meses_eventos + 1, @fec_target)

-- variables para determinar muestras de t y f
DECLARE @rows_t_all INT
DECLARE @rows_f_all INT
DECLARE @rows_t_sam INT
DECLARE @rows_f_sam INT
DECLARE @rows_t_rat REAL 
DECLARE @rows_f_rat REAL 

-- buscar cuantos f y t tengo en total
SELECT @rows_t_all = COUNT(*) FROM tgt_targets
	WHERE fec_target BETWEEN EOMONTH(@fec_target_ini) AND EOMONTH(@fec_target)
		AND tgt_target = @target
		AND tgt_label = 1
SELECT @rows_f_all = COUNT(*) FROM tgt_targets
	WHERE fec_target BETWEEN EOMONTH(@fec_target_ini) AND EOMONTH(@fec_target)
		AND tgt_target = @target
		AND tgt_label = 0

-- setear muestra final
SET @rows_t_sam = CASE WHEN @rows_t_max < @rows_t_all THEN @rows_t_max ELSE @rows_t_all END -- si tengo m�s t que el @rows_t_max definido, ir por @rows_t_max, si no, los t
SET @rows_f_sam = CAST(ROUND(@rows_t_sam * @rows_f_t_rat,0) AS INT)
SET @rows_t_rat = CAST(@rows_t_sam AS REAL) / CAST(@rows_t_all AS REAL)
SET @rows_f_rat = CAST(@rows_f_sam AS REAL) / CAST(@rows_f_all AS REAL)

/* ----------------------------------------------------
-- input data

- Se hace din�micamente para poder agregar m�s fuentes ad hoc sin tener que andar declarando fuente por fuente.
- Se deja un 10% m�s de datos de forma holgada (* 1.1) porque el m�todo de selecci�n random no es preciso.

- Ideas futuras: pasar por un json en la ejecuci�n del modelo los targets con sus respectivas fuentes.
*/ ----------------------------------------------------

DECLARE @input_data_1 AS NVARCHAR(MAX) = '
        -- here goes your input data query
        -- it must work with rows_t and rows_f variables to define sampling
        -- the final build solution does this automatically with dynamic sql
        -- the final build solution also uses dynamic sql to add an arbitrary number of input tables without the need to declare every column
'
/* ----------------------------------------------------
-- py script
*/ ----------------------------------------------------
DECLARE @script AS NVARCHAR(MAX) =
	N'
# ---------------------------------------------------
# librerias
# ---------------------------------------------------
import pandas as pd
import numpy as np
import datetime
import lightgbm as lgb

# ---------------------------------------------------
# setear target
# ---------------------------------------------------
target_name = df["tgt_target"][0]
target = "tgt_label"
df[target] = df[target].astype(int)

# ---------------------------------------------------
# variables a remover
# ---------------------------------------------------
vars_remover = [
	"id",
	"fec_datos",
	"fec_target",
	"tgt_label",
	"tgt_target",
	"tgt_json",
]
variables = list(set(list(df.columns)) - set(vars_remover))        

# ---------------------------------------------------
# model cv
# ---------------------------------------------------

lgb_train = lgb.Dataset(df[variables].values, df[target].values, categorical_feature = categoricas_names, feature_name = list(df[variables].columns), free_raw_data = False)

# parametros para cv y train (menos rounds)
parametros = {
    "objective"       : "binary",
    "metric"          : "auc",
    "num_threads"     : 0, # seg�n lightgbm docs, especificar a n�mero de cores disponibles, si es posible
    "num_leaves"      : ' + CAST(@lgb_leaves AS NVARCHAR) + ',
    "min_data_in_leaf": ' + CAST(@lgb_min_data AS NVARCHAR) + ',
    "bagging_fraction": ' + CAST(@lgb_bag_frac AS NVARCHAR) + ',
	"bagging_freq"    : 1,
    "feature_fraction": ' + CAST(@lgb_feat_frac AS NVARCHAR)+ ',
    "learning_rate"   : ' + CAST(@lgb_rate AS NVARCHAR)+ ',
    "seed"            : 7,
	"verbosity"       : -1,
} 

# cv check perfo y rounds
folds = 5
lgb_cv = lgb.cv(
    parametros,
    lgb_train,
	nfold = folds,
    early_stopping_rounds = 30,
    num_boost_round = 5000,
    categorical_feature = categoricas_names,
    eval_train_metric = True,
	seed = 7,
)
cv_df = pd.DataFrame.from_dict(lgb_cv).sort_values("valid auc-mean", ascending = False).head(1) # me quedo con la mejor ronda
mod_cv_tr_gini = cv_df["train auc-mean"].iloc[0] * 2 - 1
mod_cv_te_gini = cv_df["valid auc-mean"].iloc[0] * 2 - 1
mod_cv_rounds =  cv_df.index[0] + 1

# ---------------------------------------------------
# entrenar
# ---------------------------------------------------

lgb_model = lgb.train(
    parametros,
    lgb_train,
    num_boost_round = mod_cv_rounds,
    categorical_feature = categoricas_names,
)

# ---------------------------------------------------
# cuantas variables usa el modelo
# ---------------------------------------------------

impo_df = pd.DataFrame(data = {"feature":lgb_model.feature_name(),"split":lgb_model.feature_importance(importance_type = "split")})
mdl_cols = len(impo_df.loc[impo_df["split"] > 0])

# ---------------------------------------------------
# guardar resultados
# ---------------------------------------------------

OutputDataSet = pd.DataFrame(data = {
	# columnas comunes a todos los modelos
	"mdl_target"     : target_name,
	"mdl_nombre"     : "' + @target + '_devo1' + '",
	"mdl_comentarios": "' + @comentarios + '",
	"mdl_fec_datos"  : str(df["fec_datos"].max()),
	"mdl_fec_target" : "' + CAST(@fec_target AS NVARCHAR) + '",
	"mdl_timestamp"  : datetime.datetime.now(),
	"mdl_t_rows"     : rows_t_sam,
	"mdl_f_rows"     : rows_f_sam,
	"mdl_rounds"     : mod_cv_rounds,
	"mdl_cols"       : mdl_cols,
	"mdl_cv_folds"   : folds,
	"mdl_cv_metrica" : "gini",
	"mdl_cv_train"   : mod_cv_tr_gini,
	"mdl_cv_test"    : mod_cv_te_gini,
	"mdl_string"     : lgb_model.model_to_string(),
}, index = [0])

	'
/* ----------------------------------------------------
-- exec e insert into
*/ ----------------------------------------------------
INSERT INTO mdl_modelos
EXECUTE sp_execute_external_script @language = N'Python'
    ,@script = @script
    ,@input_data_1 = @input_data_1
	,@input_data_1_name = N'df'
    ,@params = N'
			 @rows_t_sam INT
			,@rows_f_sam INT
			'
	,@rows_t_sam = @rows_t_sam
	,@rows_f_sam = @rows_f_sam
END