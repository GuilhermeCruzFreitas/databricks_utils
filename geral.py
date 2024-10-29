#parametros programaticamente
run_parameters = dbutils.notebook.entry_point.getCurrentBindings()

#dados como run id, user que rodou o job, etc
context = json.loads(dbutils.notebook.entry_point.getDbutils().notebook().getContext().safeToJson()) 

