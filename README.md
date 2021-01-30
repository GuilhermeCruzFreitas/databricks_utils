# databricks_utils
Repositório destinado a funções que facilitam a vida no ambiente databricks

## fnc_existencia_arquivo
Verifica a existencia de um arquivo específico em determinado path do adlsGen2

## fnc_pandas_para_spark
Faz as conversões necessárias para a importação de notebooks que utilizam pandas para ambiente spark

## fnc_pegar_status_pipeline_df
Confere o status de um pipeline do data factory para sincronizar a execução de pipelines

## fnc_retorna_path_containers
Em casos que determinados usuário deverão usar cluster diferentes dos utilizados para a execução de jobs no data factory, essa função é útil para manter a camada de autorização destes clusters separadas

## ods_delete_changed_keys
Função que detecta através de bitmask quando uma chave primária "virtual" é modificada (carga de dados via attunity)

