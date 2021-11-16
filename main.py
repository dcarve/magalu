
import analise
from etl import ETL
import datetime as dt
from gerar_simulacao_data_warehouse import DataWarehouse
from gerar_simulucao_db_transacional import DbTransacional




#gerar simulacao db transacional
DbTransacional().run()



#gerar simulacao data_warehouse
DataWarehouse().run()

# run etl transfere os dados para data warehouse
ETL().run()
 
#analise
#período analisado pelo analista
data_inicio=dt.date(2021,6,1).strftime('%Y-%m-%d')
data_fim=(dt.date(2021,10,1)-dt.timedelta(days=1)).strftime('%Y-%m-%d')

df = analise.run(data_inicio, data_fim)
