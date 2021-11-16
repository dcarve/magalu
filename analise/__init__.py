
import pandas as pd
import sqlite3
from  dateutil.relativedelta import relativedelta
import datetime as dt


def run(data_inicio,  data_fim ):
    
    query = f"""
    with comissao as (select 
    	ped.vr_unitario,
    	substr(ped.dt_pedido,1,7) as ano_mes,
    	ped.nm_parceiro,
    	prod.perc_parceiro * ped.vr_unitario / 100 as comissao
    from pedido as ped
    left join produto as prod
    on ped.id_dw_produto = prod.id_dw_produto
    	where ped.dt_pedido between '{data_inicio}' and '{data_fim}'
    ),
    agrupamento as (
    
    select 
    	round(sum(vr_unitario),2)  as total_venda,
    	count(*) as quant_itens_vendidos,
    	ano_mes,
    	nm_parceiro,
    	sum(comissao) as comissao
    from comissao
    group by 
    	ano_mes,
    	nm_parceiro
    )
    
    select 
    	total_venda,
    	quant_itens_vendidos,
    	case 
    		when comissao <=100.00 then 0.0
    		else round(comissao - cast(total_venda/10000 as integer) * 100, 2) 
    		end as comissao,
    	
    	ano_mes || '-01' as ano_mes,
    	nm_parceiro
    
    from agrupamento
     
    
    
    """
    
    conn = sqlite3.connect('gerar_simulacao_data_warehouse/data_warehouse.db')
    df = pd.read_sql(query, con=conn)
    

    
    df['ano_mes'] = pd.to_datetime(df['ano_mes']).dt.date
    
    df['ano_mes'] = df.apply(lambda row:   row['ano_mes'] + relativedelta(months=1) - dt.timedelta(days=1), axis=1)
    
    df.to_csv('relatorio_final.csv', index=False, sep=';', decimal=',')
    
    return df
