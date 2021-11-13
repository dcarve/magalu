
import pandas as pd
import sqlite3


def run(data_inicio,  data_fim ):
    
    query = f"""
    with A as (select 
    		ped.vr_unitario,
    		ped.dt_pedido,
    		ped.nm_parceiro,
    		prod.perc_parceiro,
    		prod.dt_carga,
    		ped.id_produto,
    		ped.id_pedido
    	from pedido as ped
    	left join produto as prod
    	on ped.id_produto = prod.id_produto
    	where substr(ped.dt_pedido,1,10) >= prod.dt_carga
        and ped.dt_pedido between '{data_inicio}' and '{data_fim}'
    
    ),
    pedido_produto as (
    	select 
    		A.vr_unitario, 
    		A.dt_pedido as data_completa,
    		substr(A.dt_pedido,1,7) as ano_mes,
    		A.nm_parceiro,
    		A.perc_parceiro * A.vr_unitario / 100 as comissao
    	from (select id_pedido, id_produto, max(dt_carga) as dt_carga from A group by id_produto, id_pedido) as B
    	left join A
    	on  A.id_produto=B.id_produto and A.id_pedido=B.id_pedido and A.dt_carga = B.dt_carga
    ),
    agrupamento as (
    	select 
    		round(sum(vr_unitario),2)  as total_venda,
            count(*) as quant_itens_vendidos,
    		ano_mes,
    		nm_parceiro,
    		sum(comissao) as comissao
    	from pedido_produto
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
    
    

    
    df.to_csv('relatorio_final.csv', index=False, sep=';', decimal=',')
    
    return df