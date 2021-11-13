import sqlite3
import pandas as pd



class ETL:
    
    
    def __init__(self):
        pass
    
    
    def run(self):
        self.pedido() #delta id_pedido
        self.filial() #SCD tipo 2 com data
        self.cliente() #SCD tipo 2 com data
        self.produto() #SCD tipo 2 com data
    
    
    
    def sqlite_open_con(self,tipo):
        if tipo=='t':
            return sqlite3.connect('gerar_simulucao_db_transacional/vendas.db')
        
        elif tipo=='dw':
            return sqlite3.connect('gerar_simulacao_data_warehouse/data_warehouse.db')
        

    
    def pedido(self):
        
    
        conn_t = self.sqlite_open_con('t')
        conn_dw = self.sqlite_open_con('dw')
        
        
        
        cur = conn_dw.cursor()
        cur.execute("""with A as (select max(id_pedido) as max_id_pedido from pedido)
                        select
                        case 
                            when max_id_pedido is null then 0
                            else max_id_pedido
                            end as a
                        from A
                        """)
        
        max_id_pedido = cur.fetchall()[0][0]
        
        
        query = f"""
        select 
        	ped.id_pedido,
            item.id_produto,
        	REPLACE(REPLACE(ped.dt_pedido,'T',' '),'Z','') as dt_pedido,
        	ped.id_cliente,
        	ped.id_filial,
        	item.quantidade,
        	item.vr_unitario,
        	par.nm_parceiro
        from pedido as ped
        left join item_pedido as item
        on  item.id_pedido = ped.id_pedido
        left join parceiro as par
        on par.id_parceiro = ped.id_parceiro
        where ped.id_pedido>{max_id_pedido}
        """
        
        
        pedido = pd.read_sql(query, conn_t)
        pedido.to_sql('pedido', con=conn_dw, if_exists='append', index=False)
        
        conn_dw.commit()
        conn_dw.close()
        
        conn_t.commit()
        conn_t.close()
        
        
    def filial(self):
        
        conn_t = self.sqlite_open_con('t')
        conn_dw = self.sqlite_open_con('dw')
        
        

        
        
        import pandas as pd
        
        query = """
            select 
            fil.id_filial,
            fil.ds_filial,
            cid.ds_cidade,
            est.ds_estado,
            CURRENT_DATE  as dt_carga
            from filial as fil
            left join cidade as cid
            on  fil.id_cidade = cid.id_cidade
            left join estado as est
            on cid.id_estado = est.id_estado
    
        """
        
        
        pedido = pd.read_sql(query, conn_t)
        pedido.to_sql('filial_temp', con=conn_dw, if_exists='replace', index=False)
        
        cur= conn_dw.cursor()
        
        
        cur.execute("""
                    INSERT INTO filial (id_filial, ds_filial, ds_cidade, ds_estado, dt_carga)
                    SELECT 
                    tmp.id_filial,
                    tmp.ds_filial,
                    tmp.ds_cidade,
                    tmp.ds_estado,
                    case 
                        when ori.dt_carga is null then '1900-01-01'  --primeiro registro no datalake
                        else tmp.dt_carga
                        end as dt_carga
                    FROM filial_temp AS tmp
                    LEFT JOIN filial AS ori
                    ON tmp.id_filial=ori.id_filial
                    AND tmp.ds_filial=ori.ds_filial
                    AND tmp.ds_cidade=ori.ds_cidade
                    AND tmp.ds_estado=ori.ds_estado
                    WHERE ori.id_filial is null
        """)
        
        cur.execute('drop table if exists filial_temp')
                    
        
        conn_dw.commit()
        conn_dw.close()
        
        conn_t.commit()
        conn_t.close()
        
        
        
    def cliente(self):
        
        conn_t = self.sqlite_open_con('t')
        conn_dw = self.sqlite_open_con('dw')
        
       
        
        import pandas as pd
        
        query = """
            select 
            id_cliente,
            nm_cliente,
            flag_ouro,
            CURRENT_DATE  as dt_carga
            from cliente
    
        """
        
        
        pedido = pd.read_sql(query, conn_t)
        pedido.to_sql('cliente_temp', con=conn_dw, if_exists='replace', index=False)
        
        cur= conn_dw.cursor()
        
        
        cur.execute("""
                    INSERT INTO cliente (id_cliente, nm_cliente, flag_ouro, dt_carga)
                    SELECT 
                    tmp.id_cliente,
                    tmp.nm_cliente,
                    tmp.flag_ouro,
                    case 
                        when ori.dt_carga is null then '1900-01-01'  --primeiro registro no datalake
                        else tmp.dt_carga
                        end as dt_carga
                    FROM cliente_temp AS tmp
                    LEFT JOIN cliente AS ori
                    ON tmp.id_cliente=ori.id_cliente
                    AND tmp.nm_cliente=ori.nm_cliente
                    AND tmp.flag_ouro=ori.flag_ouro
                    WHERE ori.id_cliente is null
        """)
        
        cur.execute('drop table if exists cliente_temp')
        
        conn_dw.commit()
        conn_dw.close()
        
        conn_t.commit()
        conn_t.close()
        
        
        
    def produto(self):
        
        conn_t = self.sqlite_open_con('t')
        conn_dw = self.sqlite_open_con('dw')
        
       
        
        import pandas as pd
        
        query = """
            SELECT
            prod.id_produto,
            prod.ds_produto,
            s_car.ds_subcategoria,
            car.ds_categoria,
            car.perc_parceiro,
            CURRENT_DATE  as dt_carga
            from produto as prod
            left join sub_categoria as s_car
            on  s_car.id_subcategoria = prod.id_subcategoria
            left join categoria as car
            on car.id_categoria = s_car.id_categoria
    
        """
        
        
        pedido = pd.read_sql(query, conn_t)
        pedido.to_sql('produto_temp', con=conn_dw, if_exists='replace', index=False)
        
        cur= conn_dw.cursor()
        
        
        cur.execute("""
                    INSERT INTO produto (id_produto, ds_produto, perc_parceiro, ds_subcategoria, ds_categoria, dt_carga)
                    SELECT 
                    tmp.id_produto,
                    tmp.ds_produto,
                    tmp.perc_parceiro,
                    tmp.ds_subcategoria,
                    tmp.ds_categoria,
                    case 
                        when ori.dt_carga is null then '1900-01-01'  --primeiro registro no datalake
                        else tmp.dt_carga
                        end as dt_carga
                    FROM produto_temp AS tmp
                    LEFT JOIN produto AS ori
                    ON tmp.id_produto=ori.id_produto
                    AND tmp.ds_produto=ori.ds_produto
                    AND tmp.perc_parceiro=ori.perc_parceiro
                    AND tmp.ds_subcategoria=ori.ds_subcategoria
                    AND tmp.ds_categoria=ori.ds_categoria
                    WHERE ori.id_produto is null
        """)
        
        cur.execute('drop table if exists produto_temp')
        
        conn_dw.commit()
        conn_dw.close()
        
        conn_t.commit()
        conn_t.close()
        
        
        
        
        
        