import sqlite3
import pandas as pd


class ETL:
    
    
    def __init__(self):
        pass
    
    
    def run(self):
        self.filial() #SCD tipo 2 com data
        self.cliente() #SCD tipo 2 com data
        self.produto() #SCD tipo 2 com data
        self.pedido() #delta id_pedido
    
    
    
    def sqlite_open_con(self,tipo):
        if tipo=='t':
            return sqlite3.connect('gerar_simulucao_db_transacional/vendas.db')
        
        elif tipo=='dw':
            return sqlite3.connect('gerar_simulacao_data_warehouse/data_warehouse.db')
        

    
        
        
    def filial(self):
        
        conn_t = self.sqlite_open_con('t')
        conn_dw = self.sqlite_open_con('dw')
        

        
        query = """
            select 
            fil.id_filial,
            fil.ds_filial,
            cid.ds_cidade,
            est.ds_estado
            from filial as fil
            left join cidade as cid
            on  fil.id_cidade = cid.id_cidade
            left join estado as est
            on cid.id_estado = est.id_estado
                
        """
        
        cur= conn_dw.cursor()
        cur.execute('DELETE FROM filial_temp')
        conn_dw.commit()
        
        df = pd.read_sql(query, conn_t)
        df.to_sql('filial_temp', con=conn_dw, if_exists='append', index=False)
        

        
        
        cur.execute("""
                    with A as (
                    SELECT 
                    tmp.id_filial,
                    tmp.ds_filial,
                    tmp.ds_cidade,
                    tmp.ds_estado,
                    case 
                    	when ori.dt_carga is not null then CURRENT_DATE
                    	else '1900-01-01'
                    	end as dt_carga,
                    case 
                    	when ((ori.ds_cidade <> tmp.ds_cidade) or 
                    		  (ori.ds_estado <> tmp.ds_estado) or 
                    		  (ori.ds_filial <> tmp.ds_filial)) then 1
                    	else NULL
                    	end as change
                    
                    FROM filial_temp AS tmp
                    LEFT JOIN filial AS ori
                    ON tmp.id_filial=ori.id_filial
                    WHERE ori.id_filial is null or change = 1
                    )
                    
                    INSERT INTO filial (id_filial, ds_filial, ds_cidade, ds_estado, dt_carga)
                    SELECT 
                    A.id_filial,
                    A.ds_filial,
                    A.ds_cidade,
                    A.ds_estado,
                    A.dt_carga
                    from A
        """)
        
        cur.execute('DELETE FROM filial_temp')
                    
        
        conn_dw.commit()
        conn_dw.close()
        
        conn_t.commit()
        conn_t.close()
        
        
        
    def cliente(self):
        
        conn_t = self.sqlite_open_con('t')
        conn_dw = self.sqlite_open_con('dw')
        

        
        query = """
            select 
            id_cliente,
            nm_cliente,
            flag_ouro
            from cliente
    
        """
        
        cur= conn_dw.cursor()
        cur.execute('DELETE FROM cliente_temp')
        conn_dw.commit()
        
        
        df = pd.read_sql(query, conn_t)
        df.to_sql('cliente_temp', con=conn_dw, if_exists='replace', index=False)
        

        
        cur.execute("""
                with A as (
                SELECT 
                tmp.id_cliente,
                tmp.nm_cliente,
                tmp.flag_ouro,
                case 
                    when ori.dt_carga is not null then CURRENT_DATE
                    else '1900-01-01'
                    end as dt_carga,
                case 
                    when ((ori.nm_cliente <> tmp.nm_cliente) or 
                            (ori.flag_ouro <> tmp.flag_ouro)) then 1
                    else NULL
                    end as change
                FROM cliente_temp AS tmp
                LEFT JOIN cliente AS ori
                ON tmp.id_cliente=ori.id_cliente
                WHERE ori.id_cliente is null or change = 1
                )
                
                INSERT INTO cliente (id_cliente, nm_cliente, flag_ouro, dt_carga)
                SELECT 
                A.id_cliente,
                A.nm_cliente,
                A.flag_ouro,
                A.dt_carga
                from A
        """)
        
        cur.execute('DELETE FROM cliente_temp')
        
        conn_dw.commit()
        conn_dw.close()
        
        conn_t.commit()
        conn_t.close()
        
        
        
    def produto(self):
        
        conn_t = self.sqlite_open_con('t')
        conn_dw = self.sqlite_open_con('dw')
        

        
        query = """
            SELECT
            prod.id_produto,
            prod.ds_produto,
            s_car.ds_subcategoria,
            car.ds_categoria,
            car.perc_parceiro
            from produto as prod
            left join sub_categoria as s_car
            on  s_car.id_subcategoria = prod.id_subcategoria
            left join categoria as car
            on car.id_categoria = s_car.id_categoria
    
        """
        
        cur= conn_dw.cursor()
        cur.execute('DELETE FROM produto_temp')
        conn_dw.commit()
        
        
        df = pd.read_sql(query, conn_t)
        df.to_sql('produto_temp', con=conn_dw, if_exists='replace', index=False)

        
        
        cur.execute("""
            with A as (
                SELECT 
                tmp.id_produto,
                tmp.ds_produto,
                tmp.perc_parceiro,
                tmp.ds_subcategoria,
                tmp.ds_categoria,
                case 
                    when ori.dt_carga is not null then CURRENT_DATE
                    else '1900-01-01'
                    end as dt_carga,
                case 
                    when ((ori.ds_produto <> tmp.ds_produto) or 
                            (ori.perc_parceiro <> tmp.perc_parceiro) or 
                            (ori.ds_subcategoria <> tmp.ds_subcategoria) or 
                            (ori.ds_categoria <> tmp.ds_categoria)) then 1
                    else NULL
                    end as change
            
                FROM produto_temp AS tmp
                LEFT JOIN produto AS ori
                ON tmp.id_produto=ori.id_produto
                WHERE ori.id_produto is null  or change = 1
            )
            
            INSERT INTO produto (id_produto, ds_produto, perc_parceiro, ds_subcategoria, ds_categoria, dt_carga)
            SELECT 
            A.id_produto,
            A.ds_produto,
            A.perc_parceiro,
            A.ds_subcategoria,
            A.ds_categoria,
            A.dt_carga
            from A
        """)
        
        cur.execute('DELETE FROM produto_temp')
        
        conn_dw.commit()
        conn_dw.close()
        
        conn_t.commit()
        conn_t.close()
        
        

    
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
        

        cur.execute('DELETE FROM pedido_temp')
        conn_dw.commit()
        
        
        pedido = pd.read_sql(query, conn_t)
        pedido.to_sql('pedido_temp', con=conn_dw, if_exists='replace', index=False)
        
        
        cur.execute("""
            with A as (select 
                tmp.id_pedido,
                prod.id_dw_produto,
                tmp.dt_pedido,
                cli.id_dw_cliente,
                fi.id_dw_filial,
                tmp.quantidade,
                tmp.vr_unitario,
                tmp.nm_parceiro
            from pedido_temp as tmp
            left join cliente as cli
            on cli.id_cliente = tmp.id_cliente
            left join produto as prod
            on tmp.id_produto = prod.id_produto
            left join filial as fi
            on tmp.id_filial = fi.id_filial
            left join pedido as ori
            on  ori.id_pedido= tmp.id_pedido
            where ori.id_pedido is null
            
            )
            
            INSERT INTO pedido (id_pedido, id_dw_produto, dt_pedido, id_dw_cliente, id_dw_filial, quantidade, vr_unitario, nm_parceiro)
            
            
            select 
                id_pedido,
                id_dw_produto,
                dt_pedido,
                id_dw_cliente,
                id_dw_filial,
                quantidade,
                vr_unitario,
                nm_parceiro
            from A
        """)
        
        cur.execute('DELETE FROM pedido_temp')
        
        
        conn_dw.commit()
        conn_dw.close()
        
        conn_t.commit()
        conn_t.close()
        
        

        
        