

import sqlite3

class DataWarehouse:
    
    def __init__(self):
        pass
    
    def run(self):
    
        create_query = [query.strip() for query in open('gerar_simulacao_data_warehouse/create_table.sql', 'r').read().split(';')]
        delete_query = [query.strip() for query in open('gerar_simulacao_data_warehouse/delete_table.sql', 'r').read().split(';')]
        
        
        conn = sqlite3.connect('gerar_simulacao_data_warehouse/data_warehouse.db')
        cur = conn.cursor()
        
        for query in delete_query:
            cur.execute(query)
            
        for query in create_query:
            cur.execute(query)
            
        conn.commit()
        conn.close()
        
        
        
