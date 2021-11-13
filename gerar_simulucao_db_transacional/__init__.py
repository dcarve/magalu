import sqlite3
import os
import glob
import pandas as pd
import numpy as np


class DbTransacional:
    
    
    def __init__(self):
        
        self.query_create = [query.strip() for query in open('gerar_simulucao_db_transacional/create_tables.sql','r').read().split(';')]
        self.query_delete = [query.strip() for query in open('gerar_simulucao_db_transacional/delete_tables.sql','r').read().split(';')]
        

    def run(self):
        
        self.open_conn_sqlite()
        self.create_tables()
        self.insert_data()
        self.close_conn_sqlite()



    def open_conn_sqlite(self):


        self.conn = sqlite3.connect('gerar_simulucao_db_transacional/vendas.db')
        

    def close_conn_sqlite(self):
        self.conn.close()
    
    def create_tables(self):
        
  
        cur = self.conn.cursor()
        
        
        for query in self.query_delete:
            if len(query)>0:
                cur.execute(query)
            
        for query in self.query_create:
            if len(query)>0:
                cur.execute(query)  
        
        
        self.conn.commit()
    
    
    def insert_data(self):
    
        
        
        files_data = {}
        for i in glob.glob('gerar_simulucao_db_transacional/data/*'):
            files_data[os.path.basename(i).replace('.csv','')] = i
            
        
        ordem_colunas = [i.replace('CREATE TABLE ','')[:i.replace('CREATE TABLE ','').find(' ')] for i in self.query_create]
        ordem_colunas.remove('')
        
        ordem_colunas = [files_data[i] for i in ordem_colunas]
        
        cur = self.conn.cursor()

        for file in ordem_colunas:
        
            
            name_table = os.path.basename(file).replace('.csv','')
            
            cols = [c.strip() for c in open(file,'r', encoding='ansi').readline().split('|')]
            schema = {}
            for col in cols:
                schema[col] = str
            
            
            data_df = pd.read_csv(file, sep='|', dtype=schema)
            
            
            query = """ PRAGMA table_info({table});""".format(table=name_table)
            
            
            describe = pd.read_sql(query, self.conn)[['name','type']]
            
            for col_describe in describe['name'].to_list():
                if describe.loc[describe['name']==col_describe]['type'].max().upper() == 'TEXT':
                    data_df[col_describe] = "'" + data_df[col_describe].str.replace("'",' ') + "'"
           
            data_df['values'] = ''
            for c in cols:
                data_df['values'] = data_df['values'] + ',' + data_df[c]
            
            data_df['values'] = "(" + data_df['values'].str[1:] + ")"
                   
            list_values = data_df['values'].to_list()
            
            list_values = [',\n'.join(list(str(i) for i in a)) for a in np.array_split(list_values, int(len(list_values)/10000) + 1)]
            
            for values in list_values:    
                insert = "insert into " + name_table + "(" + ','.join(cols) + ") values \n" +  values
                
                cur.execute(insert)  
                
            self.conn.commit()
