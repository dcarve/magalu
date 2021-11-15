# Magalu


Para simular o ETL, precisei pensar numa forma de simular algum tipo de arquitetura, então usei SQLITE para simular o banco de dados transacional e o data warehouse

## Transacional: 
https://github.com/dcarve/magalu/tree/main/gerar_simulucao_db_transacional

Em [__ init __.py ](https://github.com/dcarve/magalu/blob/main/gerar_simulucao_db_transacional/__init__.py), o codigo lê os arquivos [create_tables.sql](https://github.com/dcarve/magalu/blob/main/gerar_simulucao_db_transacional/create_tables.sql) e [delete_tables.sql](https://github.com/dcarve/magalu/blob/main/gerar_simulucao_db_transacional/delete_tables.sql) para deletar (caso existam as tabelas) e para criar.
o código para deletar é apenas para facilitar o debug do código, ele pode ser retirado do código.

o código gera o seguinte arquivo sqlite gerar_simulucao_db_transacional/vendas.db

Precisei fazer uma alteração no tipo de dados de pedido.dt_pedido para string (text),  porque sqlite não suporta date, datetime e timestamp.

O código também usar os arquivos csv com os dados que foram disponibilizados e insere no bando de dados sqlite,  o diretório dos arquivos csv é [gerar_simulucao_db_transacional/data](https://github.com/dcarve/magalu/tree/main/gerar_simulucao_db_transacional/data)

do arquivo item_pedido.csv
removi as seguintes tuplas (linhas), porque os para essas tuplas os id_pedido não existem no arquivo pedido.csv, gerando assim uma violação de chave.
devido a caracteristicas dos bando de dados considerarei que foi um erro.
Em uma situação real, iria entrar em contato com a área responsável, e apontar que há um problema no bando de dados e que a tabela item_pedido não foi criada com uma Foreing Key em id_pedido.

|id_pedido|id_produto|quantidade|vr_unitario|
|---|---|---|---|
|49380480716|4102853|1|90.87|
|49373493516|1822668|2|37.41|
|49373493516|1873163|1|68.63|
|4942505805|4350055|1|21.97|
|49390884213|3204711|1|220.99|
|49370834016|4617321|1|45.49|
|4937222026|3745861|1|467.87|
|9388745716|1596770|1|168.99|
|9372492013|1738325|1|84.49|
|49394876213|4345499|1|233.99|
|49394876213|3275155|1|232.7|
|49434582513|1652034|1|29.89|
|49392532713|1602865|1|311.99|
|49395367016|1846818|1|48.09|
|49451473516|1704469|1|77.99|
|49392006213|3439163|1|123.49|
|49392006213|4801826|1|103.99|
|49371186516|768880|1|31.19|
|49434996213|2278401|1|142.99|
|9377080716|3750522|1|53.29|
|9377042016|920747|1|142.99|
|9381329213|2233007|1|232.7|
|9375455013|2689982|1|220.99|
|9375754016|3258479|1|64.99|
|9372898516|1774043|1|155.99|
|9373114013|2699600|1|128.69|
|49373114013|1004297|1|129.99|
|49373114013|724166|1|103.99|
|49392734713|4066006|1|259.99|
|49373670013|2486675|1|45.49|
|49393120713|3806899|1|149.49|
|9394085016|4587161|1|97.4|




## data warehouse


