CREATE TABLE cliente (
	id_dw_cliente INTEGER,
	id_cliente	INTEGER,
	nm_cliente	TEXT,
	flag_ouro	INTEGER,
	dt_carga	TEXT,
	PRIMARY KEY(id_dw_cliente AUTOINCREMENT)
);

CREATE TABLE filial (
	id_dw_filial	INTEGER,
	id_filial	INTEGER,
	ds_filial	TEXT,
	ds_cidade	TEXT,
	ds_estado	TEXT,
	dt_carga	TEXT,
	PRIMARY KEY(id_dw_filial AUTOINCREMENT)
);

CREATE TABLE produto (
	id_dw_produto	INTEGER,
	id_produto	INTEGER,
	ds_produto	TEXT,
	perc_parceiro	REAL,
	ds_subcategoria	TEXT,
	ds_categoria	TEXT,
	dt_carga	TEXT,
	PRIMARY KEY(id_dw_produto AUTOINCREMENT)
);

CREATE TABLE pedido (
	id_registro INTEGER,
	id_pedido	INTEGER,
	id_dw_cliente	INTEGER,
	id_dw_filial	INTEGER,
	id_dw_produto	INTEGER,
	vr_unitario	REAL,
	dt_pedido	TEXT,
	quantidade	INTEGER,
	nm_parceiro	TEXT,
	PRIMARY KEY(id_registro AUTOINCREMENT)
);


CREATE TABLE cliente_temp (
	id_cliente	INTEGER,
	nm_cliente	TEXT,
	flag_ouro	INTEGER
);

CREATE TABLE filial_temp (
	id_filial	INTEGER,
	ds_filial	TEXT,
	ds_cidade	TEXT,
	ds_estado	TEXT
);

CREATE TABLE produto_temp (
	id_produto	INTEGER,
	ds_produto	TEXT,
	perc_parceiro	REAL,
	ds_subcategoria	TEXT,
	ds_categoria	TEXT
);

CREATE TABLE pedido_temp (
	id_pedido	INTEGER,
	id_cliente	INTEGER,
	id_filial	INTEGER,
	id_produto	INTEGER,
	vr_unitario	REAL,
	dt_pedido	TEXT,
	quantidade	INTEGER,
	nm_parceiro	TEXT
);