CREATE TABLE cliente (
	id_cliente	INTEGER,
	nm_cliente	TEXT,
	flag_ouro	INTEGER,
	dt_carga	TEXT
);

CREATE TABLE filial (
	id_filial	INTEGER,
	ds_filial	TEXT,
	ds_cidade	TEXT,
	ds_estado	TEXT,
	dt_carga	TEXT
);

CREATE TABLE pedido (
	id_pedido	INTEGER,
	id_cliente	INTEGER,
	id_filial	INTEGER,
	id_produto	INTEGER,
	vr_unitario	REAL,
	dt_pedido	TEXT,
	quantidade	INTEGER,
	nm_parceiro	TEXT
);

CREATE TABLE produto (
	id_produto	INTEGER,
	ds_produto	TEXT,
	perc_parceiro	REAL,
	ds_subcategoria	TEXT,
	ds_categoria	TEXT,
	dt_carga	TEXT
);