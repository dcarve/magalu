CREATE TABLE parceiro (
	id_parceiro	INTEGER,
	nm_parceiro	TEXT,
	PRIMARY KEY(id_parceiro)
);

CREATE TABLE cliente (
	id_cliente	INTEGER,
	nm_cliente	TEXT,
	flag_ouro	INTEGER,
	PRIMARY KEY(id_cliente)
);


CREATE TABLE estado (
	id_estado	INTEGER,
	ds_estado	TEXT,
	PRIMARY KEY(id_estado)
);

CREATE TABLE cidade (
	id_cidade	INTEGER,
	ds_cidade	TEXT,
	id_estado	INTEGER,
	FOREIGN KEY(id_estado) REFERENCES estado(id_estado),
	PRIMARY KEY(id_cidade)
);

CREATE TABLE filial (
	id_filial	INTEGER,
	ds_filial	TEXT,
	id_cidade	INTEGER,
	FOREIGN KEY(id_cidade) REFERENCES cidade(id_cidade),
	PRIMARY KEY(id_filial)
);

CREATE TABLE pedido (
	id_pedido	INTEGER NOT NULL,
	dt_pedido	TEXT,
	id_parceiro	INTEGER,
	id_cliente	INTEGER,
	id_filial	INTEGER,
	vr_total_pago	REAL,
	FOREIGN KEY(id_parceiro) REFERENCES parceiro(id_parceiro),
	FOREIGN KEY(id_filial) REFERENCES filial(id_filial),
	FOREIGN KEY(id_cliente) REFERENCES cliente(id_cliente),
	PRIMARY KEY(id_pedido)
);

CREATE TABLE categoria (
	id_categoria	INTEGER,
	ds_categoria	TEXT,
	perc_parceiro	REAL,
	PRIMARY KEY(id_categoria)
);

CREATE TABLE sub_categoria (
	id_subcategoria	INTEGER,
	ds_subcategoria	TEXT,
	id_categoria	INTEGER,
	FOREIGN KEY(id_categoria) REFERENCES categoria(id_categoria),
	PRIMARY KEY(id_subcategoria)
);

CREATE TABLE produto (
	id_produto	INTEGER,
	ds_produto	TEXT,
	id_subcategoria	INTEGER,
	FOREIGN KEY(id_subcategoria) REFERENCES sub_categoria(id_subcategoria),
	PRIMARY KEY(id_produto)
);

CREATE TABLE item_pedido (
	id_pedido	INTEGER,
	id_produto	INTEGER,
	quantidade	INTEGER,
	vr_unitario	REAL,
	FOREIGN KEY(id_pedido) REFERENCES pedido(id_pedido),
	FOREIGN KEY(id_produto) REFERENCES produto(id_produto)
);

