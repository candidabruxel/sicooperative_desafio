-- Criação das tabelas
CREATE TABLE associado (
    id SERIAL PRIMARY KEY,
    nome VARCHAR(50),
    sobrenome VARCHAR(50),
    idade INTEGER,
    email VARCHAR(100)
);

CREATE TYPE tipo_conta AS ENUM ('Investimento', 'Corrente');

CREATE TABLE conta (
    id SERIAL PRIMARY KEY,
    tipo tipo_conta,
    data_criacao TIMESTAMP,
    id_associado INTEGER REFERENCES associado(id)
);

CREATE TABLE cartao (
    id SERIAL PRIMARY KEY,
    num_cartao INTEGER,
    nom_impresso VARCHAR(100),
    id_conta INTEGER REFERENCES conta(id),
    id_associado INTEGER REFERENCES associado(id)
);

CREATE TABLE movimento (
    id SERIAL PRIMARY KEY,
    vls_transacao DECIMAL(10, 2),
    des_transacao VARCHAR(255),
    data_movimento TIMESTAMP,
    id_cartao INTEGER REFERENCES cartao(id)
);


