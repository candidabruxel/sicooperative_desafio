-- Inserts para a tabela associado
INSERT INTO associado (nome, sobrenome, idade, email) VALUES
    ('Carlos', 'Ferreira', 28, 'carlos.ferreira@email.com'),
    ('Ana', 'Oliveira', 35, 'ana.oliveira@email.com'),
    ('Pedro', 'Almeida', 40, 'pedro.almeida@email.com'),
    ('Mariana', 'Costa', 22, 'mariana.costa@email.com'),
    ('Ricardo', 'Silveira', 33, 'ricardo.silveira@email.com'),
    ('Camila', 'Santana', 29, 'camila.santana@email.com'),
    ('Lucas', 'Pereira', 31, 'lucas.pereira@email.com'),
    ('Aline', 'Martins', 26, 'aline.martins@email.com'),
    ('Fernando', 'Ribeiro', 37, 'fernando.ribeiro@email.com'),
    ('Juliana', 'Lima', 24, 'juliana.lima@email.com');

-- Inserts para a tabela conta
INSERT INTO conta (tipo, data_criacao, id_associado) VALUES
    ('Investimento', '2023-02-10', 3),
    ('Corrente', '2023-02-15', 4),
    ('Corrente', '2023-02-20', 5),
    ('Investimento', '2023-02-25', 6),
    ('Corrente', '2023-03-01', 7),
    ('Corrente', '2023-03-05', 8),
    ('Investimento', '2023-03-10', 9),
    ('Corrente', '2023-03-15', 10),
    ('Corrente', '2023-03-20', 1),
    ('Investimento', '2023-03-25', 2);

-- Inserts para a tabela cartao
INSERT INTO cartao (num_cartao, nom_impresso, id_conta, id_associado) VALUES
    (12345, 'Carlos Ferreira', 9, 1),
    (98765, 'Ana Oliveira', 10, 2),
    (11112, 'Pedro Almeida', 1, 3),
    (55556, 'Mariana Costa', 2, 4),
    (99990, 'Ricardo Silveira', 3, 5),
    (33334, 'Camila Santana', 4, 6),
    (77778, 'Lucas Pereira', 5, 7),
    (44445, 'Aline Martins', 6, 8),
    (88889, 'Fernando Ribeiro', 7, 9),
    (22223, 'Juliana Lima', 8, 10);

-- Inserts para a tabela movimento
INSERT INTO movimento (vls_transacao, des_transacao, data_movimento, id_cartao) VALUES
    (75.20, 'Compra online', '2023-04-01', 1),
    (30.50, 'Posto de gasolina', '2023-04-05', 2),
    (150.75, 'Supermercado', '2023-04-10', 3),
    (45.80, 'Restaurante', '2023-04-15', 4),
    (60.00, 'Compra eletronicos', '2023-04-20', 5),
    (25.30, 'Farmacia', '2023-04-25', 6),
    (80.45, 'Livraria', '2023-05-01', 7),
    (55.60, 'Shopping', '2023-05-05', 8),
    (90.20, 'Viagem', '2023-05-10', 9),
    (40.75, 'Cinema', '2023-05-15', 10);