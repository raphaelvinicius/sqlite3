# manager_db.py
import os
import sqlite3
import io
import datetime
import names
import csv
from gen_random_values import *

class Connect(object):
    def __init__(self, db_name):
        try:
            # conectando...
            self.conn = sqlite3.connect(db_name)
            self.cursor = self.conn.cursor()
            print("Banco:", db_name)
            self.cursor.execute('SELECT SQLITE_VERSION()')
            self.data = self.cursor.fetchone()
            print("SQLite version: %s" % self.data)
        except sqlite3.Error:
            print("Erro ao abrir banco.")
            return False

    def commit_db(self):
        if self.conn:
            self.conn.commit()
            
    def close_db(self):
        if self.conn:
            self.conn.close()
            print("Conexão fechada.")
         
class ClientesDb(object):
    tb_name = 'clientes'
    def __init__(self):
        self.db = Connect('clientes.db')
        self.tb_name
    def fechar_conexao(self):
        self.db.close_db()

    def criar_schema(self, schema_name='clientes_schema.sql'):
        print("Criando tabela %s ..." % self.tb_name)
        try:
            with open(schema_name, 'rt') as f:
                schema = f.read()
                self.db.cursor.executescript(schema)
        except sqlite3.Error:
            print("Aviso: A tabela %s já existe." % self.tb_name)
            return False
        print("Tabela %s criada com sucesso." % self.tb_name)

        
    def inserir_um_registro(self):
        try:
            self.db.cursor.execute(""" INSERT INTO clientes (nome, idade, cpf, email, fone, cidade, uf, criado_em) VALUES ('Regis da Silva', 35, '12345678901', 'regis@email.com', '(11) 9876-5342', 'Sao Paulo', 'SP', '2014-07-30 11:23:00.199000') """)
            # gravando no bd
            self.db.commit_db()
            print("Um registro inserido com sucesso.")
        except sqlite3.IntegrityError:
            print("Aviso: O email deve ser único.")
            return False
        
    def inserir_com_lista(self):
        # criando uma lista de dados
        lista = [('Agenor de Sousa', 23, '12345678901', 'agenor@email.com',
                  '(10) 8300-0000', 'Salvador', 'BA', '2014-07-29 11:23:01.199001'),
                 ('Bianca Antunes', 21, '12345678902', 'bianca@email.com',
                  '(10) 8350-0001', 'Fortaleza', 'CE', '2014-07-28 11:23:02.199002'),
                 ('Carla Ribeiro', 30, '12345678903', 'carla@email.com',
                  '(10) 8377-0002', 'Campinas', 'SP', '2014-07-28 11:23:03.199003'),
                 ('Fabiana de Almeida', 25, '12345678904', 'fabiana@email.com',
                  '(10) 8388-0003', 'São Paulo', 'SP', '2014-07-29 11:23:04.199004'),
                 ]
        try:
            self.db.cursor.executemany("""
            INSERT INTO clientes (nome, idade, cpf, email, fone, cidade, uf, criado_em)
            VALUES (?,?,?,?,?,?,?,?)
            """, lista)
            # gravando no bd
            self.db.commit_db()
            print("Dados inseridos da lista com sucesso: %s registros." %
                  len(lista))
        except sqlite3.IntegrityError:
            print("Aviso: O email deve ser único.")
            return False

    def inserir_de_arquivo(self):
        try:
            with open('clientes_dados.sql.txt', 'rt') as f:
                dados = f.read()
                self.db.cursor.executescript(dados)
                # gravando no bd
                self.db.commit_db()
                print("Dados inseridos do arquivo com sucesso.")
        except sqlite3.IntegrityError:
            print("Aviso: O email deve ser único.")
            return False

    def inserir_de_csv(self, file_name='clientes.csv'):
        try:
            reader = csv.reader(
                open(file_name, 'rt'), delimiter=',')
            linha = (reader,)
            for linha in reader:
                self.db.cursor.execute("""
                INSERT INTO clientes (nome, idade, cpf, email, fone, cidade, uf, criado_em)
                VALUES (?,?,?,?,?,?,?,?)
                """, linha)
            # gravando no bd
            self.db.commit_db()
            print("Dados importados do csv com sucesso.")
        except sqlite3.IntegrityError:
            print("Aviso: O email deve ser único.")
            return False

    def inserir_com_parametros(self):
        # solicitando os dados ao usuario
        self.nome = input('Nome: ')
        self.idade = input('Idade: ')
        self.cpf = input('CPF: ')
        self.email = input('Email: ')
        self.fone = input('Fone: ')
        self.cidade = input('Cidade: ')
        self.uf = input('UF: ') or 'SP'
        date = datetime.datetime.now().isoformat(" ")
        self.criado_em = input('Criado em (%s): ' % date) or date

        try:
            self.db.cursor.execute("""
            INSERT INTO clientes (nome, idade, cpf, email, fone, cidade, uf, criado_em)
            VALUES (?,?,?,?,?,?,?,?)
            """, (self.nome, self.idade, self.cpf, self.email, self.fone,
                  self.cidade, self.uf, self.criado_em))
            # gravando no bd
            self.db.commit_db()
            print("Dados inseridos com sucesso.")
        except sqlite3.IntegrityError:
            print("Aviso: O email deve ser único.")
            return False

        
    def inserir_randomico(self, repeat=10):
        ''' Inserir registros com valores randomicos names '''
        lista = []
        for _ in range(repeat):
            date = datetime.datetime.now().isoformat(" ")
            fname = names.get_first_name()
            lname = names.get_last_name()
            name = fname + ' ' + lname
            email = fname[0].lower() + '.' + lname.lower() + '@email.com'
            c = gen_city()
            city = c[0]
            uf = c[1]
            lista.append((name, gen_age(), gen_cpf(),
                          email, gen_phone(),
                          city, uf, date))
        try:
            self.db.cursor.executemany("""
            INSERT INTO clientes (nome, idade, cpf, email, fone, cidade, uf, criado_em)
            VALUES (?,?,?,?,?,?,?,?)
            """, lista)
            self.db.commit_db()
            print("Inserindo %s registros na tabela..." % repeat)
            print("Registros criados com sucesso.")
        except sqlite3.IntegrityError:
            print("Aviso: O email deve ser único.")
            return False

    def ler_todos_clientes(self):
        sql = 'SELECT * FROM clientes ORDER BY nome'
        r = self.db.cursor.execute(sql)
        return r.fetchall()

    def imprimir_todos_clientes(self):
        lista = self.ler_todos_clientes()
        print('{:>3s} {:20s} {:<5s} {:15s} {:21s} {:14s} {:15s} {:s} {:s}'.format(
            'id', 'nome', 'idade', 'cpf', 'email', 'fone', 'cidade', 'uf', 'criado_em'))
        for c in lista:
            print('{:3d} {:23s} {:2d} {:s} {:>25s} {:s} {:15s} {:s} {:s}'.format(
                c[0], c[1], c[2],
                c[3], c[4], c[5],
                c[6], c[7], c[8]))
            
    def localizar_cliente(self, id):
        r = self.db.cursor.execute(
            'SELECT * FROM clientes WHERE id = ?', (id,))
        return r.fetchone()

    def imprimir_cliente(self, id):
        if self.localizar_cliente(id) == None:
            print('Não existe cliente com o id informado.')
        else:
            print(self.localizar_cliente(id))

    def contar_cliente(self):
        r = self.db.cursor.execute(
            'SELECT COUNT(*) FROM clientes')
        print("Total de clientes:", r.fetchone()[0])
        
    def contar_cliente_por_idade(self, t=50):
        r = self.db.cursor.execute(
            'SELECT COUNT(*) FROM clientes WHERE idade > ?', (t,))
        print("Clientes maiores que", t, "anos:", r.fetchone()[0])

    def localizar_cliente_por_idade(self, t=50):
        resultado = self.db.cursor.execute(
            'SELECT * FROM clientes WHERE idade > ?', (t,))
        print("Clientes maiores que", t, "anos:")
        for cliente in resultado.fetchall():
            print(cliente)

    def localizar_cliente_por_uf(self, t='SP'):
        resultado = self.db.cursor.execute(
            'SELECT * FROM clientes WHERE uf = ?', (t,))
        print("Clientes do estado de", t, ":")
        for cliente in resultado.fetchall():
            print(cliente)
            
    def meu_select(self, sql="SELECT * FROM clientes WHERE uf='RJ';"):
        r = self.db.cursor.execute(sql)
        # gravando no bd
        self.db.commit_db()
        for cliente in r.fetchall():
            print(cliente)

    def ler_arquivo(self, file_name='clientes_sp.sql'):
        with open(file_name, 'rt') as f:
            dados = f.read()
            sqlcomandos = dados.split(';')
            print("Consulta feita a partir de arquivo externo.")
            for comando in sqlcomandos:
                r = self.db.cursor.execute(comando)
                for c in r.fetchall():
                    print(c)
        # gravando no bd
        self.db.commit_db()

    def atualizar(self, id):
        try:
            c = self.localizar_cliente(id)
            if c:
                # solicitando os dados ao usuário
                self.novo_fone = input('Fone: ')
                self.db.cursor.execute("""
                UPDATE clientes
                SET fone = ?
                WHERE id = ?
                """, (self.novo_fone, id,))
                # gravando no bd
                self.db.commit_db()
                print("Dados atualizados com sucesso.")
            else:
                print('Não existe cliente com o id informado.')
        except e:
            raise e

    def deletar(self, id):
        try:
            c = self.localizar_cliente(id)
            # verificando se existe cliente com o ID passado, caso exista
            if c:
                self.db.cursor.execute("""
                DELETE FROM clientes WHERE id = ?
                """, (id,))
                # gravando no bd
                self.db.commit_db()
                print("Registro %d excluído com sucesso." % id)
            else:
                print('Não existe cliente com o código informado.')
        except e:
            raise e

    def alterar_tabela(self):
        try:
            self.db.cursor.execute("""
            ALTER TABLE clientes
            ADD COLUMN bloqueado BOOLEAN;
            """)
            # gravando no bd
            self.db.commit_db()
            print("Novo campo adicionado com sucesso.")
        except sqlite3.OperationalError:
            print("Aviso: O campo 'bloqueado' já existe.")
            return False

    def table_info(self):
        # obtendo informações da tabela
        t = self.db.cursor.execute(
            'PRAGMA table_info({})'.format(self.tb_name))
        colunas = [tupla[1] for tupla in t.fetchall()]
        print('Colunas:', colunas)

    def table_list(self):
        # listando as tabelas do bd
        l = self.db.cursor.execute("""
        SELECT name FROM sqlite_master WHERE type='table' ORDER BY name
        """)
        print('Tabelas:')
        for tabela in l.fetchall():
            print("%s" % (tabela))

    def table_schema(self):
        # obtendo o schema da tabela
        s = self.db.cursor.execute("""
        SELECT sql FROM sqlite_master WHERE type='table' AND name=?
        """, (self.tb_name,))

        print('Schema:')
        for schema in s.fetchall():
            print("%s" % (schema))

    def backup(self, file_name='sql/clientes_bkp.sql'):
        with io.open(file_name, 'w') as f:
            for linha in self.db.conn.iterdump():
                f.write('%s\n' % linha)

        print('Backup realizado com sucesso.')
        print('Salvo como %s' % file_name)

    def importar_dados(self, db_name='clientes_recovery.db', file_name='sql/clientes_bkp.sql'):
        try:
            self.db = Connect(db_name)
            f = io.open(file_name, 'r')
            sql = f.read()
            self.db.cursor.executescript(sql)
            print('Banco de dados recuperado com sucesso.')
            print('Salvo como %s' % db_name)
        except sqlite3.OperationalError:
            print(
                "Aviso: O banco de dados %s já existe. Exclua-o e faça novamente." %
                db_name)
            return False

    def fechar_conexao(self):
        self.db.close_db()


class PessoasDb(object):

    tb_name = 'pessoas'

    def __init__(self):
        self.db = Connect('pessoas.db')
        self.tb_name

    def criar_schema(self, schema_name='pessoas_schema.sql.txt'):
        print("Criando tabela %s ..." % self.tb_name)

        try:
            with open(schema_name, 'rt') as f:
                schema = f.read()
                self.db.cursor.executescript(schema)
        except sqlite3.Error:
            print("Aviso: A tabela %s já existe." % self.tb_name)
            return False

        print("Tabela %s criada com sucesso." % self.tb_name)

    def inserir_de_csv(self, file_name='cidades.csv'):
        try:
            c = csv.reader(
                open(file_name, 'rt'), delimiter=',')
            t = (c,)
            for t in c:
                self.db.cursor.execute("""
                INSERT INTO cidades (cidade, uf)
                VALUES (?,?)
                """, t)
            # gravando no bd
            self.db.commit_db()
            print("Dados importados do csv com sucesso.")
        except sqlite3.IntegrityError:
            print("Aviso: A cidade deve ser única.")
            return False

    def gen_cidade(self):
        ''' conta quantas cidades estão cadastradas e escolhe uma delas pelo id. '''
        sql = 'SELECT COUNT(*) FROM cidades'
        q = self.db.cursor.execute(sql)
        return q.fetchone()[0]

    def inserir_randomico(self, repeat=10):
        lista = []
        for _ in range(repeat):
            fname = names.get_first_name()
            lname = names.get_last_name()
            email = fname[0].lower() + '.' + lname.lower() + '@email.com'
            cidade_id = random.randint(1, self.gen_cidade())
            lista.append((fname, lname, email, cidade_id))
        try:
            self.db.cursor.executemany("""
            INSERT INTO pessoas (nome, sobrenome, email, cidade_id)
            VALUES (?,?,?,?)
            """, lista)
            self.db.commit_db()
            print("Inserindo %s registros na tabela..." % repeat)
            print("Registros criados com sucesso.")
        except sqlite3.IntegrityError:
            print("Aviso: O email deve ser único.")
            return False

    def ler_todas_pessoas(self):
        sql = 'SELECT * FROM pessoas INNER JOIN cidades ON pessoas.cidade_id = cidades.id'
        r = self.db.cursor.execute(sql)
        return r.fetchall()
    
    def imprimir_todas_pessoas(self):
        lista = self.ler_todas_pessoas()
        for c in lista:
            print(c)

    def meu_select(self, sql="SELECT * FROM pessoas WHERE nome LIKE 'R%' ORDER BY nome;"):
        r = self.db.cursor.execute(sql)
        self.db.commit_db()
        print('Nomes que começam com R:')
        for c in r.fetchall():
            print(c)

    def table_list(self):
        # listando as tabelas do bd
        l = self.db.cursor.execute("""
        SELECT name FROM sqlite_master WHERE type='table' ORDER BY name
        """)
        print('Tabelas:')
        for tabela in l.fetchall():
            print("%s" % (tabela))

    def fechar_conexao(self):
        self.db.close_db()

            
        

    

    

        
if __name__ == '__main__':
    c = ClientesDb()
