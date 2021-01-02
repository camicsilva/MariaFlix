import psycopg2
import sys

class MariaDB:
    def __init__(self, user="postgres", psw="33713546", host="127.0.0.1", port="5432",database="MariaFlix", cursor=None, dados=None, column_update=None):
        self.dados = dados
        self.user = user
        self.psw = psw
        self.host = host
        self.port = port
        self.database = database
        self.cursor = cursor
        self.column_update = column_update


    def connection(self):

        self.connection = psycopg2.connect(user=self.user,
                                      password=self.psw,
                                      host=self.host,
                                      port=self.port,
                                      database=self.database)

        # Create a cursor to perform database operations
        self.cursor = self.connection.cursor()
        return self.cursor, self.connection

    def consult_users(self):
        self.cursor.execute('select * from pessoa')
        result_cons = self.cursor.fetchall()
        result = []
        for row in result_cons:
            cpf = row[0]
            nome = row[1]
            telefone = row[2]
            nascimento = row[3]

            obj = {"cpf": cpf,
                   "nome": nome,
                   "telefone": telefone,
                   "nascimento": str(nascimento)}

            result.append(obj)

        return result

    def insert_users(self):
        query = ''' 
        INSERT INTO pessoa (cpf, nome, telefone, nascimento) VALUES (%s, %s, %s, %s)
        '''

        self.cursor.execute(query, (self.dados))
        self.connection.commit()

    def update_users(self):

        query = '''
        UPDATE pessoa
        set '''+self.column_update+''' = %s
        WHERE cpf = %s'''
        self.cursor.execute(query, (self.dados))
        self.connection.commit()

    def delete_users(self):
        query = '''
        DELETE FROM pessoa
        WHERE cpf = %s
        '''
        self.cursor.execute(query, (self.dados))
        self.connection.commit()

    def insert_filmes_series(self):
        query = '''
        INSERT INTO filme_series (nome, tipo, temporada, episodio) VALUES (%s, %s, %s, %s)
        '''
        self.cursor.execute(query, (self.dados))
        self.connection.commit()

    def consult_filmes_series(self):
        self.cursor.execute('select * from filme_series')
        result_cons = self.cursor.fetchall()
        result = []
        for row in result_cons:
            id = row[0]
            nome = row[1]
            tipo = row[2]
            temporada = row[3]
            episodio = row[4]

            obj = {"id": id,
                   "nome": nome,
                   "tipo": tipo,
                   "temporada": temporada,
                   "episodio": episodio
                   }

            result.append(obj)

        return result

    def update_filmes_series(self):
        query = '''
                UPDATE filme_series
                set ''' + self.column_update + ''' = %s
                WHERE id = %s'''
        self.cursor.execute(query, (self.dados))
        self.connection.commit()

    def delete_filmes_series(self):
        query = '''
                DELETE FROM filme_series
                WHERE id = %s
                '''
        self.cursor.execute(query, (self.dados))
        self.connection.commit()

    def insert_assistidos(self):
        query = '''
        INSERT INTO assistidos (cpf_pessoa, id_filmes_series, data_assistido)
        VALUES (%s, %s, %s)
        '''
        self.cursor.execute(query, (self.dados))
        self.connection.commit()

    def consult_assistidos(self):
        self.cursor.execute('select * from assistidos')
        result_cons = self.cursor.fetchall()
        result = []
        for row in result_cons:
            cpf_pessoa = row[0]
            id_filmes = row[1]
            data_assistidos = row[2]

            obj = {"cpf_pessoa": cpf_pessoa,
                   "id_filmes_series": id_filmes,
                   "data_assistido": str(data_assistidos),
                   }

            result.append(obj)

        return result

    def consult_filmes_assistidos(self):
        query='''
        SELECT 
	         NOME 
	        ,TO_CHAR(DATA_ASSISTIDO, 'DD/MM/YYYY') AS DATA
        FROM
			FILME_SERIES	T0
        LEFT JOIN	ASSISTIDOS		T1	ON T0.ID = T1.ID_FILMES_SERIES
        WHERE
	        T0.TIPO = 'F'
        AND	T1.CPF_PESSOA = %s
        '''
        self.cursor.execute(query, (self.dados))
        result_cons = self.cursor.fetchall()
        result = []
        for row in result_cons:
            nome = row[0]
            data = row[1]

            obj = {"nome": nome,
                   "data_assistido": data
                   }

            result.append(obj)

        return result

    def consult_temporadas_inteiras(self):
        query = '''
        DROP TABLE IF EXISTS USUARIO_TEMP_ASSISTIDAS;
        WITH TEMPORARIA_2 AS (
        SELECT
             T1.ID_FILMES_SERIES
            ,T0.TEMPORADA
            ,T0.NOME
            ,COUNT (DISTINCT CPF_PESSOA)			AS QTD
        
        FROM
                FILME_SERIES	T0
        LEFT JOIN	ASSISTIDOS		T1	ON T0.ID = T1.ID_FILMES_SERIES
        WHERE
            T0.TIPO = 'S'
        AND	T1.CPF_PESSOA = %s
        GROUP BY 
             T1.ID_FILMES_SERIES
            ,T0.TEMPORADA
            ,T0.NOME
        )
        SELECT
             NOME
            ,TEMPORADA
            ,SUM (QTD) AS QTD_EP
        INTO
            USUARIO_TEMP_ASSISTIDAS
        FROM 
            TEMPORARIA_2
        GROUP BY
             NOME
            ,TEMPORADA;
    
        DROP TABLE IF EXISTS TEMPORADAS_INTEIRAS_ASSISTIDAS;
        WITH TEMPORARIA_3 AS (
        SELECT
             NOME
            ,TEMPORADA
            ,COUNT (*)			AS QTD
        FROM
            FILME_SERIES
        WHERE
            TIPO = 'S'
        GROUP BY
             NOME
            ,TEMPORADA
        )
        SELECT 
             T0.NOME
            ,T0.TEMPORADA
        INTO
            TEMPORADAS_INTEIRAS_ASSISTIDAS
        FROM
                USUARIO_TEMP_ASSISTIDAS T0
        INNER JOIN	TEMPORARIA_3			T1	ON	T0.TEMPORADA = T1.TEMPORADA AND T0.NOME = T1.NOME
        WHERE
            T0.QTD_EP = T1.QTD;
    
        SELECT * FROM TEMPORADAS_INTEIRAS_ASSISTIDAS
        '''
        self.cursor.execute(query, (self.dados))
        result_cons = self.cursor.fetchall()
        result = []
        for row in result_cons:
            nome = row[0]
            temporada = row[1]

            obj = {"nome": nome,
                   "temporada": temporada
                   }

            result.append(obj)

        return result

    def consult_top5(self):
        query = '''
        WITH TEMPORARIA AS (
        SELECT
             T1.CPF_PESSOA
            ,T2.NOME
            ,COUNT (T1.*) AS QTD
        FROM
                    FILME_SERIES	T0
        LEFT JOIN	ASSISTIDOS		T1	ON T0.ID = T1.ID_FILMES_SERIES
        LEFT JOIN	PESSOA			T2 	ON T1.CPF_PESSOA = T2.CPF
        WHERE
            EXTRACT (MONTH FROM T1.DATA_ASSISTIDO) = %s
        AND EXTRACT  (YEAR FROM T1.DATA_ASSISTIDO) = %s
        GROUP BY 
             T1.CPF_PESSOA
            ,T2.NOME
        ORDER BY 
            QTD DESC
        )
        SELECT
              CPF_PESSOA
             ,NOME
        FROM 
            TEMPORARIA
        LIMIT 5
        '''
        self.cursor.execute(query, (self.dados))
        result_cons = self.cursor.fetchall()
        result = []
        for row in result_cons:
            cpf = row[0]
            nome = row[1]

            obj = {"cpf": cpf,
                   "nome": nome
                   }

            result.append(obj)

        return result
