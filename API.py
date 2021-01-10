from flask import Flask, jsonify, request
from flask_restful import Resource, Api, reqparse, abort
from model import MariaDB
import json
import sys
from psycopg2 import Error

app = Flask(__name__)
api = Api(app)

class User(Resource):
    def post(self):
        pessoa = request.get_json()
        for dado in range(0, len(pessoa)):
            if 'cpf' in pessoa[dado]:
                cpf = pessoa[dado]['cpf']
            else:
                return 'CPF do usuario e obrigatorio'

            nome = pessoa[dado]['nome']
            telefone = pessoa[dado]['telefone']
            nascimento = pessoa[dado]['nascimento']

            dados = cpf, nome, telefone, nascimento
            try:
                usuario = MariaDB(dados=dados)
                usuario.connection()
                usuario.insert_users()
                return "Incluído com sucesso"
            except Error as e:
                return str(e)

    def get(self):
        consultUsuario = MariaDB()
        consultUsuario.connection()
        result =consultUsuario.consult_users()
        return result

    def put(self):
        pessoa = request.get_json()
        for dado in range(0, len(pessoa)):
            if "nome" in pessoa[dado]:
                cpf = pessoa[dado]['cpf']
                nome = pessoa[dado]['nome']
                dados = nome, cpf
                usuario = MariaDB(dados=dados, column_update='nome')
                usuario.connection()
                usuario.update_users()
                return 'Nome alterado com sucesso'

            if "telefone" in pessoa[dado]:
                cpf = pessoa[dado]['cpf']
                telefone = pessoa[dado]['telefone']
                dados = telefone, cpf
                usuario = MariaDB(dados=dados, column_update='telefone')
                usuario.connection()
                usuario.update_users()
                return 'Telefone alterado com sucesso'

            if "nascimento" in pessoa[dado]:
                cpf = pessoa[dado]['cpf']
                nascimento = pessoa[dado]['nascimento']
                dados = nascimento, cpf
                usuario = MariaDB(dados=dados, column_update='nascimento')
                usuario.connection()
                usuario.update_users()
                return 'Nascimento alterado com sucesso'

    def delete(self):
        pessoa = request.get_json()
        for dado in range(0, len(pessoa)):
            if "cpf" in pessoa[dado]:
                cpf = pessoa[dado]['cpf']
                dados = cpf,
                try:
                    usuario = MariaDB(dados=dados)
                    usuario.connection()
                    usuario.delete_users()
                    return "Usuario deletado"
                except Error as e:
                    return str(e)
            else:
                return "CPF nao informado"

class Filmes_Series(Resource):
    def post(self):
        filmeSeries = request.get_json()
        for dado in range(0, len(filmeSeries)):
            nome = filmeSeries[dado]['nome']
            tipo = filmeSeries[dado]['tipo']
            if "temporada" in filmeSeries[dado]:
                temporada = filmeSeries[dado]['temporada']
            else:
                temporada = None
            if "episodio" in filmeSeries[dado]:
                episodio = filmeSeries[dado]['episodio']
            else:
                episodio = None

            dados = nome, tipo, temporada, episodio

            filmeserie = MariaDB(dados=dados)
            filmeserie.connection()
            filmeserie.insert_filmes_series()
        return "Incluído com sucesso"

    def get(self):
        filmeserie = MariaDB()
        filmeserie.connection()
        result = filmeserie.consult_filmes_series()
        return result

    def put(self):
        filmeSeries = request.get_json()
        for dado in range(0, len(filmeSeries)):
            if 'id' in filmeSeries[dado]:
                if "nome" in filmeSeries[dado]:
                    id = filmeSeries[dado]['id']
                    nome = filmeSeries[dado]['nome']
                    dados = nome, id
                    filmeserie = MariaDB(dados=dados, column_update='nome')
                    filmeserie.connection()
                    filmeserie.update_filmes_series()

                if "tipo" in filmeSeries[dado]:
                    id = filmeSeries[dado]['id']
                    tipo = filmeSeries[dado]['tipo']
                    dados = tipo, id
                    filmeserie = MariaDB(dados=dados, column_update='tipo')
                    filmeserie.connection()
                    filmeserie.update_filmes_series()

                if "temporada" in filmeSeries[dado]:
                    id = filmeSeries[dado]['id']
                    temporada = filmeSeries[dado]['temporada']
                    dados = temporada, id
                    filmeserie = MariaDB(dados=dados, column_update='temporada')
                    filmeserie.connection()
                    filmeserie.update_filmes_series()

                if "episodio" in filmeSeries[dado]:
                    id = filmeSeries[dado]['id']
                    episodio = filmeSeries[dado]['episodio']
                    dados = episodio, id
                    filmeserie = MariaDB(dados=dados, column_update='episodio')
                    filmeserie.connection()
                    filmeserie.update_filmes_series()

            else:
                return 'Id nao informado'
        return 'Dados alterados com sucesso'

    def delete(self):
        filmeSeries = request.get_json()
        for dado in range(0, len(filmeSeries)):
            if "id" in filmeSeries[dado]:
                id = filmeSeries[dado]['id']
                dados = id,
                try:
                    filmeserie = MariaDB(dados=dados)
                    filmeserie.connection()
                    filmeserie.delete_filmes_series()
                    return "Filme ou serie deletado"
                except Error as e:
                    return str(e)
            else:
                return "Id nao informado"

class assistidos(Resource):
    def post(self):
        assistidos = request.get_json()
        for dado in range(0, len(assistidos)):
            if "cpf_pessoa" in assistidos[dado]:
                cpf = assistidos[dado]['cpf_pessoa']
            else:
                return "CPF do usuario nao informado"

            if 'id_filmes_series' in assistidos[dado]:
                id = assistidos[dado]['id_filmes_series']
            else:
                return "Id do filme ou serie nao informado"

            if 'data_assistido' in assistidos[dado]:
                data = assistidos[dado]['data_assistido']
            else:
                return 'Data em que foi assistido nao informada'

            dados = cpf, id, data
            assistido = MariaDB(dados=dados)
            assistido.connection()
            assistido.insert_assistidos()
        return 'Registro incluido com sucesso'

    def get(self):
        assistido = MariaDB()
        assistido.connection()
        result = assistido.consult_assistidos()
        return result

class rel_filmes_assistidos(Resource):
    def get(self):
        filmes_assistidos = request.get_json()
        for dado in range(0, len(filmes_assistidos)):
            if "cpf_pessoa" in filmes_assistidos[dado]:
                cpf = filmes_assistidos[dado]['cpf_pessoa']
            else:
                return "CPF do usuario nao informado"
            if cpf != None and cpf != "":
                dados = cpf,
                filmesassitidos = MariaDB(dados=dados)
                filmesassitidos.connection()
                result = filmesassitidos.consult_filmes_assistidos()
                return result
            else:
                return "CPF do usuario nao informado"
class rel_temporadas_completas(Resource):
    def get(self):
        temporadas_completas = request.get_json()
        for dado in range(0, len(temporadas_completas)):
            if "cpf_pessoa" in temporadas_completas[dado]:
                cpf = temporadas_completas[dado]['cpf_pessoa']
            else:
                return "CPF do usuario nao informado"
            if cpf != None and cpf != "":
                dados = cpf,
                temporadascompletas = MariaDB(dados=dados)
                temporadascompletas.connection()
                result = temporadascompletas.consult_temporadas_inteiras()
                return result
            else:
                return "CPF do usuario nao informado"

class rel_top5(Resource):
    def get(self):
        topusers = request.get_json()
        for dado in range(0, len(topusers)):
            if "mes" in topusers[dado]:
                mes = topusers[dado]['mes']
            else:
                return "Mes da consulta nao informado"

            if "ano" in topusers[dado]:
                ano = topusers[dado]['ano']
            else:
                return "Ano da consulta nao informado"

            if mes != None and mes != "" and ano != None and ano != "":
                dados = mes, ano
                top_five = MariaDB(dados=dados)
                top_five.connection()
                result = top_five.consult_top5()
                return result
            else:
                return "Mes e/ou ano da consulta nao informado"

api.add_resource(User, '/usuarios')
api.add_resource(Filmes_Series, '/filmeseseries')
api.add_resource(assistidos, '/assistidos')
api.add_resource(rel_filmes_assistidos, '/filmes_assistidos')
api.add_resource(rel_temporadas_completas, '/temporadas_completas')
api.add_resource(rel_top5, '/top_5')

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)