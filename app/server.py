import json
import os

import psycopg2
import redis
from bottle import Bottle, request, route, run


class Server(Bottle):
    def __init__(self):
        super().__init__()
        
        # conexão com postgres
        db_host = os.getenv('DB_HOST', 'db')
        db_user = os.getenv('DB_USERNAME', 'postgres')
        db_name = os.getenv('DB_NAME', 'email_sender')
        dsn = f'dbname={db_name} user={db_user} host={db_host}'

        print(f'#################### conectando no banco {db_name}')

        self.conn = psycopg2.connect(dsn)


        redis_host = os.getenv('REDIS_HOST', 'queue')
        self.fila = redis.StrictRedis(host=redis_host, port=6379, db=0) # queue -> container do redis 3.2
        self.initRoutes()
        

    def register_message(self, assunto, mensagem):
        sql = 'INSERT INTO emails (assunto, mensagem) VALUES (%s, %s)'
        cur = self.conn.cursor()
        cur.execute(sql, (assunto, mensagem))
        self.conn.commit()
        cur.close()

        msg = {'assunto': assunto, 'mensagem': mensagem}
        self.fila.rpush('sender', json.dumps(msg))

        return 'Mensagem enfileirada'

    def send(self):
        assunto = request.forms.get('assunto')
        mensagem = request.forms.get('mensagem')
        response = self.register_message(assunto, mensagem)
        return response 

    def initRoutes(self):
        self.route('/', method='POST', callback=self.send)


if __name__ == '__main__':
    srv = Server()
    srv.run(host='0.0.0.0', port=8080, debug=True) #starta o server na porta 8080
