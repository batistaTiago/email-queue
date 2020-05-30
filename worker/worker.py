import redis
import json
from time import sleep
from random import randint

print('starting worker...')

if __name__ == '__main__':
    r = redis.Redis(host='queue', port=6379, db=0)
    while True:
        mensagem = json.loads(r.blpop('sender')[1])
        # Simulando envio de email...
        print("Enviando mensagem...")
        sleep(randint(4, 20))
        print('Mensagem enviada com sucesso: ' + mensagem['assunto'])