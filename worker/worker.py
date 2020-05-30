import redis
import json
import os
from time import sleep
from random import randint

print('starting worker...')

if __name__ == '__main__':
    redis_host = os.getenv('REDIS_HOST', 'queue')
    r = redis.Redis(host=redis_host, port=6379, db=0)
    while True:
        mensagem = json.loads(r.blpop('sender')[1])
        # Simulando envio de email...
        print("#################### Enviando mensagem")
        sleep(randint(5, 10))
        print('#################### Mensagem enviada com sucesso: ' + mensagem['assunto'])