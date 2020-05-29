import psycopg2
from bottle import route, run, request


dsn = 'dbname=email_sender user=postgres host=db'
sql = 'INSERT INTO emails (assunto, mensagem) VALUES (%s, %s)'

def register_message(assunto, mensagem):
    conn = psycopg2.connect(dsn)
    cur = conn.cursor()
    cur.execute(sql, (assunto, mensagem))
    conn.commit()
    cur.close()
    conn.close()

    return 'Mensagem enfileirada'


@route('/', method='POST')
def send():
    assunto = request.forms.get('assunto')
    mensagem = request.forms.get('mensagem')
    response = register_message(assunto, mensagem)
    return response 

if __name__ == '__main__':
    run(host='0.0.0.0', port=8080, debug=True) #starta o server na porta 8080