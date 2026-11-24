import json
import sqlite3
from kafka import KafkaConsumer, KafkaProducer

conn = sqlite3.connect('estoque.db')
cursor = conn.cursor()
cursor.execute('CREATE TABLE IF NOT EXISTS produtos (id INTEGER PRIMARY KEY, nome TEXT NOT NULL, quantidade INTEGER NOT NULL)')
cursor.execute('SELECT COUNT(*) FROM produtos')
if cursor.fetchone()[0] == 0:
    cursor.executemany('INSERT INTO produtos (id, nome, quantidade) VALUES (?, ?, ?)', [(1, 'Notebook', 3), (2, 'Mouse', 1)])
    conn.commit()

consumer = KafkaConsumer(
    'estoque.comandos',
    bootstrap_servers=['localhost:9092'],
    group_id='grupo-estoque',
    auto_offset_reset='earliest',
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    key_serializer=lambda k: str(k).encode('utf-8'),
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print("=== Serviço de Estoque (Worker) aguardando comandos... ===")

try:
    for msg in consumer:
        cmd = msg.value
        acao = cmd['acao']
        pid = cmd['pedido_id']
        prod_id = cmd['produto_id']
        qtd = cmd['quantidade']

        if acao == 'RESERVAR':
            cursor.execute(
                'UPDATE produtos SET quantidade = quantidade - ? WHERE id = ? AND quantidade >= ?',
                (qtd, prod_id, qtd)
            )
            conn.commit()
            sucesso = cursor.rowcount > 0
            print(f"[RESERVAR] Pedido {pid}: {'OK' if sucesso else 'Sem estoque'}")

        elif acao == 'ESTORNAR':
            cursor.execute(
                'UPDATE produtos SET quantidade = quantidade + ? WHERE id = ?',
                (qtd, prod_id)
            )
            conn.commit()
            sucesso = True
            print(f"[ESTORNAR] Pedido {pid}: {qtd} item(ns) devolvido(s) ao estoque.")

        producer.send('orquestrador.respostas', key=pid, value={
            "etapa": "ESTOQUE",
            "acao": acao,
            "pedido_id": pid,
            "sucesso": sucesso
        })
        producer.flush()

except KeyboardInterrupt:
    print("\nEncerrando Estoque...")
finally:
    consumer.close()
    producer.close()
    conn.close()