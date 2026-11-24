import json
import sqlite3
from kafka import KafkaConsumer, KafkaProducer

# Banco próprio do Orquestrador para rastrear o estado de cada pedido
conn = sqlite3.connect('orquestrador.db')
cursor = conn.cursor()
cursor.execute('''
    CREATE TABLE IF NOT EXISTS pedidos (
        pedido_id TEXT PRIMARY KEY,
        produto_id INTEGER,
        quantidade INTEGER,
        status TEXT,
        nota_fiscal TEXT,
        rastreio TEXT
    )
''')
conn.commit()

consumer = KafkaConsumer(
    'pedidos.iniciados',
    'orquestrador.respostas',
    bootstrap_servers=['localhost:9092'],
    group_id='grupo-orquestrador',
    auto_offset_reset='earliest',
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    key_serializer=lambda k: str(k).encode('utf-8'),
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print("=== Orquestrador de Pedidos rodando... ===")

try:
    for msg in consumer:
        dados = msg.value
        pid = dados['pedido_id']

        # 1. Chegou um pedido novo da API -> Manda reservar estoque
        if msg.topic == 'pedidos.iniciados':
            prod_id = dados['produto_id']
            qtd = dados['quantidade']

            cursor.execute(
                'INSERT OR REPLACE INTO pedidos (pedido_id, produto_id, quantidade, status) VALUES (?, ?, ?, ?)',
                (pid, prod_id, qtd, 'RESERVANDO_ESTOQUE')
            )
            conn.commit()

            print(f"\n[Pedido {pid}] Iniciado. Ordenando reserva de estoque...")
            producer.send('estoque.comandos', key=pid, value={
                "acao": "RESERVAR",
                "pedido_id": pid,
                "produto_id": prod_id,
                "quantidade": qtd
            })

        # 2. Chegou resposta de algum serviço especialista
        elif msg.topic == 'orquestrador.respostas':
            etapa = dados['etapa']
            sucesso = dados['sucesso']

            if etapa == 'ESTOQUE':
                acao = dados['acao']
                if acao == 'RESERVAR':
                    if sucesso:
                        cursor.execute('UPDATE pedidos SET status = ? WHERE pedido_id = ?', ('EMITINDO_NF', pid))
                        conn.commit()
                        print(f"[Pedido {pid}] Estoque OK! Ordenando emissão de NF...")
                        producer.send('nf.comandos', key=pid, value={"pedido_id": pid})
                    else:
                        cursor.execute('UPDATE pedidos SET status = ? WHERE pedido_id = ?', ('CANCELADO_SEM_ESTOQUE', pid))
                        conn.commit()
                        print(f"[Pedido {pid}] Cancelado por falta de estoque.")

                elif acao == 'ESTORNAR':
                    cursor.execute('UPDATE pedidos SET status = ? WHERE pedido_id = ?', ('CANCELADO_ESTORNADO', pid))
                    conn.commit()
                    print(f"[Pedido {pid}] Estoque estornado! Pedido cancelado com segurança.")

            elif etapa == 'NF':
                if sucesso:
                    nf = dados['nota_fiscal']
                    cursor.execute('UPDATE pedidos SET status = ?, nota_fiscal = ? WHERE pedido_id = ?', ('DESPACHANDO', nf, pid))
                    conn.commit()
                    print(f"[Pedido {pid}] NF {nf} emitida! Ordenando transportadora...")
                    producer.send('transportadora.comandos', key=pid, value={
                        "pedido_id": pid,
                        "nota_fiscal": nf
                    })
                else:
                    # Compensação (Saga): NF falhou -> Orquestrador manda devolver o estoque!
                    cursor.execute('SELECT produto_id, quantidade FROM pedidos WHERE pedido_id = ?', (pid,))
                    prod_id, qtd = cursor.fetchone()
                    cursor.execute('UPDATE pedidos SET status = ? WHERE pedido_id = ?', ('ESTORNANDO_ESTOQUE', pid))
                    conn.commit()
                    print(f"[Pedido {pid}] Falha na NF! Ordenando estorno do estoque...")
                    producer.send('estoque.comandos', key=pid, value={
                        "acao": "ESTORNAR",
                        "pedido_id": pid,
                        "produto_id": prod_id,
                        "quantidade": qtd
                    })

            elif etapa == 'TRANSPORTADORA':
                if sucesso:
                    rastreio = dados['rastreio']
                    cursor.execute('UPDATE pedidos SET status = ?, rastreio = ? WHERE pedido_id = ?', ('CONCLUIDO', rastreio, pid))
                    conn.commit()
                    print(f"[Pedido {pid}] CONCLUÍDO! Rastreio: {rastreio}")

        producer.flush()

except KeyboardInterrupt:
    print("\nEncerrando Orquestrador...")
finally:
    consumer.close()
    producer.close()
    conn.close()