import json
import uuid
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    key_serializer=lambda k: str(k).encode('utf-8'),
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print("=== API de Pedidos (Digite 'sair' para encerrar) ===")
print("Produtos cadastrados no teste: 1 = Notebook | 2 = Mouse\n")

try:
    while True:
        entrada = input("Digite ID do produto e quantidade (ex: 1 2): ").strip()
        if entrada.lower() == 'sair':
            break

        try:
            produto_id, quantidade = map(int, entrada.split())
        except ValueError:
            print("Formato inválido! Digite dois números separados por espaço (ex: 1 1).\n")
            continue

        pedido_id = str(uuid.uuid4())[:8]
        evento = {
            "pedido_id": pedido_id,
            "produto_id": produto_id,
            "quantidade": quantidade
        }

        producer.send('pedidos.criados', key=pedido_id, value=evento)
        producer.flush()

        print(f"[202 Accepted] Pedido {pedido_id} enviado para processamento!\n")

except KeyboardInterrupt:
    print("\nEncerrando API...")
finally:
    producer.close()