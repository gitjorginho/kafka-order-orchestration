import json, time, random
from kafka import KafkaConsumer, KafkaProducer

consumer = KafkaConsumer(
    'nf.comandos',
    bootstrap_servers=['localhost:9092'],
    group_id='grupo-nf',
    auto_offset_reset='earliest',
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    key_serializer=lambda k: str(k).encode('utf-8'),
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print("=== Serviço de NF (Worker) aguardando comandos... ===")

try:
    for msg in consumer:
        pid = msg.value['pedido_id']
        print(f"[NF] Emitindo nota para pedido {pid}...")
        time.sleep(1.5)

        # Dica: mude 'sucesso' para False abaixo se quiser ver o Orquestrador estornando o estoque!
        producer.send('orquestrador.respostas', key=pid, value={
            "etapa": "NF",
            "pedido_id": pid,
            "sucesso": True,
            "nota_fiscal": f"NF-{random.randint(10000, 99999)}"
        })
        producer.flush()
except KeyboardInterrupt:
    pass
finally:
    consumer.close()
    producer.close()