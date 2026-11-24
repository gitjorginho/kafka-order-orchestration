import json, time, uuid
from kafka import KafkaConsumer, KafkaProducer

consumer = KafkaConsumer(
    'transportadora.comandos',
    bootstrap_servers=['localhost:9092'],
    group_id='grupo-transportadora',
    auto_offset_reset='earliest',
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    key_serializer=lambda k: str(k).encode('utf-8'),
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print("=== Serviço de Transportadora (Worker) aguardando comandos... ===")

try:
    for msg in consumer:
        pid = msg.value['pedido_id']
        nf = msg.value['nota_fiscal']
        print(f"[Transportadora] Despachando pedido {pid} (NF: {nf})...")
        time.sleep(1.5)

        producer.send('orquestrador.respostas', key=pid, value={
            "etapa": "TRANSPORTADORA",
            "pedido_id": pid,
            "sucesso": True,
            "rastreio": f"BR{str(uuid.uuid4())[:8].upper()}"
        })
        producer.flush()
except KeyboardInterrupt:
    pass
finally:
    consumer.close()
    producer.close()