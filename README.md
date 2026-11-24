# Kafka Order Orchestration

Projeto prático demonstrando o padrão **Saga por Orquestração** com **Apache Kafka**, **Python** e **SQLite**.

Diferente da abordagem por Coreografia, aqui existe um **Orquestrador central** que coordena todo o fluxo do pedido enviando comandos para os serviços especialistas, acompanhando o estado no banco de dados e executando ações de compensação (estorno) em caso de falha.

## Arquitetura e Fluxo

```text
[1. API Pedidos] ──(pedidos.iniciados)──▶ [  ORQUESTRADOR (+ SQLite)  ]
                                              │                    ▲
                         (A) "Reserva!"       │                    │ "OK, reservado!"
                                              ▼                    │
                                          [ 2. Estoque ]

                                          [  ORQUESTRADOR  ]
                                              │                    ▲
                         (B) "Emite NF!"      │                    │ "OK, NF emitida!"
                                              ▼                    │
                                        [ 3. Nota Fiscal ]

                                          [  ORQUESTRADOR  ]
                                              │                    ▲
                         (C) "Envia!"         │                    │ "OK, Rastreio!"
                                              ▼                    │
                                      [ 4. Transportadora ]
```

### Como funciona a comunicação no Kafka
* **`pedidos.iniciados`**: A API publica o pedido recém-criado.
* **`estoque.comandos` / `nf.comandos` / `transportadora.comandos`**: Tópicos onde o Orquestrador envia ordens específicas para cada serviço.
* **`orquestrador.respostas`**: Tópico único onde os serviços devolvem o resultado (`sucesso: True/False`) para o Orquestrador decidir o próximo passo.

### Serviços
* **`orquestrador.py`**: O "maestro" do fluxo. Mantém o status de cada pedido no banco `orquestrador.db`, dispara os comandos e aciona o estorno de estoque caso a Nota Fiscal falhe.
* **`1_api_pedidos.py`**: Recebe os pedidos no terminal e publica em `pedidos.iniciados`.
* **`2_servico_estoque.py`**: Worker que obedece aos comandos `RESERVAR` e `ESTORNAR` atualizando o banco SQLite (`estoque.db`).
* **`3_servico_nf.py`**: Worker que simula a emissão da Nota Fiscal.
* **`4_servico_transportadora.py`**: Worker que simula o despacho do produto e gera o código de rastreio.

## Tecnologias
* Python 3
* Apache Kafka (`kafka-python`)
* SQLite (`sqlite3`)

## Como executar

1. Certifique-se de que o **Apache Kafka** está rodando em `localhost:9092`.

2. Crie e ative o ambiente virtual e instale as dependências:
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

3. Em terminais separados (com a `.venv` ativada), inicie o Orquestrador e os Workers:
```bash
python3 orquestrador.py
python3 2_servico_estoque.py
python3 3_servico_nf.py
python3 4_servico_transportadora.py
```

4. Em outro terminal, inicie a API para enviar pedidos:
```bash
python3 1_api_pedidos.py
```

### Testando o Estorno (Saga Compensation)
Para ver a orquestração desfazendo uma operação na prática, altere `"sucesso": True` para `"sucesso": False` dentro do arquivo `3_servico_nf.py`. Ao enviar um novo pedido, o estoque será reservado, a NF falhará e o Orquestrador enviará automaticamente o comando `ESTORNAR` devolvendo o item para o `estoque.db`.

---

🔗 **Projeto relacionado:** Veja também a implementação deste mesmo fluxo usando o padrão de Coreografia em [kafka-order-choreography](https://github.com/SEU-USUARIO/kafka-order-choreography).