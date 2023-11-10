# importa a biblioteca paho.mqtt.client, que é utilizada para criar e gerenciar conexões mqtt
import paho.mqtt.client as mqtt
# importa a biblioteca json, utilizada para manipulação de objetos no formato json
import json

# variaveis iniciais: minhas informacoes para identificacao e preenchimento do objeto
eu_nome = "Artur Ritzel"
eu_turma = "4411"
eu_matricula = 20000247

# variaveis necessarias para funcionamento basico do mqtt
broker_adress = "test.mosquitto.org"
broker_port   = 1883
client_identifier = "artur 4411"

# caminhos para os topicos utilizados para envio e recebimento das mensagens
topic_path          = "Liberato/iotTro/44xx/" # caminho base dos topicos
topic_data          = topic_path + "data" # topico de recebimento dos dados
topic_response      = topic_path + "rply/" + str(eu_matricula) # topico para envio das respostas
topic_feedback      = topic_path + "ack" # topico de feedback 
topic_success       = topic_feedback + "/" + str(eu_matricula) # topico de feedback 2

# variaveis globais posteriormente utilizadas para rastrear informacoes relevantes sobre a ultima vez chamado e feedback da resposta
ultima_chamada = "--:--" # horario da ultima chamada
ultimo_feedback = "---" # feedback recebido na ultima resposta
ultima_seq = -1 # sequencia enviada na ultima resposta enviada
esperando_resposta = False # esperando feedback do sistema?

# callback quando o cliente se conecta
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("conectado ao mqtt broker")
    else:
        print(f"conexao falhou com o codigo de erro {rc}")

# callback quando recebe uma mensagem
def on_message(client, userdata, message):

    # para utilizar as variaveis globais de verificacao de resposta
    global ultima_chamada
    global ultimo_feedback
    global ultima_seq
    global esperando_resposta

    # caso a mensagem seja nos topicos de feedback e o usuario esteja esperando uma resposta nesses canais
    if (message.topic == topic_feedback or message.topic == topic_success) and esperando_resposta == True:
        # para nao quebrar caso nao seja um json
        try:
            # decodifica o recebido
            objeto_feedback = json.loads(message.payload.decode())
            # caso a sequencia da resposta seja igual a ultima seq enviado (a resposta eh para a MINHA mensagem)
            if objeto_feedback['seq'] == ultima_seq:
                # para de esperar uma resposta
                esperando_resposta = False
                # armazena o feedback recebido
                ultimo_feedback = objeto_feedback['ack']

        # caso o recebido nao seja um json
        except json.JSONDecodeError as e:
            print("recebida uma mensagem não-json.")

    # para o console nao ficar baguncado, uma nova linha de separacao eh gerada cada vez que uma mensagem eh recebida no topico de dados
    if message.topic == topic_data:
        print()
    
    # debug: printa toda mensagem recebida
    print(f"{message.topic}: {message.payload}")

    # caso o topico da mensagem seja do topico de dados
    if message.topic == topic_data:
        # para de esperar uma resposta. se uma resposta nao foi recebida ate agora, provavelmente a mensagem se perdeu
        esperando_resposta = False

        # tracking de quando fui chamado pela ultima vez, e qual foi o feedback
        print(f"ultima chamada: {ultima_chamada} : {ultimo_feedback}")

        # para nao quebrar caso nao seja um json 
        try:
            # prepara a mensagem a ser enviada
            objeto_resposta = json.loads(message.payload.decode())

            # so continua se a matricula for minha
            if objeto_resposta['matricula'] == eu_matricula:

                # modifica o recebido baseado nos requisitos
                objeto_resposta['seq']   = objeto_resposta['seq'] + 800000
                objeto_resposta['nome']  = eu_nome
                objeto_resposta['turma'] = eu_turma

                #> calculos para o campo 'climatizado':
                temperatura_int_celsius = -1
                temperatura_ext_celsius = -1

                # transforma as temperaturas para celsius, para evitar calculos errados caso as unidades sejam diferentes
                if objeto_resposta['tempInt']['unidade'] == 'C':
                    temperatura_int_celsius = objeto_resposta['tempInt']['valor']
                elif objeto_resposta['tempInt']['unidade'] == 'F':
                    temperatura_int_celsius = (objeto_resposta['tempInt']['valor']-32) * 5 / 9

                if objeto_resposta['tempExt']['unidade'] == 'C':
                    temperatura_ext_celsius = objeto_resposta['tempInt']['valor']
                elif objeto_resposta['tempExt']['unidade'] == 'F':
                    temperatura_ext_celsius = (objeto_resposta['tempInt']['valor']-32) * 5 / 9

                # pra evitar que encontre a temperatura com uma unidade além de celsius ou fahrenheit
                if temperatura_ext_celsius != -1 and temperatura_int_celsius != -1:
                    # define como ativo ou inativo
                    if temperatura_int_celsius < temperatura_ext_celsius:
                        objeto_resposta['climatizado'] = "ativo"
                    else:
                        objeto_resposta['climatizado'] = "inativo"

                # suprimindo as variaveis de temperatura e umidade informadas, como pedido na proposta.
                objeto_resposta.pop('tempExt', None)
                objeto_resposta.pop('tempInt', None)
                objeto_resposta.pop('umidade', None)

                # para verificacao de feedback do sistema
                ultima_chamada = objeto_resposta['hora']
                ultima_seq = objeto_resposta['seq']
                # comeca a esperar um feedback com o numero de seq enviado
                esperando_resposta = True

                # debugging
                print(f"enviando: {topic_response}: {objeto_resposta}")

                # publica a mensagem recebida ao canal de resposta
                client.publish(topic_response, json.dumps(objeto_resposta))

        except json.JSONDecodeError as e:
            print("recebida uma mensagem não-json.")

# inicializa o cliente mqtt
client = mqtt.Client(client_identifier)

# define as funcoes de callback
client.on_connect = on_connect
client.on_message = on_message

# conecta ao broker mqtt
client.connect(broker_adress, broker_port)

client.subscribe(topic_data)
client.subscribe(topic_feedback)
client.subscribe(topic_success)

# comeca o loop mqtt pra receber as mensagens
client.loop_forever()