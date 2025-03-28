import requests
import paramiko
import pandas as pd
from tkinter import Tk
from tkinter.filedialog import askopenfilename
import os
import json
import psutil
import time
from google.cloud import storage
import datetime
import pytz
import smtplib
import logging
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from flask import Flask, jsonify, Response
import os
import logging
from datetime import datetime
# Configurações do bucket e do arquivo de credenciais
BUCKET_NAME = "apibling"  # Nome do bucket no Google Cloud
CREDENTIALS_PATH = "key.json"  # Nome do arquivo de credenciais no bucket

app = Flask(__name__)



# Configurações do SFTP
SFTP_HOST = 'sftp.marchon.com.br'
SFTP_PORT = 2221
SFTP_USERNAME = 'CompreOculos'
SFTP_PASSWORD = '@CMPCLS$2023'
REMOTE_DIR = 'COMPREOCULOS/ESTOQUE'
FILE_TO_CHECK = 'estoque_disponivel.csv'

# Configuração da API
API_URL = 'https://api.bling.com.br/Api/v3/estoques'
LOG_FILE = "log_envio_api.log"
# Configuração do log
logging.basicConfig(filename=LOG_FILE, level=logging.INFO, format="%(asctime)s - %(message)s")

# Configuração de autenticação na API do Bling
TOKEN_FILE = "token_novo.json"
BLING_AUTH_URL = "https://api.bling.com.br/Api/v3/oauth/token"
BASIC_AUTH = ("19f357c5eccab671fe86c94834befff9b30c3cea", "0cf843f8d474ebcb3f398df79077b161edbc6138bcd88ade942e1722303a")

# Definição do ID do depósito
DEPOSITO_ID = 14888163276  # Substitua pelo ID do depósito desejado

# Defina manualmente o ID do projeto
PROJECT_ID = "api-bling-450013"
BUCKET_NAME = "apibling"  # Nome do bucket no Google Cloud
BLOB_NAME = "Estoque.xlsx"  # Caminho do arquivo no bucket
LOCAL_FILE = os.path.join(os.getcwd(), "estoque.xlsx")


app = Flask(__name__)

def carregar_credenciais():
    """Carrega o arquivo de credenciais diretamente do bucket."""
    try:
        # Inicializa o cliente de armazenamento sem credenciais ainda
        storage_client = storage.Client(project="api-bling-450013")
        bucket = storage_client.bucket(BUCKET_NAME)
        blob = bucket.blob(CREDENTIALS_PATH)

        # Lê o conteúdo do blob em memória
        credenciais_json = blob.download_as_text()
        credenciais = json.loads(credenciais_json)

        # Salva as credenciais como um arquivo temporário para uso pelo cliente
        with open(CREDENTIALS_PATH, "w") as cred_file:
            json.dump(credenciais, cred_file)

        # Configura a variável de ambiente para o Google Cloud
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = CREDENTIALS_PATH
        print(f"✅ Credenciais carregadas e configuradas.")
    except Exception as e:
        print(f"⚠ Erro ao carregar credenciais do bucket: {e}")

# Carrega as credenciais
carregar_credenciais()
# Definição do ID do projeto
PROJECT_ID = "api-bling-450013"  # Substitua pelo seu ID de projeto

def carregar_credenciais():
    """Carrega o arquivo de credenciais diretamente do bucket."""
    try:
        storage_client = storage.Client(project="api-bling-450013")
        bucket = storage_client.bucket(BUCKET_NAME)
        blob = bucket.blob(CREDENTIALS_PATH)

        # Lê o conteúdo do blob em memória
        credenciais_json = blob.download_as_text()
        credenciais = json.loads(credenciais_json)

        # Salva as credenciais como um arquivo temporário para uso pelo cliente
        with open(CREDENTIALS_PATH, "w") as cred_file:
            json.dump(credenciais, cred_file)

        # Configura a variável de ambiente para o Google Cloud
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = CREDENTIALS_PATH
        print(f"✅ Credenciais carregadas e configuradas.")
    except Exception as e:
        print(f"⚠ Erro ao carregar credenciais do bucket: {e}")

def registrar_log(mensagem):
    """Registra mensagens no arquivo de log e imprime na saída."""
    logging.info(mensagem)
    print(mensagem)

@app.route("/")
def home():
    return "<h2>🛠 API de Atualização de Estoque Bling</h2><p>Use <code>/logs</code> para ver os logs em tempo real.</p>"

@app.route("/logs")
def stream_logs():
    """Exibe os logs do processo diretamente no navegador."""
    def generate():
        if os.path.exists(LOG_FILE):
            with open(LOG_FILE, "r", encoding="utf-8") as file:
                for line in file:
                    yield line + "<br>"
        else:
            yield "Nenhum log encontrado.<br>"

    return Response(generate(), mimetype="text/html")

def executar_processamento():
    """Executa o envio de estoque e registra logs detalhados."""
    registrar_log("Iniciando o processamento...")

    # Exemplo de produtos para envio
    produtos = [
        {"id": 101, "nome": "Produto A", "quantidade": 50},
        {"id": 102, "nome": "Produto B", "quantidade": 30}
    ]

    for produto in produtos:
        registrar_log(f"Enviando produto: {produto['nome']} - Quantidade: {produto['quantidade']}")

    registrar_log("Processamento concluído!")

def log_envio(mensagem):
    """Salva logs localmente, imprime na tela e envia para o bucket."""
    brt_tz = pytz.timezone('America/Sao_Paulo')
    data_hora = datetime.now(brt_tz).strftime("%Y-%m-%d %H:%M:%S")

    log_mensagem = f"[{data_hora}] {mensagem}"

    # Salvar no arquivo local
    with open(LOG_FILE, "a", encoding="utf-8") as log:
        log.write(log_mensagem + "\n")

    # Exibir no terminal (para ver no Cloud Shell)
    print(log_mensagem)

    # Enviar para o bucket
    enviar_log_para_bucket()

def enviar_log_para_bucket():
    """Envia o arquivo de log para o Google Cloud Storage."""
    try:
        storage_client = storage.Client(project="api-bling-450013")
        bucket = storage_client.bucket(BUCKET_NAME)
        blob = bucket.blob(f"logs/{LOG_FILE}")  # Salva dentro da pasta "logs"

        blob.upload_from_filename(LOG_FILE)
        print(f"✅ Log enviado para {BUCKET_NAME}/logs/{LOG_FILE}")
    except Exception as e:
        print(f"⚠ Erro ao enviar log para o bucket: {e}")

def conectar_sftp():
    """Conecta ao servidor SFTP e retorna uma sessão."""
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    try:
        print("Conectando ao servidor SFTP...")
        client.connect(SFTP_HOST, SFTP_PORT, SFTP_USERNAME, SFTP_PASSWORD)
        return client.open_sftp()
    except Exception as e:
        print(f"Erro ao conectar ao servidor SFTP: {e}")
        return None
def baixar_arquivo_sftp(sftp, remote_file_path, local_file_path):
    """Baixa um arquivo do SFTP para a máquina local."""
    try:
        print(f"Baixando o arquivo {remote_file_path}...")
        start_time = time.time()
        sftp.get(remote_file_path, local_file_path)
        end_time = time.time()
        download_time = end_time - start_time
        print(f"Arquivo baixado para {local_file_path} em {download_time:.2f} segundos.")
    except Exception as e:
        print(f"Erro ao baixar o arquivo: {e}")

def ler_planilha_sftp(caminho_arquivo):
    """Lê e processa o arquivo CSV baixado do SFTP."""
    try:
        sftp_df = pd.read_csv(caminho_arquivo)
        print(f"Arquivo do SFTP carregado com {sftp_df.shape[0]} linhas.")
        sftp_df[['codigo_produto', 'balanco']] = sftp_df.iloc[:, 0].str.split(';', expand=True)
        sftp_df['balanco'] = sftp_df['balanco'].astype(float)
        return sftp_df[['codigo_produto', 'balanco']]
    except Exception as e:
        print(f"Erro ao ler a planilha do SFTP: {e}")
        return None

def baixar_planilha():
    """Baixa a planilha estoque.xlsx do bucket do Google Cloud Storage."""
    try:
        storage_client = storage.Client(project="api-bling-450013")
        bucket = storage_client.bucket(BUCKET_NAME)
        blob = bucket.blob(BLOB_NAME)

        # Faz o download do arquivo para o caminho local
        blob.download_to_filename(LOCAL_FILE)
        print(f"✅ Planilha {BLOB_NAME} baixada com sucesso!")
        return LOCAL_FILE
    except Exception as e:
        print(f"❌ Erro ao baixar {BLOB_NAME} do bucket: {e}")
        return None


def ler_planilha_usuario():
    """Lê os dados da planilha estoque.xlsx baixada do bucket."""
    caminho_planilha = baixar_planilha()
    
    if not caminho_planilha or not os.path.exists(caminho_planilha):
        print("⚠ Erro: A planilha não pôde ser baixada ou não foi encontrada.")
        return None

    try:
        df = pd.read_excel(caminho_planilha)
        if df.shape[1] < 3:
            raise ValueError("A planilha deve conter pelo menos 3 colunas.")

        return pd.DataFrame({
            "id_usuario": df.iloc[:, 1].astype(str).str.strip(),
            "codigo_produto": df.iloc[:, 2].astype(str).str.strip()
        })
    except Exception as e:
        print(f"❌ Erro ao ler a planilha {caminho_planilha}: {e}")
        return None


def buscar_correspondencias(sftp_df, usuario_df):
    """Faz a correspondência entre os produtos do usuário e os do SFTP."""
    if sftp_df is None or usuario_df is None:
        print("Erro: Arquivos de entrada não carregados corretamente.")
        return pd.DataFrame()

    resultado = usuario_df.merge(sftp_df, on="codigo_produto", how="left")
    return resultado

def ajustar_estoque(valor):
    """Ajusta o valor do estoque, subtraindo 10, garantindo que não fique negativo."""
    return max(0, valor - 10)

def baixar_token():
    """Baixa o token_novo.json do bucket do Google Cloud."""
    storage_client = storage.Client(project="api-bling-450013")
    bucket = storage_client.bucket(BUCKET_NAME)
    blob = bucket.blob(TOKEN_FILE)

    try:
        conteudo = blob.download_as_text()
        return json.loads(conteudo)
    except Exception as e:
        print(f"⚠ Erro ao baixar token do bucket: {e}")
        return None

def salvar_token(dados):
    """Salva o token_novo.json atualizado no bucket."""
    storage_client = storage.Client(project="api-bling-450013")
    bucket = storage_client.bucket(BUCKET_NAME)
    blob = bucket.blob(TOKEN_FILE)

    try:
        blob.upload_from_string(json.dumps(dados, indent=4))
        print("✅ Token atualizado e salvo no bucket.")
    except Exception as e:
        print(f"❌ Erro ao salvar token no bucket: {e}")

def obter_refresh_token():
    """Obtém o refresh_token do arquivo JSON baixado."""
    data = baixar_token()
    return data.get("refresh_token") if data else None

def gerar_novo_token():
    """Gera um novo access_token e salva no bucket."""
    refresh_token = obter_refresh_token()
    if not refresh_token:
        raise ValueError("⚠ Refresh token não encontrado. Faça login manualmente para gerar um novo.")

    payload = {
        "grant_type": "refresh_token",
        "refresh_token": refresh_token
    }

    response = requests.post(BLING_AUTH_URL, auth=BASIC_AUTH, data=payload)

    if response.status_code in [200, 201]:
        novo_token = response.json()
        salvar_token(novo_token)  # Salva o novo token no bucket
        print("✅ Novo access_token gerado com sucesso!")
        return novo_token["access_token"]
    else:
        raise Exception(f"❌ Erro ao gerar novo token: {response.status_code} - {response.text}")

def obter_access_token():
    """Sempre gera um novo access_token antes de cada execução"""
    return gerar_novo_token()

# ---------------------------------------------
# SUBSTITUI A SOLICITAÇÃO DO TOKEN PELO GERADOR AUTOMÁTICO
# ---------------------------------------------

def salvar_planilha_envio(resultado_df):
    """Gera e salva uma planilha com os dados enviados à API no bucket."""
    if resultado_df.empty:
        print("Nenhum dado para salvar na planilha.")
        return None

    caminho_planilha_envio = "/tmp/estoque_enviado.xlsx"

    try:
        # Criar a planilha
        resultado_df.to_excel(caminho_planilha_envio, index=False)

        # Enviar para o bucket
        storage_client = storage.Client(project="api-bling-450013")
        bucket = storage_client.bucket(BUCKET_NAME)
        blob = bucket.blob("envios/estoque_enviado.xlsx")

        blob.upload_from_filename(caminho_planilha_envio)
        print(f"✅ Planilha de envio salva em {BUCKET_NAME}/envios/estoque_enviado.xlsx")

        return caminho_planilha_envio
    except Exception as e:
        print(f"❌ Erro ao salvar planilha de envio: {e}")
        return None

def enviar_dados_api(resultado_df, deposito_id):
    """Envia os dados processados para a API do Bling e salva a planilha no bucket."""
    if resultado_df.empty:
        print("Nenhum dado para enviar à API.")
        return

    # Ajustar o estoque antes de enviar
    resultado_df['balanco'] = resultado_df['balanco'].apply(lambda x: max(0, x - 10))

    token = obter_access_token()  # 🔥 Agora o token é gerado automaticamente!
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    session = requests.Session()  # Inicializando a sessão aqui
    session.headers.update(headers)

    payload = {
        "estoques": [
            {"idProduto": row["codigo_produto"], "idDeposito": deposito_id, "quantidade": row["balanco"]}
            for _, row in resultado_df.iterrows()
        ]
    }

    response = session.post(API_URL, json=payload)

    if response.status_code in [200, 201]:
        print("✅ Dados enviados com sucesso à API!")

        # Salvar e enviar a planilha de envio ao bucket
        salvar_planilha_envio(resultado_df)
    else:
        print(f"❌ Erro ao enviar dados: {response.status_code} - {response.text}")

    # Continue com o restante do código para envio e log


    log_envio("\n🔍 Iniciando envio de dados para a API...\n")
    # Contador de envios bem-sucedidos
    contador_envios = 0
    total_bytes_enviados = 0
    start_time = time.time()

    for _, row in resultado_df.iterrows():
        if pd.notna(row["balanco"]) and pd.notna(row["id_usuario"]):
            payload = {
                "produto": {
                    "id": int(row["id_usuario"]),
                    "codigo": row["codigo_produto"]
                },
                "deposito": {
                    "id": deposito_id
                },
                "operacao": "B",
                "preco": 100,
                "custo": 10,
                "quantidade": row["balanco"],
                "observacoes": "Atualização de estoque via script"
            }
            try:
                # Verifica se o balanço é maior que zero antes de enviar
                if row["balanco"] > 0:
                    send_start_time = time.time()  # Início do envio
                    response = session.post(API_URL, json=payload)
                    send_end_time = time.time()  # Fim do envio
                    total_bytes_enviados += len(json.dumps(payload).encode('utf-8'))
                    
                    log_msg = f"\n📦 Enviado para API:\n{json.dumps(payload, indent=2)}"
                    
                    if response.status_code in [200, 201]:
                        log_envio(f"✔ Sucesso [{response.status_code}]: Produto {row['codigo_produto']} atualizado na API.{log_msg}")
                        contador_envios += 1  # Incrementa o contador de envios
                    else:
                        log_envio(f"❌ Erro [{response.status_code}]: {response.text}{log_msg}")
                    # Calcular o tempo de resposta do servidor
                    response_time = send_end_time - send_start_time
                    log_envio(f"⏱ Tempo de resposta do servidor para {row['codigo_produto']}: {response_time:.2f} segundos")
                else:
                    log_envio(f"⚠ Produto {row['codigo_produto']} não enviado, balanço igual a zero.")

            except Exception as e:
                log_envio(f"❌ Erro ao enviar {row['codigo_produto']}: {e}")

    end_time = time.time()
    total_time = end_time - start_time
    upload_speed = total_bytes_enviados / total_time if total_time > 0 else 0
    cpu_usage = psutil.cpu_percent(interval=1)

    # Log do total de envios
    log_envio(f"\n✅ Envio finalizado! Total de IDs enviados: {contador_envios}")
    log_envio(f"⏱ Tempo total de envio: {total_time:.2f} segundos")
    log_envio(f"📊 Velocidade de upload: {upload_speed / 1024:.2f} KB/s")
    log_envio(f"🖥 Uso de CPU: {cpu_usage}%")

def salvar_planilha_resultado(resultado_df, nome_arquivo="resultado_correspondencias.xlsx"):
    """Salva os resultados da correspondência localmente e no bucket do Google Cloud."""
    try:
        resultado_df.to_excel(nome_arquivo, index=False)
        print(f"Resultados salvos em {os.path.abspath(nome_arquivo)}")

        # Enviar para o bucket
        upload_to_bucket("apibling", nome_arquivo, f"resultados/{nome_arquivo}")
    except Exception as e:
        print(f"Erro ao salvar os resultados: {e}")

# Agora, você pode usar o cliente de armazenamento
def upload_to_bucket(bucket_name, source_file_name, destination_blob_name):
    """Faz o upload de um arquivo para o bucket do Google Cloud Storage."""
    try:
        storage_client = storage.Client(project="api-bling-450013") # Crie o cliente depois de carregar as credenciais
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_filename(source_file_name)
        print(f"📂 Arquivo {source_file_name} enviado para {bucket_name} como {destination_blob_name}.")
    except Exception as e:
        print(f"❌ Erro ao salvar {source_file_name} no bucket: {e}")

# Crie o cliente de armazenamento aqui, após carregar as credenciais
storage_client = storage.Client(project=PROJECT_ID)

def main():
        # Crie o cliente de armazenamento aqui, após carregar as credenciais
    #storage_client = storage.Client()

    sftp = conectar_sftp()
    if not sftp:
        print("Conexão com o SFTP falhou. Finalizando o script.")
        return

    local_file_path = FILE_TO_CHECK
    remote_file_path = f"{REMOTE_DIR}/{FILE_TO_CHECK}"
    baixar_arquivo_sftp(sftp, remote_file_path, local_file_path)
    sftp.close()

    sftp_df = ler_planilha_sftp(local_file_path)
    usuario_df = ler_planilha_usuario()

    if sftp_df is None or usuario_df is None:
        return

    resultados = buscar_correspondencias(sftp_df, usuario_df)
    salvar_planilha_resultado(resultados)
# Usar o DEPOSITO_ID definido no início
    enviar_dados_api(resultados, DEPOSITO_ID)

# Enviar o e-mail com o relatório após o envio dos dados
    enviar_email_com_anexo(
        "victor@compreoculos.com.br",
        "Relatório de Estoque",
        "Segue em anexo o relatório atualizado.",
        "resultado_correspondencias.xlsx"  # O arquivo que você gerou anteriormente
    )



def enviar_email_com_anexo(destinatario, assunto, mensagem, anexo_path):
    """Envia um e-mail com um arquivo anexo."""
    remetente = "victor@compreoculos.com.br"  # Altere para seu e-mail
    senha = "Compre2024"  # Use um App Password ou método seguro para armazenar credenciais

    msg = MIMEMultipart()
    msg["From"] = remetente
    msg["To"] = destinatario
    msg["Subject"] = assunto

    msg.attach(MIMEText(mensagem, "plain"))

    # Anexar arquivo
    if os.path.exists(anexo_path):
        with open(anexo_path, "rb") as anexo:
            parte = MIMEBase("application", "octet-stream")
            parte.set_payload(anexo.read())
            encoders.encode_base64(parte)
            parte.add_header("Content-Disposition", f"attachment; filename={os.path.basename(anexo_path)}")
            msg.attach(parte)
    else:
        print(f"⚠ Arquivo {anexo_path} não encontrado para anexo.")

    try:
        servidor = smtplib.SMTP("smtp.gmail.com", 587)
        servidor.starttls()
        servidor.login(remetente, senha)
        servidor.sendmail(remetente, destinatario, msg.as_string())
        servidor.quit()
        print(f"📧 E-mail enviado com sucesso para {destinatario}")
    except Exception as e:
        print(f"❌ Erro ao enviar e-mail: {e}")



if __name__ == "__main__":
    main()
