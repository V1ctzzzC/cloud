import requests
import paramiko
import pandas as pd
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
LOG_FILE = "/tmp/log_envio_api.log"  # Usar um caminho temporário
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
BLOB_NAME = "Estoque.xlsx"  # Caminho do arquivo no bucket
LOCAL_FILE = "/tmp/Estoque.xlsx"  # Caminho temporário no servidor

def carregar_credenciais():
    """Carrega o arquivo de credenciais diretamente do bucket."""
    try:
        storage_client = storage.Client(project=PROJECT_ID)
        bucket = storage_client.bucket(BUCKET_NAME)
        blob = bucket.blob(CREDENTIALS_PATH)

        credenciais_json = blob.download_as_text()
        credenciais = json.loads(credenciais_json)

        with open(CREDENTIALS_PATH, "w") as cred_file:
            json.dump(credenciais, cred_file)

        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = CREDENTIALS_PATH
        print("✅ Credenciais carregadas e configuradas.")
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
        sftp.get(remote_file_path, local_file_path)
        print(f"Arquivo baixado para {local_file_path}.")
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
        storage_client = storage.Client(project=PROJECT_ID)
        bucket = storage_client.bucket(BUCKET_NAME)
        blob = bucket.blob(BLOB_NAME)

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

def obter_access_token():
    """Obtém um novo access_token para a API do Bling."""
    # Implementar a lógica para obter o token
    return "seu_access_token_aqui"  # Substitua pela lógica real

def enviar_dados_api(resultado_df, deposito_id):
    """Envia os dados processados para a API do Bling."""
    if resultado_df.empty:
        print("Nenhum dado para enviar à API.")
        return

    # Ajustar o estoque antes de enviar
    resultado_df['balanco'] = resultado_df['balanco'].apply(ajustar_estoque)

    token = obter_access_token()  # Obter o token
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    payload = {
        "estoques": [
            {"idProduto": row["codigo_produto"], "idDeposito": deposito_id, "quantidade": row["balanco"]}
            for _, row in resultado_df.iterrows()
        ]
    }

    response = requests.post(API_URL, json=payload, headers=headers)

    if response.status_code in [200, 201]:
        print("✅ Dados enviados com sucesso à API!")
    else:
        print(f"❌ Erro ao enviar dados: {response.status_code} - {response.text}")

@app.route("/exec", methods=['POST'])
def executar_processamento():
    """Executa o processamento de dados."""
    sftp = conectar_sftp()
    if not sftp:
        return jsonify({"message": "Conexão com o SFTP falhou."}), 500

    local_file_path = FILE_TO_CHECK
    remote_file_path = f"{REMOTE_DIR}/{FILE_TO_CHECK}"
    baixar_arquivo_sftp(sftp, remote_file_path, local_file_path)
    sftp.close()

    sftp_df = ler_planilha_sftp(local_file_path)
    usuario_df = ler_planilha_usuario()

    if sftp_df is None or usuario_df is None:
        return jsonify({"message": "Erro ao ler os dados."}), 500

    resultados = buscar_correspondencias(sftp_df, usuario_df)
    enviar_dados_api(resultados, DEPOSITO_ID)

    return jsonify({"message": "Processamento executado com sucesso."})

if __name__ == "__main__":
    carregar_credenciais()  # Carregar credenciais ao iniciar
    app.run(host='0.0.0.0', port=int(os.environ.get("PORT", 8080)))  # Usar a porta do ambiente
