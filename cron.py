import time
import subprocess
import datetime
import logging
import os
import schedule

# Configuração de logs
LOG_DIR = "logs"
if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)

# Setup do logger
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(LOG_DIR, f"cron_{datetime.datetime.now().strftime('%Y%m%d')}.log")),
        logging.StreamHandler()
    ]
)

def executar_integracao():
    """Executa o script de integração Imoview-RD Station"""
    logging.info("Iniciando execução agendada da integração Imoview-RD Station")
    
    try:
        # Executar o script principal
        resultado = subprocess.run(
            ["python", "integracao_imoview_rd.py"], 
            capture_output=True, 
            text=True
        )
        
        # Log da saída
        logging.info(f"Script executado com código de saída: {resultado.returncode}")
        
        if resultado.stdout:
            logging.info(f"Saída do script: {resultado.stdout[:500]}...")
            
        if resultado.returncode != 0:
            logging.error(f"Erro na execução: {resultado.stderr}")
            
        return resultado.returncode == 0
    except Exception as e:
        logging.error(f"Erro ao executar o script: {str(e)}")
        return False

# Agendar para executar às 23:58 todos os dias
schedule.every().day.at("23:58").do(executar_integracao)

logging.info("Serviço de agendamento iniciado.")
logging.info("Próxima execução: " + str(schedule.next_run()))

# Executar uma vez imediatamente para teste
logging.info("Executando integração inicial para teste...")
executar_integracao()

# Loop principal
while True:
    schedule.run_pending()
    time.sleep(60)  # Verifica a cada minuto

# No topo do arquivo, após os imports:
PORT = os.environ.get("PORT", "8080")

# Adicione depois do loop principal
import http.server
import socketserver
import threading

def start_web_server():
    """Inicia um servidor web simples para manter o Railway feliz"""
    handler = http.server.SimpleHTTPRequestHandler
    with socketserver.TCPServer(("", int(PORT)), handler) as httpd:
        logging.info(f"Servidor web iniciado na porta {PORT}")
        httpd.serve_forever()

# Inicia o servidor web em uma thread separada
threading.Thread(target=start_web_server, daemon=True).start()
