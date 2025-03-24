import requests
import time
import json
import os
import datetime
import logging
from logging.handlers import RotatingFileHandler

# Configuração de logs
LOG_DIR = "logs"
if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)

# Setup do logger
log_file = os.path.join(LOG_DIR, f"integracao_{datetime.datetime.now().strftime('%Y%m%d')}.log")
logger = logging.getLogger("imoview_rd_integration")
logger.setLevel(logging.INFO)

# Handler para arquivo
file_handler = RotatingFileHandler(log_file, maxBytes=5*1024*1024, backupCount=5)
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logger.addHandler(file_handler)

# Handler para console
console_handler = logging.StreamHandler()
console_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logger.addHandler(console_handler)

# Configurações da API - Carregar de variáveis de ambiente ou usar valores padrão
IMOVIEW_API_KEY = os.environ.get("IMOVIEW_API_KEY", "52f913f76e0e9fe2f928f2fe86c2d38d")
RD_TOKEN_PUBLICO = os.environ.get("RD_TOKEN_PUBLICO", "3857b1803f2f80e63010781e6592710a")

# Cabeçalhos para Imoview
HEADERS_IMOVIEW = {
    "accept": "application/json",
    "chave": IMOVIEW_API_KEY
}

# URLs base para as APIs
IMOVIEW_BASE_URL = "https://api.imoview.com.br/Atendimento/RetornarAtendimentos"
RD_CONVERSIONS_URL = "https://api.rd.services/platform/events?event_type=conversion"
RD_LEGACY_URL = "https://www.rdstation.com.br/api/1.3/conversions"

# Mapeamento de fases do Imoview
FASE_VISITA = 4    # Visita
FASE_PROPOSTA = 5  # Proposta
FASE_VENDA = 6     # Negócio

# Mapeamento de eventos por fase
EVENTOS_CONVERSAO = {
    FASE_VISITA: "imoview-update_Visita",
    FASE_PROPOSTA: "imoview-update_Proposta",
    FASE_VENDA: "imoview-update_Venda"
}

def obter_inicio_do_dia():
    """
    Retorna a data/hora do início do dia atual no formato usado pelo Imoview (dd/mm/aaaa hh:mm)
    """
    inicio_do_dia = datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    return inicio_do_dia.strftime("%d/%m/%Y %H:%M")

def obter_dados_imoview(fase, pagina=1, registros_por_pagina=100, finalidade=2, situacao=0):
    """
    Obtém os dados da API do Imoview com os parâmetros corretos, filtrando registros do dia atual
    
    Parâmetros:
    - fase: código da fase (ex: 4=visita)
    - pagina: número da página para paginação
    - registros_por_pagina: quantidade de registros por página (aumentado para 100)
    - finalidade: código de finalidade (padrão=2)
    - situacao: código de situação (padrão=0)
    """
    # Obter data/hora do início do dia atual para filtro
    data_inicio_dia = obter_inicio_do_dia()
    
    # Construir parâmetros para a requisição
    params = {
        "numeroPagina": pagina,
        "numeroRegistros": registros_por_pagina,
        "finalidade": finalidade,
        "situacao": situacao,
        "fase": fase,
        "dataInicio": data_inicio_dia  # Filtrar registros desde o início do dia atual
    }
    
    try:
        logger.info(f"Buscando dados do Imoview para fase {fase}, página {pagina}, a partir de {data_inicio_dia}...")
        
        # Log detalhado para debug
        param_str = "&".join([f"{k}={v}" for k, v in params.items()])
        logger.info(f"URL completa: {IMOVIEW_BASE_URL}?{param_str}")
        logger.info(f"Cabeçalhos: {HEADERS_IMOVIEW}")
        
        # Fazer a requisição GET
        response = requests.get(
            IMOVIEW_BASE_URL,
            params=params,
            headers=HEADERS_IMOVIEW,
            timeout=15
        )
        
        # Log da resposta
        logger.info(f"Status da resposta: {response.status_code}")
        
        response.raise_for_status()
        
        try:
            data = response.json()
            logger.info(f"Resposta JSON recebida com sucesso. Começo: {str(data)[:100]}...")
            
            # Verifica se há mais páginas para buscar
            if isinstance(data, dict) and 'lista' in data and len(data['lista']) == registros_por_pagina:
                logger.info(f"Detectada possibilidade de mais registros. Buscando próxima página...")
                proxima_pagina = pagina + 1
                dados_proxima_pagina = obter_dados_imoview(fase, proxima_pagina, registros_por_pagina, finalidade, situacao)
                
                # Combina os resultados
                if dados_proxima_pagina and isinstance(dados_proxima_pagina, dict) and 'lista' in dados_proxima_pagina:
                    data['lista'].extend(dados_proxima_pagina['lista'])
                    logger.info(f"Combinados {len(data['lista'])} registros no total após buscar múltiplas páginas")
            
            return data
        except json.JSONDecodeError as e:
            logger.error(f"Erro ao decodificar JSON: {e}")
            logger.error(f"Resposta recebida: {response.text[:300]}...")
            return None
            
    except requests.exceptions.RequestException as e:
        logger.error(f"Erro ao acessar a API do Imoview: {e}")
        if hasattr(e, 'response') and e.response:
            logger.error(f"Detalhes do erro: {e.response.status_code} - {e.response.text[:200]}")
        return None
    except Exception as e:
        logger.error(f"Erro não esperado: {type(e).__name__}: {e}")
        return None

def enviar_evento_conversao(email, tipo_fase, midia=None, campanha=None):
    """
    Envia um evento de conversão para o RD Station
    Usa API legada diretamente para a fase VENDA
    
    Parâmetros:
    - email: email do lead
    - tipo_fase: tipo de fase (será usado para determinar o evento)
    - midia: valor para traffic_medium (opcional)
    - campanha: valor para traffic_campaign (opcional)
    """
    if tipo_fase not in EVENTOS_CONVERSAO:
        logger.error(f"Tipo de fase inválido: {tipo_fase}")
        return False
    
    evento = EVENTOS_CONVERSAO[tipo_fase]
    
    # Para o evento de VENDA, usar diretamente a API legada
    if tipo_fase == FASE_VENDA:
        return enviar_evento_legacy(email, evento, midia, campanha)
    
    # Para outros eventos, tentar a API nova primeiro
    # Preparar payload conforme documentação atualizada da API do RD Station
    payload = {
        "event_type": "CONVERSION",
        "event_family": "CDP",
        "payload": {
            "conversion_identifier": evento,
            "email": email
        }
    }
    
    # Adicionar campos opcionais se fornecidos
    if midia:
        payload["payload"]["traffic_medium"] = midia
    
    if campanha:
        payload["payload"]["traffic_campaign"] = campanha
    
    headers = {
        "accept": "application/json",
        "content-type": "application/json"
    }
    
    try:
        logger.info(f"Enviando evento {evento} para {email} via API nova...")
        
        # Log da URL para debug
        logger.info(f"URL de envio: {RD_CONVERSIONS_URL}")
        
        response = requests.post(RD_CONVERSIONS_URL, json=payload, headers=headers, timeout=15)
        
        # Log da resposta para depuração
        logger.info(f"Status: {response.status_code}")
        logger.info(f"Resposta: {response.text[:200]}...")
        
        response.raise_for_status()
        logger.info(f"Evento de conversão enviado com sucesso ({email}, {evento})")
        return True
    except requests.exceptions.RequestException as e:
        logger.warning(f"Erro ao enviar evento de conversão ({email}, {evento}): {e}")
        if hasattr(e, 'response') and e.response:
            logger.warning(f"Detalhes do erro: {e.response.status_code} - {e.response.text[:200]}")
        
        # Tentar com formato antigo
        logger.info("Tentando enviar usando o formato antigo de API...")
        return enviar_evento_legacy(email, evento, midia, campanha)
    except Exception as e:
        logger.error(f"Erro não esperado: {type(e).__name__}: {e}")
        return False

def enviar_evento_legacy(email, evento, midia=None, campanha=None):
    """
    Envia um evento usando a API legada do RD Station
    
    Parâmetros:
    - email: email do lead
    - evento: identificador do evento
    - midia: valor para traffic_medium (opcional)
    - campanha: valor para traffic_campaign (opcional)
    """
    # Payload para a API legada
    legacy_payload = {
        "token_rdstation": RD_TOKEN_PUBLICO,
        "identificador": evento,
        "email": email
    }
    
    if midia:
        legacy_payload["traffic_medium"] = midia
    
    if campanha:
        legacy_payload["traffic_campaign"] = campanha
        
    legacy_headers = {"Content-Type": "application/json"}
    
    try:
        logger.info(f"Enviando evento {evento} para {email} via API legada...")
        
        # Log da URL para debug
        logger.info(f"URL de envio legacy: {RD_LEGACY_URL}")
        
        response = requests.post(RD_LEGACY_URL, json=legacy_payload, headers=legacy_headers, timeout=15)
        
        # Log da resposta para depuração
        logger.info(f"Status: {response.status_code}")
        logger.info(f"Resposta: {response.text[:200]}...")
        
        response.raise_for_status()
        logger.info(f"Evento enviado com sucesso via API legada ({email}, {evento})")
        return True
    except requests.exceptions.RequestException as e:
        logger.warning(f"Erro ao enviar via API legada ({email}, {evento}): {e}")
        if hasattr(e, 'response') and e.response:
            logger.warning(f"Detalhes do erro: {e.response.status_code} - {e.response.text[:200]}")
        
        # Tentar alternativa com form-urlencoded (último recurso)
        try:
            logger.info("Tentando enviar como form-urlencoded...")
            headers = {"Content-Type": "application/x-www-form-urlencoded"}
            response = requests.post(RD_LEGACY_URL, data=legacy_payload, headers=headers, timeout=15)
            response.raise_for_status()
            logger.info(f"Evento enviado com sucesso após retry como form-urlencoded ({email}, {evento})")
            return True
        except requests.exceptions.RequestException as e2:
            logger.error(f"Erro na última tentativa: {e2}")
            return False
    except Exception as e:
        logger.error(f"Erro não esperado: {type(e).__name__}: {e}")
        return False

def extrair_email(negocio):
    """
    Função para extrair email de um registro do Imoview
    """
    # Verifica se há um objeto 'lead' e se ele contém email
    if 'lead' in negocio and isinstance(negocio['lead'], dict):
        email = negocio['lead'].get('email')
        if email and '@' in email:
            return email
    
    # Verifica se há email no objeto principal
    email_campos = ['email', 'emailcontato', 'email_contato', 'emailCliente']
    for campo in email_campos:
        if campo in negocio and negocio[campo] and '@' in negocio[campo]:
            return negocio[campo]
    
    # Se chegou até aqui, não encontrou email
    return None

def extrair_midia_campanha(negocio):
    """
    Extrai informações de mídia e campanha de um registro do Imoview
    
    Retorna:
    - tuple: (midia, campanha)
    """
    midia = None
    campanha = None
    
    # Tentar extrair mídia de vários campos possíveis
    midia_campos = ['midia', 'media', 'origem', 'source', 'traffic_medium']
    for campo in midia_campos:
        if campo in negocio and negocio[campo]:
            midia = negocio[campo]
            break
    
    # Tentar extrair campanha de vários campos possíveis
    campanha_campos = ['campanha', 'campaign', 'traffic_campaign']
    for campo in campanha_campos:
        if campo in negocio and negocio[campo]:
            campanha = negocio[campo]
            break
    
    return midia, campanha

def processar_dados(dados, tipo_fase):
    """
    Processa os dados recebidos da API Imoview
    
    Parâmetros:
    - dados: dados retornados pela API
    - tipo_fase: tipo de fase sendo processada (para determinar ação no RD Station)
    """
    if not dados:
        logger.info(f"Nenhum dado para processar na fase {tipo_fase}")
        return 0
    
    # Verifica o formato da resposta
    if isinstance(dados, dict):
        if 'lista' in dados:
            registros = dados['lista']
            logger.info(f"Encontrados {len(registros)} registros na propriedade 'lista'")
        else:
            logger.warning(f"Resposta em formato de dicionário, mas sem a propriedade 'lista'")
            logger.warning(f"Chaves disponíveis: {dados.keys()}")
            return 0
    elif isinstance(dados, list):
        registros = dados
        logger.info(f"Encontrados {len(registros)} registros no formato de lista")
    else:
        logger.warning(f"Formato de dados inesperado: {type(dados)}")
        return 0

    logger.info(f"Processando {len(registros)} registros da fase {tipo_fase}...")
    contador = 0
    contador_sem_email = 0
    
    for negocio in registros:
        # Extrair o email do registro
        email = extrair_email(negocio)
        
        # Se não encontrou email, ignora este registro
        if not email:
            contador_sem_email += 1
            codigo = negocio.get('codigo', 'sem_codigo')
            logger.warning(f"Registro sem email: {codigo}")
            continue
        
        # Extrair mídia e campanha do registro
        midia, campanha = extrair_midia_campanha(negocio)
        
        # Log para depuração
        logger.info(f"Processando negócio {negocio.get('codigo', 'sem_codigo')}, email: {email}, mídia: {midia}, campanha: {campanha}")
        
        # Enviar evento de conversão conforme a fase
        if enviar_evento_conversao(email, tipo_fase, midia, campanha):
            contador += 1
        
        # Pequena pausa para não sobrecarregar as APIs
        time.sleep(0.5)
    
    logger.info(f"Processamento concluído. {contador} eventos enviados com sucesso. {contador_sem_email} registros sem email.")
    return contador

def main():
    """
    Função principal que executa o processo completo
    """
    data_hoje = datetime.datetime.now().strftime("%d/%m/%Y")
    logger.info(f"Iniciando integração Imoview -> RD Station para o dia {data_hoje}...")
    logger.info(f"Buscando registros desde: {obter_inicio_do_dia()}")
    
    # Processar cada tipo de fase
    fases = [FASE_VISITA, FASE_PROPOSTA, FASE_VENDA]
    total_processado = 0
    
    for fase in fases:
        logger.info(f"\n=== Processando fase {fase} ({EVENTOS_CONVERSAO[fase]}) ===")
        
        # Obter dados da fase atual
        dados = obter_dados_imoview(fase)
        
        # Processar dados
        contador = processar_dados(dados, fase)
        total_processado += contador
        
        logger.info(f"Fase {fase}: {contador} eventos enviados")
    
    logger.info(f"\nProcessamento concluído. Total de {total_processado} eventos enviados para o dia {data_hoje}.")

if __name__ == "__main__":
    main()
