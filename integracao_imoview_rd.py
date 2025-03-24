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

# Configurações da API - Carregar de variáveis de ambiente
IMOVIEW_API_KEY = os.environ.get("IMOVIEW_API_KEY")
RD_TOKEN_PUBLICO = os.environ.get("RD_TOKEN_PUBLICO")
# Tempo em horas para olhar para trás (padrão: 24 horas = 1 dia)
HOURS_LOOKBACK = int(os.environ.get("HOURS_LOOKBACK", 24))

# Verificar se as chaves de API estão configuradas
if not IMOVIEW_API_KEY:
    logger.error("IMOVIEW_API_KEY não configurada nas variáveis de ambiente")
    raise ValueError("IMOVIEW_API_KEY não configurada")

if not RD_TOKEN_PUBLICO:
    logger.error("RD_TOKEN_PUBLICO não configurado nas variáveis de ambiente")
    raise ValueError("RD_TOKEN_PUBLICO não configurado")

# Cabeçalhos para Imoview - com a chave de acesso fornecida pela UNIVERSAL SOFTWARE
HEADERS_IMOVIEW = {
    "accept": "application/json",
    "chave": IMOVIEW_API_KEY  # A chave de acesso deve ser enviada no cabeçalho com o nome 'chave'
}

# URL base Imoview
IMOVIEW_BASE_URL = "https://api.imoview.com.br/Atendimento/RetornarAtendimentos"

# URLs do RD Station
RD_CONVERSIONS_URL = "https://api.rd.services/platform/events?event_type=conversion"
RD_LEGACY_URL = "https://www.rdstation.com.br/api/1.3/conversions"  # URL legada

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

def testar_conexao():
    """Função para testar a conexão básica com a API Imoview"""
    try:
        # Tente uma requisição simples
        response = requests.get(
            "https://api.imoview.com.br/versao", 
            headers=HEADERS_IMOVIEW
        )
        logger.info(f"Teste de conexão: {response.status_code}")
        logger.info(f"Resposta: {response.text}")
        return response.status_code == 200
    except Exception as e:
        logger.error(f"Erro no teste de conexão: {e}")
        return False

def obter_data_anterior(horas=HOURS_LOOKBACK):
    """
    Retorna a data/hora de X horas atrás
    Por padrão, retorna a data/hora de 24 horas atrás (último dia)
    """
    data_anterior = datetime.datetime.now() - datetime.timedelta(hours=horas)
    return data_anterior

def parse_data(data_str):
    """
    Tenta converter uma string de data para um objeto datetime
    Suporta múltiplos formatos de data
    
    Parâmetros:
    - data_str: string contendo a data
    
    Retorna:
    - objeto datetime ou None se não conseguir converter
    """
    if not data_str:
        return None
        
    # Verificar o formato da data (se tem hora ou não)
    if len(data_str) > 10 and (':' in data_str or ' ' in data_str):
        # Formato com data e hora
        formats_to_try = [
            "%d/%m/%Y %H:%M:%S",
            "%d/%m/%Y %H:%M",
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d %H:%M"
        ]
    else:
        # Formato apenas com data
        formats_to_try = [
            "%d/%m/%Y",
            "%Y-%m-%d"
        ]
    
    # Tenta diferentes formatos de data
    for date_format in formats_to_try:
        try:
            return datetime.datetime.strptime(data_str, date_format)
        except ValueError:
            continue
    
    return None

def obter_dados_imoview(fase, pagina=1, registros_por_pagina=20, finalidade=2, situacao=0):
    """
    Obtém os dados da API do Imoview com os parâmetros básicos, sem filtro de data
    
    Parâmetros:
    - fase: código da fase (ex: 4=visita)
    - pagina: número da página para paginação
    - registros_por_pagina: quantidade de registros por página
    - finalidade: código de finalidade (padrão=2)
    - situacao: código de situação (padrão=0)
    """
    params = {
        "numeroPagina": pagina,
        "numeroRegistros": registros_por_pagina,
        "finalidade": finalidade,
        "situacao": situacao,
        "fase": fase,
    }
    
    try:
        logger.info(f"Buscando dados do Imoview para fase {fase}, página {pagina}...")
        logger.info(f"Parâmetros: {params}")
        
        response = requests.get(IMOVIEW_BASE_URL, params=params, headers=HEADERS_IMOVIEW)
        response.raise_for_status()
        logger.info(f"Resposta Imoview: {response.status_code}")
        
        data = response.json()
        
        # Verificar se precisamos buscar mais páginas
        total_registros = data.get('totalRegistros', 0) if isinstance(data, dict) else 0
        registros_atuais = len(data.get('lista', [])) if isinstance(data, dict) else len(data)
        
        logger.info(f"Recebidos {registros_atuais} de um total de {total_registros} registros")
        
        # Limitar a quantidade máxima de páginas para não sobrecarregar a API
        # Vamos buscar no máximo 5 páginas (100 registros com 20 por página)
        max_paginas = min(5, (total_registros + registros_por_pagina - 1) // registros_por_pagina)
        
        # Se existem mais registros e estamos na primeira página, busca algumas páginas adicionais
        if isinstance(data, dict) and 'lista' in data and pagina == 1 and max_paginas > 1:
            registros = data.get('lista', [])
            
            # Buscar páginas adicionais (a partir da página 2)
            for p in range(2, max_paginas + 1):
                logger.info(f"Buscando página adicional {p} de {max_paginas}...")
                next_page = obter_dados_imoview(fase, p, registros_por_pagina, finalidade, situacao)
                
                if isinstance(next_page, dict) and 'lista' in next_page:
                    registros.extend(next_page['lista'])
                elif isinstance(next_page, list):
                    registros.extend(next_page)
                
                # Pequena pausa para não sobrecarregar a API
                time.sleep(1)
            
            return {'lista': registros, 'totalRegistros': total_registros}
        
        return data
    except requests.exceptions.RequestException as e:
        logger.error(f"Erro ao acessar a API do Imoview: {e}")
        if hasattr(e, 'response') and e.response:
            logger.error(f"Detalhes do erro: {e.response.status_code} - {e.response.text}")
        return None
    except json.JSONDecodeError as e:
        logger.error(f"Erro ao decodificar resposta JSON: {e}")
        logger.error(f"Resposta recebida: {response.text[:200]}...")
        return None

def filtrar_registros_por_data(registros, data_corte, tipo_fase):
    """
    Filtra registros pela data específica de acordo com o tipo de fase
    Mantém apenas registros mais recentes que a data de corte
    
    Parâmetros:
    - registros: lista de registros do Imoview
    - data_corte: data/hora para filtrar registros
    - tipo_fase: tipo de fase (visita, proposta, venda)
    
    Retorna:
    - lista filtrada contendo apenas registros mais recentes que a data de corte
    """
    registros_filtrados = []
    data_corte_str = data_corte.strftime("%d/%m/%Y %H:%M")
    
    logger.info(f"Filtrando registros a partir de {data_corte_str}")
    
    for registro in registros:
        registro_valido = False
        data_registro = None
        
        # Filtro específico para cada tipo de fase
        if tipo_fase == FASE_VISITA:
            # Para Visita, verificar o campo datavisita
            if 'datavisita' in registro:
                data_registro = parse_data(registro['datavisita'])
            elif 'dataVisita' in registro:
                data_registro = parse_data(registro['dataVisita'])
                
        elif tipo_fase == FASE_PROPOSTA:
            # Para Proposta, verificar datanegociacao dentro de imoveisproposta
            if 'imoveisproposta' in registro and isinstance(registro['imoveisproposta'], list) and len(registro['imoveisproposta']) > 0:
                for proposta in registro['imoveisproposta']:
                    if 'negociacoes' in proposta and isinstance(proposta['negociacoes'], list) and len(proposta['negociacoes']) > 0:
                        for negociacao in proposta['negociacoes']:
                            if 'datanegociacao' in negociacao:
                                data_temp = parse_data(negociacao['datanegociacao'])
                                if data_temp and (data_registro is None or data_temp > data_registro):
                                    data_registro = data_temp
                                    
        elif tipo_fase == FASE_VENDA:
            # Para Venda, verificar datanegocio dentro de imoveisnegocio
            if 'imoveisnegocio' in registro and isinstance(registro['imoveisnegocio'], list) and len(registro['imoveisnegocio']) > 0:
                for negocio in registro['imoveisnegocio']:
                    if 'datanegocio' in negocio:
                        data_temp = parse_data(negocio['datanegocio'])
                        if data_temp and (data_registro is None or data_temp > data_registro):
                            data_registro = data_temp
        
        # Se não encontramos dados específicos para a fase, verificar campos genéricos
        if data_registro is None:
            # Verificar outros campos de data comuns
            data_campos = [
                'datainclusao', 'dataInclusao', 'data_inclusao',
                'dataalteracao', 'dataAlteracao', 'data_alteracao',
                'dataCadastro', 'data'
            ]
            
            for campo in data_campos:
                if campo in registro and registro[campo]:
                    data_temp = parse_data(registro[campo])
                    if data_temp:
                        data_registro = data_temp
                        break
        
        # Verificar se a data encontrada é mais recente que a data de corte
        if data_registro and data_registro >= data_corte:
            registro_valido = True
            
        if registro_valido:
            registros_filtrados.append(registro)
    
    logger.info(f"Filtro aplicado: {len(registros_filtrados)} de {len(registros)} registros mantidos")
    return registros_filtrados

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
        response = requests.post(RD_CONVERSIONS_URL, json=payload, headers=headers)
        
        # Log da resposta para depuração
        logger.debug(f"Status: {response.status_code}")
        logger.debug(f"Resposta: {response.text[:200]}...")
        
        response.raise_for_status()
        logger.info(f"Evento de conversão enviado com sucesso ({email}, {evento})")
        return True
    except requests.exceptions.RequestException as e:
        logger.warning(f"Erro ao enviar evento de conversão ({email}, {evento}): {e}")
        if hasattr(e, 'response') and e.response:
            logger.warning(f"Detalhes do erro: {e.response.status_code} - {e.response.text}")
        
        # Tentar com formato antigo
        logger.info("Tentando enviar usando o formato antigo de API...")
        return enviar_evento_legacy(email, evento, midia, campanha)

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
        response = requests.post(RD_LEGACY_URL, json=legacy_payload, headers=legacy_headers)
        
        # Log da resposta para depuração
        logger.debug(f"Status: {response.status_code}")
        logger.debug(f"Resposta: {response.text[:200]}...")
        
        response.raise_for_status()
        logger.info(f"Evento enviado com sucesso via API legada ({email}, {evento})")
        return True
    except requests.exceptions.RequestException as e:
        logger.warning(f"Erro ao enviar via API legada ({email}, {evento}): {e}")
        if hasattr(e, 'response') and e.response:
            logger.warning(f"Detalhes do erro: {e.response.status_code} - {e.response.text}")
        
        # Tentar alternativa com form-urlencoded (último recurso)
        try:
            logger.info("Tentando enviar como form-urlencoded...")
            headers = {"Content-Type": "application/x-www-form-urlencoded"}
            response = requests.post(RD_LEGACY_URL, data=legacy_payload, headers=headers)
            response.raise_for_status()
            logger.info(f"Evento enviado com sucesso após retry como form-urlencoded ({email}, {evento})")
            return True
        except requests.exceptions.RequestException as e2:
            logger.error(f"Erro na última tentativa: {e2}")
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

def processar_dados(dados, tipo_fase, data_corte):
    """
    Processa os dados recebidos da API Imoview
    
    Parâmetros:
    - dados: dados retornados pela API
    - tipo_fase: tipo de fase sendo processada (para determinar ação no RD Station)
    - data_corte: data a partir da qual considerar os registros
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
    
    # Filtrar os registros pela data específica para cada fase
    registros_filtrados = filtrar_registros_por_data(registros, data_corte, tipo_fase)
    
    logger.info(f"Processando {len(registros_filtrados)} registros da fase {tipo_fase} após filtro de data...")
    contador = 0
    contador_sem_email = 0
    
    # Rastrear emails já processados para evitar duplicatas
    emails_processados = set()
    
    for negocio in registros_filtrados:
        # Extrair o email do registro
        email = extrair_email(negocio)
        
        # Se não encontrou email, ignora este registro
        if not email:
            contador_sem_email += 1
            codigo = negocio.get('codigo', 'sem_codigo')
            logger.warning(f"Registro sem email: {codigo}")
            continue
        
        # Verificar se já processamos este email para evitar duplicatas na mesma fase
        email_key = f"{email}_{tipo_fase}"
        if email_key in emails_processados:
            logger.info(f"Email {email} já processado para fase {tipo_fase}, pulando...")
            continue
        
        # Extrair mídia e campanha do registro
        midia, campanha = extrair_midia_campanha(negocio)
        
        # Log para depuração
        logger.info(f"Processando negócio {negocio.get('codigo', 'sem_codigo')}, email: {email}, mídia: {midia}, campanha: {campanha}")
        
        # Enviar evento de conversão conforme a fase
        if enviar_evento_conversao(email, tipo_fase, midia, campanha):
            contador += 1
            emails_processados.add(email_key)
        
        # Pequena pausa para não sobrecarregar as APIs
        time.sleep(0.5)
    
    logger.info(f"Processamento concluído. {contador} eventos enviados com sucesso. {contador_sem_email} registros sem email.")
    return contador

def main():
    """
    Função principal que executa o processo completo
    """
    logger.info("Iniciando integração Imoview -> RD Station...")
    
    # Testar conexão básica com a API
    if not testar_conexao():
        logger.error("Falha no teste de conexão básica com a API Imoview")
        return
        
    # Calcular a data de corte (registros mais recentes que esta data serão processados)
    data_corte = obter_data_anterior()
    logger.info(f"Data de corte para processamento: {data_corte.strftime('%d/%m/%Y %H:%M')}")
    
    # Processar cada tipo de fase
    fases = [FASE_VISITA, FASE_PROPOSTA, FASE_VENDA]
    total_processado = 0
    
    for fase in fases:
        logger.info(f"\n=== Processando fase {fase} ({EVENTOS_CONVERSAO[fase]}) ===")
        
        # Obter dados da fase atual (sem filtro de data)
        dados = obter_dados_imoview(fase)
        
        # Processar dados com filtro de data específico para cada fase
        contador = processar_dados(dados, fase, data_corte)
        total_processado += contador
        
        logger.info(f"Fase {fase}: {contador} eventos enviados")
        
    # Adicionar envio de um evento de teste se não houver dados processados
    if total_processado == 0:
        logger.info("Nenhum registro processado. Enviando evento de teste...")
        test_email = "teste@example.com"  # Substitua por um email válido para teste
        if enviar_evento_conversao(test_email, FASE_VISITA, "teste", "teste"):
            logger.info(f"Evento de teste enviado com sucesso para {test_email}")
            total_processado += 1
    
    logger.info(f"\nProcessamento concluído. Total de {total_processado} eventos enviados.")

if __name__ == "__main__":
    main()
