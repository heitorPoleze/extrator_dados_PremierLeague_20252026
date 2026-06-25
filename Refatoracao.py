import os
import soccerdata as sd
import json
import pandas as pd 
import shutil
from pathlib import Path
from tqdm import tqdm
import matplotlib.pyplot as plt
import matplotlib.patches as patches
import seaborn as sns
import numpy as np
# ============================================================
# FUNÇÕES DE EXTRAÇÃO E LIMPEZA DOS DADOS
# ============================================================

# CONFIGURAÇÕES GLOBAIS
LIGA = "ENG-Premier League"
TEMPORADA = 2526
CSV = "dataset_refatorado.csv"

PATH_BASE = Path(r"C:\Users\heito\soccerdata\data\WhoScored")
PATH_MATCHES = PATH_BASE / "matches" / f"{LIGA}_{TEMPORADA}"
PATH_EVENTS = PATH_BASE / "events" / f"{LIGA}_{TEMPORADA}"
def salvarWebscrapeWhoScored():
    qtd_arquivos_minimos = 200

    if not PATH_MATCHES.exists():
        qtd_jsons = 0
    else: 
        arquivos_cache = list(PATH_EVENTS.glob("*.json"))
        qtd_jsons = len(arquivos_cache)

    if(qtd_jsons < qtd_arquivos_minimos):
        print("Cachê insuficiente. Começando a Extração dos dados da web")    
        wsdata = sd.WhoScored(leagues=LIGA, seasons=[TEMPORADA], headless=True)
        wsdata.read_events()

        #  Cria a subpasta de matches (e de eventos) se elas não existirem
        PATH_MATCHES.mkdir(parents=True, exist_ok=True)
        PATH_EVENTS.mkdir(parents=True, exist_ok=True)
    
        pasta_raiz_matches = PATH_BASE / "matches"
        arquivos_novos = list(pasta_raiz_matches.glob(f"{LIGA}_{TEMPORADA}*"))
        for arquivo in arquivos_novos:
            if arquivo.is_file():

                destino_final = PATH_MATCHES / arquivo.name
                shutil.move(str(arquivo), str(destino_final))
    else:
        print(f"Cachê suficiente. {qtd_jsons} arquivos encontrados. Começando extração do Json")

def extrairDataframeMatches():
    files_matches = list(PATH_MATCHES.glob("*.json"))
    lista_de_partidas = []

    for f in files_matches:
        with open(f, 'r', encoding='utf-8') as file:
            try:
                data = json.load(file)
                if data and 'tournaments' in data:
                    for torneio in data['tournaments']:
                        if 'matches' in torneio:
                            lista_de_partidas.extend(torneio['matches'])
            except json.JSONDecodeError:
                print(f"⚠️ Arquivo de match corrompido e ignorado: {f.name}")
                            
    return pd.DataFrame(lista_de_partidas)

def criarMapaTimes(df_matches):
    team_map = {}

    if df_matches.empty:
        return team_map
            
    df_matches['homeTeamId'] = pd.to_numeric(df_matches['homeTeamId'], errors='coerce')
    df_matches['awayTeamId'] = pd.to_numeric(df_matches['awayTeamId'], errors='coerce')
        
    for _, row in df_matches.dropna(subset=['homeTeamId', 'awayTeamId']).iterrows():
        team_map[int(row['homeTeamId'])] = row['homeTeamName']
        team_map[int(row['awayTeamId'])] = row['awayTeamName']
            
    return team_map

def extrairEventosEJogadores():
    files_events = list(PATH_EVENTS.glob("*.json"))
    lista_de_eventos = []
    player_map = {}

    for f in tqdm(files_events, desc="Processando Eventos JSON"):
        with open(f, 'r', encoding='utf-8') as file:
            try:
                data = json.load(file)
                if data is None:
                    continue
                        
                if 'events' in data:
                    df_temp = pd.json_normalize(data['events'])
                    df_temp['match_id'] = f.stem 
                    lista_de_eventos.append(df_temp)

                if 'playerIdNameDictionary' in data:
                    dicionario = {int(k): v for k, v in data['playerIdNameDictionary'].items() if k.isdigit()}
                    player_map.update(dicionario)
            except json.JSONDecodeError:
                print(f"⚠️ Arquivo de evento corrompido e ignorado: {f.name}")
        
    df_events = pd.concat(lista_de_eventos, ignore_index=True) if lista_de_eventos else pd.DataFrame()
    return df_events, player_map

def linkarDataframes(df_events, df_matches):
    if(df_events.empty or df_matches.empty):
        return pd.DataFrame()
    
    df_events['match_id'] = pd.to_numeric(df_events['match_id'], errors='coerce')
    df_matches['id'] = pd.to_numeric(df_matches['id'], errors='coerce')

    return pd.merge(
            df_events,
            df_matches[['id', 'homeTeamName', 'awayTeamName', 'startTime', 'homeScore', 'awayScore']],
            left_on='match_id',
            right_on='id',
            how='inner',
        )

def limparERenomearColunas(df, player_map, team_map):
    if(df.empty):
        return df

    print(f"Quantidade de eventos ANTES da limpeza: {len(df)}")

    # 1. MAPEAMENTO E DEFINIÇÃO DAS COLUNAS

    # Mapeamento dos ids para nomes
    df['playerName'] = df['playerId'].map(player_map).fillna("Jogador Desconhecido")
    df['teamName'] = df['teamId'].map(team_map).fillna("Time Desconhecido")

    # Colunas finais que serão apresentadas no df    
    colunas_finais = [
        'match_id', 'teamName', 'playerName', 'type.displayName', 
        'outcomeType.displayName', 'minute', 'second', 'x', 'endX', 'y', 'endY', 
        'isGoal', 'isOwnGoal', 'isShot', 'cardType.displayName', 'homeScore', 'awayScore', 
        'homeTeamName', 'awayTeamName', 'startTime'
    ]
    df = df[[c for c in colunas_finais if c in df.columns]].copy()
    df_antes_dos_filtros = df.copy()
    # 2. REMOÇÃO DE DADOS IRRELEVANTES OU DUPLICADOS

    # Remoção de linhas sem match_id ou sem o tipo do evento.
    colunas_criticas = [c for c in ['match_id', 'type.displayName'] if c in df.columns]
    df = df.dropna(subset=colunas_criticas).drop_duplicates()


    # 3. TRATAMENTO DE NULOS E CORREÇÃO DE TIPOS
    
    # BOOLEANOS
    colunas_booleanas = ['isGoal', 'isOwnGoal', 'isShot']
    for col in colunas_booleanas:
        if col in df.columns:
            df[col] = df[col].fillna(False).astype(bool)

    # INTEIROS
    colunas_inteiras = ['match_id', 'minute', 'second', 'homeScore', 'awayScore']
    for col in colunas_inteiras:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)

    # FLOATS
    colunas_floats = ['x', 'endX', 'y', 'endY']
    for col in colunas_floats:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')


    # 4. NORMALIZAÇÃO DE DADOS

    if 'cardType.displayName' in df.columns:
        df['cardType.displayName'] = df['cardType.displayName'].fillna("Nenhum")
    if 'outcomeType.displayName' in df.columns:
        df['outcomeType.displayName'] = df['outcomeType.displayName'].fillna("Desconhecido")

    # Remove espaços extras nas extremidades das strings
    colunas_texto = ['teamName', 'playerName', 'type.displayName', 'outcomeType.displayName', 'cardType.displayName', 'homeTeamName', 'awayTeamName']
    for col in colunas_texto:
        if col in df.columns:
            df[col] = df[col].fillna("Desconhecido").astype(str).str.strip()

    # Transforma a data em objeto Datetime
    if 'startTime' in df.columns:
        df['startTime'] = pd.to_datetime(df['startTime'], errors='coerce')

    # Força as iniciais maiúsculas
    for col in ['type.displayName', 'outcomeType.displayName', 'cardType.displayName']:
        if col in df.columns:
            df[col] = df[col].str.title()

    # 5. TRATAMENTO DE OUTLIERS

    colunas_coordenadas = ['x', 'y', 'endX', 'endY']
    for col in colunas_coordenadas:
        if col in df.columns:
            df = df[(df[col].isna()) | ((df[col] >= 0.0) & (df[col] <= 100.0))]

    if 'minute' in df.columns:
        #Contando com acréscimos bem longos
        df = df[(df['minute'] >= 0) & (df['minute'] <= 130)]
        
    if 'second' in df.columns:
        df = df[(df['second'] >= 0) & (df['second'] <= 59)]


    # 6. CRIAÇÃO DE OUTRAS COLUNAS


    if 'teamName' in df.columns and 'homeTeamName' in df.columns:
        df['isHomeTeamEvent'] = df['teamName'] == df['homeTeamName']

    if 'x' in df.columns:
        df['isAttackingHalf'] = df['x'] > 50.0

    nextEventIsGoal = (df['isGoal'].shift(-1) == True) & (df['isOwnGoal'].shift(-1) == False)
    nextEventIsSameTeam = (df['teamName'].shift(-1) == df['teamName'])
    isSameMatch = (df['match_id'].shift(-1) == df['match_id'])
    isDifferentPlayer = (df['playerName'].shift(-1) != df['playerName'])
    df['isAssistencia'] = nextEventIsGoal & nextEventIsSameTeam & isSameMatch & isDifferentPlayer
    df['golValido'] = ((df['isGoal'] == True) & (df['isOwnGoal'] == False)).astype(bool)
    # Mostra quais foram os dados descartados    
    df_descartado = df_antes_dos_filtros[~df_antes_dos_filtros.index.isin(df.index)].copy()

    print(f"Quantidade de eventos DEPOIS da limpeza: {len(df)}")
    print(f"Quantidade de eventos DESCARTADOS: {len(df_descartado)}")

    if not df_descartado.empty:
        print("EVENTOS DESCARTADOS:")
        with pd.option_context('display.max_columns', None, 'display.width', 1000):
            print(df_descartado.head())

    return df

def main():
    # 1. Salva no armazenamento os dados brutos do webscraping
    salvarWebscrapeWhoScored()

    # 2. Extrai todos os arquivos de dados básicos de partidas em um DF
    df_matches = extrairDataframeMatches()
    # 3. Cria um mapa dos times do campeonato
    team_map = criarMapaTimes(df_matches)

    # 4. Extrai todos os arquivos de eventos em um DF
    df_events, player_map = extrairEventosEJogadores()

    # 5. Linka os dois DFs
    df_sujo = linkarDataframes(df_events, df_matches)

    # 6. Limpa e renomeia as colunas
    df = limparERenomearColunas(df_sujo, player_map, team_map)

    df.to_csv(CSV, index=False, sep=';', encoding='utf-8-sig')

# ============================================================
# FUNÇÕES DE ESTATÍSTICA
# ============================================================

def diagnostico_do_csv(csv):
    try:
        df_csv = pd.read_csv(csv, sep=';', encoding='utf-8-sig', parse_dates=['startTime'])
        
        linhas, colunas = df_csv.shape
        
        print(" DIAGNÓSTICO DO ARQUIVO CSV")
        print(f"{'='*50}")
        print(f" Quantidade de linhas: {linhas:,}".replace(",", "."))
        print(f" Quantidade de colunas:   {colunas}")
        print(f"{'-'*50}")
        print(" TIPOS DE VARIÁVEIS NO ARQUIVO:")
        print(f"{'-'*50}")
        print(df_csv.dtypes.to_string())
        print(f"{'='*50}\n")
        
    except FileNotFoundError:
        print(f"\n Erro: O arquivo '{csv}' não foi encontrado no disco.")
    except Exception as e:
        print(f"\n Erro ao ler o arquivo CSV: {e}")

def gols_por_localizacao(csv):
    try:

        df = pd.read_csv(csv, sep=';', encoding='utf-8-sig')
        
        gols = df[(df['golValido'] == True)].copy()
        
        # 1. DETECÇÃO DE OUTLIERS POR IQR
        
        # Determina o valor MÁXIMO da coordenada X para o grupo dos 25% de gols mais distantes.
        # Define o limite MÍNIMO de distância a partir do qual esses 25% de gols de longe aconteceram.
        q1_x = gols['x'].quantile(0.25)
        # Indica o valor da coordenada X onde, por ordem crescente, 75% dos gols do campeonato já foram mapeados.
        q3_x = gols['x'].quantile(0.75)
        # Calcula o Intervalo Interquartílico, determinando a dispersão dos 50% dos gols mais centrais.
        # Delimita espacialmente a área padrão de finalizações que geraram gols.
        iqr_x = q3_x - q1_x
        #Definição dos gols típicos de x
        limite_inferior_x = q1_x - 1.5 * iqr_x
        
        q1_y = gols['y'].quantile(0.25)
        q3_y = gols['y'].quantile(0.75)
        iqr_y = q3_y - q1_y
        limite_inferior_y = q1_y - 1.5 * iqr_y
        limite_superior_y = q3_y + 1.5 * iqr_y
        
        df_outliers = gols[
            (gols['x'] < limite_inferior_x) |
            (gols['y'] < limite_inferior_y) | (gols['y'] > limite_superior_y)
        ].copy()

        gols_tipicos = gols[~gols.index.isin(df_outliers.index)].copy()


        # 2. CÁLCULO DAS ESTATÍSTICAS
        
        media_x = gols_tipicos['x'].mean()
        media_y = gols_tipicos['y'].mean()

        mediana_x = gols['x'].median()
        mediana_y = gols['y'].median()
        
        moda_x = gols['x'].mode().iloc[0] if not gols['x'].mode().empty else None
        moda_y = gols['y'].mode().iloc[0] if not gols['y'].mode().empty else None


    except Exception as e:
        print(f"Erro ao processar: {e}")

    metricas = {
        'media_x_limpa': media_x,
        'media_y_limpa': media_y,
        'media_x_com_outlier': gols['x'].mean(),
        'media_y_com_outlier': gols['y'].mean(),
        'mediana_x': mediana_x,
        'mediana_y': mediana_y,
        'moda_x': moda_x,
        'moda_y': moda_y
    }
    return gols_tipicos, metricas, df_outliers

def assistencias_por_localizacao(csv):
    df = pd.read_csv(csv, sep=';', encoding='utf-8-sig')

    assistencias = df[df['isAssistencia']].copy()

    #Detecção de Outliers
    q1_x = assistencias['x'].quantile(0.25)
    q3_x = assistencias['x'].quantile(0.75)
    iqr_x = q3_x - q1_x
    limite_inferior_x = q1_x - 1.5 * iqr_x

    q1_y = assistencias['y'].quantile(0.25)
    q3_y = assistencias['y'].quantile(0.75)
    iqr_y = q3_y - q1_y
    limite_inferior_y = q1_y - 1.5 * iqr_y
    limite_superior_y = q3_y + 1.5 * iqr_y

    df_outliers = assistencias[
        (assistencias['x'] < limite_inferior_x) |
        (assistencias['y'] < limite_inferior_y) | (assistencias['y'] > limite_superior_y)
    ].copy()
    assistencias_tipicas = assistencias[~assistencias.index.isin(df_outliers.index)].copy()

    return assistencias_tipicas, df_outliers


def gols_por_partida_time(csv):
    df = pd.read_csv(csv, sep=';', encoding='utf-8-sig')
        
    gols_por_jogo_por_time = df.groupby(['teamName', 'match_id'])['golValido'].sum().reset_index(name='gols_na_partida')

    estatisticas = gols_por_jogo_por_time.groupby('teamName').agg(
        qtd_gols=('gols_na_partida', 'sum'),
        media_gols=('gols_na_partida', 'mean'),
        moda_gols=('gols_na_partida', lambda x: x.mode().iloc[0] if not x.mode().empty else None),
        qtd_partidas=('match_id', 'count'),
        variancia_gols=('gols_na_partida', 'var'),
        desvio_padrao_gols=('gols_na_partida', 'std')
    ).reset_index()
    colunas_para_round = ['media_gols', 'variancia_gols', 'desvio_padrao_gols']
    estatisticas[colunas_para_round] = estatisticas[colunas_para_round].round(2)
    colunas_para_int = ['qtd_gols', 'moda_gols', 'qtd_partidas']
    estatisticas[colunas_para_int] = estatisticas[colunas_para_int].astype(int)

    estatisticas = estatisticas.sort_values(by='qtd_gols', ascending=False)

    return estatisticas, gols_por_jogo_por_time


def eficiencia_ataque_por_time(csv):
    df = pd.read_csv(csv, sep=';', encoding='utf-8-sig')

    eficiencia = df.groupby('teamName').agg(
        qtd_gols=('golValido', 'sum'),
        qtd_acoes_ataque=('isAttackingHalf', 'sum')
    ).reset_index()
    
    eficiencia['qtd_acoes_para_gol'] = np.where(
        eficiencia['qtd_gols'] > 0,
        eficiencia['qtd_acoes_ataque'] / eficiencia['qtd_gols'],
        0 
    ).round(2)

    eficiencia['taxa_de_conversao_por_qtd_acoes'] = np.where(
        eficiencia['qtd_acoes_ataque'] > 0,
        (eficiencia['qtd_gols'] / eficiencia['qtd_acoes_ataque']) * 100,
        0
    ).round(2)
    return eficiencia.sort_values(by='taxa_de_conversao_por_qtd_acoes', ascending=False)

# ============================================================
# GERAÇÃO DE GRÁFICOS
# ============================================================
def desenhar_campo_ataque(ax):
    ax.set_facecolor('#4e7a4c')
    
    # Linhas do campo
    ax.plot([50, 100], [0, 0], color="white", linewidth=2)     # Lateral Direita
    ax.plot([50, 100], [100, 100], color="white", linewidth=2) # Lateral Esquerda
    ax.plot([50, 50], [0, 100], color="white", linewidth=2)     # Linha de Meio Campo
    ax.plot([100, 100], [0, 100], color="white", linewidth=2)   # Linha de Fundo
    
    # Grande Área (Coordenadas aproximadas Opta: X de 83 a 100, Y de 21.1 a 78.9)
    grande_area = patches.Rectangle((83, 21.1), 17, 57.8, linewidth=2, edgecolor='white', facecolor='none', zorder=2)
    ax.add_patch(grande_area)
    
    # Pequena Área (X de 94.2 a 100, Y de 36.8 a 63.2)
    pequena_area = patches.Rectangle((94.2, 36.8), 5.8, 26.4, linewidth=2, edgecolor='white', facecolor='none', zorder=2)
    ax.add_patch(pequena_area)
    
    # Marca do Pênalti (X = 88.5, Y = 50)
    ax.scatter(88.5, 50, color="white", s=30, zorder=3)
    
    # Meia-lua da Grande Área
    meia_lua = patches.Arc((88.5, 50), 15, 20, theta1=130, theta2=230, color="white", linewidth=2, zorder=2)
    ax.add_patch(meia_lua)
    
    # Desenho das Traves/Rede
    ax.plot([100, 101.5], [45.2, 45.2], color="white", linewidth=2, zorder=2)
    ax.plot([100, 101.5], [54.8, 54.8], color="white", linewidth=2, zorder=2)
    ax.plot([101.5, 101.5], [45.2, 54.8], color="white", linewidth=2, zorder=2)
    
    # Limita o plano do gráfico a ficar estático independente de quantos dados são inseridos
    ax.set_xlim(48, 103)
    ax.set_ylim(-2, 102)
    

def grafico_gols_tipicos(df_gols_tipicos, metricas, df_gols_outliers):
    fig, ax = plt.subplots(figsize=(10, 7), facecolor='#f0f4f8')
    
    desenhar_campo_ataque(ax)

    sns.kdeplot(
        x=df_gols_tipicos['x'], 
        y=df_gols_tipicos['y'], 
        ax=ax,               
        fill=True,           
        cmap='Reds',         
        alpha=0.65,          
        levels=12,           
        thresh=0.05,         
        zorder=3,
        clip=((None, 100), (None, None))             
    )

    ax.scatter(
            df_gols_outliers['x'], df_gols_outliers['y'], 
            color="#E5BDBD", edgecolor="#5D5858", s=35, alpha=0.8, 
            label='Gols Outliers (Raros)', zorder=4
    )
    media_x  = metricas['media_x_limpa']
    media_y  = metricas['media_y_limpa']
    mediana_x = metricas['mediana_x']
    mediana_y = metricas['mediana_y']
    moda_x    = metricas['moda_x']
    moda_y    = metricas['moda_y']

    ax.scatter(media_x, media_y, color='#0055ff', edgecolor='white', marker='*', s=300, linewidth=1.5,
               zorder=6, label=f'Média Sem Outliers ({media_x:.1f}, {media_y:.1f})')
    
    ax.scatter(mediana_x, mediana_y, color='#ffd700', edgecolor='black', marker='D', s=130, linewidth=1.5,
               zorder=5, label=f'Mediana Total ({mediana_x:.1f}, {mediana_y:.1f})')
    
    if moda_x is not None and moda_y is not None:
        ax.scatter(moda_x, moda_y, color='#ff2222', edgecolor='white', marker='^', s=140, linewidth=1.5,
                   zorder=4, label=f'Moda Total ({moda_x:.1f}, {moda_y:.1f})')

    ax.set_title('MAPA DE CALOR DE GOLS', 
                 fontsize=13, fontweight='bold', pad=12, color='#1a1a1a')
    
    ax.legend(loc='lower left', framealpha=0.95, facecolor='white', edgecolor='none', fontsize=9)
    
    plt.tight_layout()
    plt.show()

def grafico_gols_por_time(estatisticas):
    df_plot = estatisticas.sort_values(by='qtd_gols', ascending=True)

    sns.set_theme(style="whitegrid", context='notebook')

    fig, ax = plt.subplots(figsize=(14, 8))

    sns.barplot(
        data=df_plot, 
        y='teamName', 
        x='qtd_gols', 
        color='#2e5c46', 
        ax=ax
    )

    sns.despine(left=True, bottom=True)
    
    for index, barra in enumerate(ax.patches):
        largura = barra.get_width()
        y_pos = barra.get_y() + barra.get_height() / 2
        
        media = df_plot.iloc[index]['media_gols']
        moda = df_plot.iloc[index]['moda_gols']
        desvio = df_plot.iloc[index]['desvio_padrao_gols']
        variancia = df_plot.iloc[index]['variancia_gols']
        
        texto_rotulo = f"  {int(largura)} gols  |  Média: {media:.2f} (±{desvio:.2f})  |  Moda: {moda}  |  Var: {variancia:.2f}"

        ax.text(
            largura, y_pos, 
            texto_rotulo, 
            va='center', ha='left', 
            fontsize=10, color='#1a1a1a', fontweight='500'
        )
        
    ax.set_title('EFICIÊNCIA OFENSIVA POR TIME', 
                 fontsize=15, fontweight='bold', color='#1a1a1a', loc='left', pad=20)
    
    max_gols = df_plot['qtd_gols'].max()
    ax.set_xlim(0, max_gols * 1.40)

    plt.tight_layout()
    plt.show()

def desenhar_campo_completo(ax):
    ax.set_facecolor('#4e7a4c')
    
    ax.plot([0, 100], [0, 0], color="white", linewidth=2)      # Lateral Inferior
    ax.plot([0, 100], [100, 100], color="white", linewidth=2)  # Lateral Superior
    ax.plot([0, 0], [0, 100], color="white", linewidth=2)      # Linha de Fundo (Esquerda/Defesa)
    ax.plot([100, 100], [0, 100], color="white", linewidth=2)  # Linha de Fundo (Direita/Ataque)
    ax.plot([50, 50], [0, 100], color="white", linewidth=2)    # Linha de Meio Campo
    
    circulo_central = patches.Ellipse((50, 50), width=15, height=20, edgecolor='white', facecolor='none', linewidth=2, zorder=2)
    ax.add_patch(circulo_central)
    ax.scatter(50, 50, color="white", s=30, zorder=3) # Ponto central
    
    # Grande Área
    grande_area_dir = patches.Rectangle((83, 21.1), 17, 57.8, linewidth=2, edgecolor='white', facecolor='none', zorder=2)
    ax.add_patch(grande_area_dir)
    # Pequena Área
    peq_area_dir = patches.Rectangle((94.2, 36.8), 5.8, 26.4, linewidth=2, edgecolor='white', facecolor='none', zorder=2)
    ax.add_patch(peq_area_dir)
    # Marca do Pênalti
    ax.scatter(88.5, 50, color="white", s=30, zorder=3)
    # Meia-lua
    meia_lua_dir = patches.Arc((88.5, 50), 15, 20, theta1=130, theta2=230, color="white", linewidth=2, zorder=2)
    ax.add_patch(meia_lua_dir)
    # Gol Direita
    ax.plot([100, 101.5], [45.2, 45.2], color="white", linewidth=2, zorder=2)
    ax.plot([100, 101.5], [54.8, 54.8], color="white", linewidth=2, zorder=2)
    ax.plot([101.5, 101.5], [45.2, 54.8], color="white", linewidth=2, zorder=2)

    # Grande Área
    grande_area_esq = patches.Rectangle((0, 21.1), 17, 57.8, linewidth=2, edgecolor='white', facecolor='none', zorder=2)
    ax.add_patch(grande_area_esq)
    # Pequena Área 
    peq_area_esq = patches.Rectangle((0, 36.8), 5.8, 26.4, linewidth=2, edgecolor='white', facecolor='none', zorder=2)
    ax.add_patch(peq_area_esq)
    # Marca do Pênalti
    ax.scatter(11.5, 50, color="white", s=30, zorder=3)
    # Meia-lua 
    meia_lua_esq = patches.Arc((11.5, 50), 15, 20, theta1=310, theta2=50, color="white", linewidth=2, zorder=2)
    ax.add_patch(meia_lua_esq)
    # Gol Esquerda
    ax.plot([0, -1.5], [45.2, 45.2], color="white", linewidth=2, zorder=2)
    ax.plot([0, -1.5], [54.8, 54.8], color="white", linewidth=2, zorder=2)
    ax.plot([-1.5, -1.5], [45.2, 54.8], color="white", linewidth=2, zorder=2)
    
    ax.set_xlim(-3, 103)
    ax.set_ylim(-3, 103)

def grafico_assistencias(df_assistencias_tipicas, df_assistencias_outliers):
    fig, ax = plt.subplots(figsize=(10, 7), facecolor='#f0f4f8')

    desenhar_campo_completo(ax)

    sns.kdeplot(
        x=df_assistencias_tipicas['x'], 
        y=df_assistencias_tipicas['y'], 
        ax=ax,               
        fill=True,           
        cmap='Reds',         
        alpha=0.65,          
        levels=12,           
        thresh=0.05,         
        zorder=3,
        clip=((None, 100), (0, 100))             
    )

    ax.scatter(
            df_assistencias_outliers['x'], df_assistencias_outliers['y'], 
            color="#E5BDBD", edgecolor="#5D5858", s=35, alpha=0.8, 
            label='Assistências Outliers', zorder=5
    )
    
    ax.set_title('MAPA DE CALOR DE ASSISTÊNCIAS', 
                 fontsize=13, fontweight='bold', pad=12, color='#1a1a1a')
    
    ax.legend(loc='lower left', framealpha=0.95, facecolor='white', edgecolor='none', fontsize=9)
    
    plt.tight_layout()
    plt.show()

def grafico_eficiencia_ataque(df_eficiencia):
    
    sns.set_theme(style="whitegrid", context="notebook")
    fig, ax = plt.subplots(figsize=(13, 9), facecolor='#f0f4f8')
    
    # Média do Campeonato para traçar as linhas divisórias
    media_acoes = df_eficiencia['qtd_acoes_ataque'].mean()
    media_conversao = df_eficiencia['taxa_de_conversao_por_qtd_acoes'].mean()

    sns.scatterplot(
        data=df_eficiencia, 
        x='qtd_acoes_ataque', 
        y='taxa_de_conversao_por_qtd_acoes', 
        color='#2e5c46', 
        s=120,          
        edgecolor='black',
        alpha=0.8,
        ax=ax
    )

    ax.axvline(media_acoes, color='#e74c3c', linestyle='--', linewidth=1.5, alpha=0.7)
    ax.axhline(media_conversao, color='#e74c3c', linestyle='--', linewidth=1.5, alpha=0.7)

    for _, row in df_eficiencia.iterrows():
         ax.text(
            row['qtd_acoes_ataque'] + (df_eficiencia['qtd_acoes_ataque'].max() * 0.01), 
            row['taxa_de_conversao_por_qtd_acoes'], 
            row['teamName'], 
            fontsize=9, color='#1a1a1a', fontweight='500', va='center'
        )

    props_caixa = dict(boxstyle='round', facecolor='white', alpha=0.8, edgecolor='none')
    
    ax.text(0.99, 0.96, 'DOMINANTES', 
            transform=ax.transAxes, fontsize=13, ha='right', va='top', color='#27ae60', fontweight='bold', bbox=props_caixa)
    ax.text(0.01, 0.96, 'LETAIS', 
            transform=ax.transAxes, fontsize=13, ha='left', va='top', color='#2980b9', fontweight='bold', bbox=props_caixa)
    ax.text(0.01, 0.08, 'INOFENSIVOS', 
            transform=ax.transAxes, fontsize=13, ha='left', va='bottom', color='#7f8c8d', fontweight='bold', bbox=props_caixa)
    ax.text(0.99, 0.04, 'INSISTENTES', 
            transform=ax.transAxes, fontsize=13, ha='right', va='bottom', color='#c0392b', fontweight='bold', bbox=props_caixa)

    ax.set_title('MAPA DE EFICIÊNCIA OFENSIVA\nCruzamento entre total de ações no ataque e conversão em gols', 
                 fontsize=15, fontweight='bold', color='#1a1a1a', loc='left', pad=20)
    
    ax.set_xlabel('Total de Ações no Campo de Ataque', fontsize=11, fontweight='bold', labelpad=10)
    ax.set_ylabel('Taxa de Conversão (%)', fontsize=11, fontweight='bold', labelpad=10)

    ax.set_facecolor('#f0f4f8')
    sns.despine(left=True, bottom=True)

    plt.tight_layout()
    plt.show()

def grafico_boxplot_gols_por_time(estatisticas, gols_por_jogo_por_time):
    
    ordem_times = estatisticas['teamName'].tolist()
    sns.set_theme(style="whitegrid", context="notebook")
    fig, ax = plt.subplots(figsize=(14, 8))

    propriedades_outliers = dict(
        marker='d',               
        markerfacecolor='#c0392b', 
        markersize=9,            
        markeredgecolor='black',  
        alpha=0.8                 
    )
    
    propriedades_media = dict(
        marker='o',
        markerfacecolor='white',
        markeredgecolor='black',
        markersize=7,
        zorder=3 
    )
    
    sns.boxplot(
        data=gols_por_jogo_por_time, 
        x='teamName', 
        y='gols_na_partida', 
        order=ordem_times,     
        color='#2e5c46', 
        width=0.5,             
        boxprops=dict(alpha=0.8),
        flierprops=propriedades_outliers, 
        showmeans=True,                     # <--- NOVO: Ativa o desenho da média
        meanprops=propriedades_media,       
        ax=ax
    )
    
    sns.despine(top=True, right=True)
    plt.xticks(rotation=45, ha='right', fontsize=10, fontweight='500')
    
    max_gols_geral = gols_por_jogo_por_time['gols_na_partida'].max()

    for index, row in estatisticas.reset_index(drop=True).iterrows():
        media = row['media_gols']
        texto_rotulo = f"x̄: {media:.1f}"
        
        ax.text(
            index, max_gols_geral + 0.8,
            texto_rotulo, 
            va='bottom', ha='center', 
            fontsize=10, color='#1a1a1a', fontweight='bold'
        )

    ax.set_title('DISTRIBUIÇÃO OFENSIVA: PADRÃO, MÉDIAS E ANOMALIAS (OUTLIERS)', 
                 fontsize=15, fontweight='bold', color='#1a1a1a', loc='left', pad=20)
    
    ax.set_ylim(0, max_gols_geral + 1.5) 
    
    ax.set_ylabel('Gols marcados em uma única partida', fontsize=11, fontweight='bold', labelpad=12)
    ax.set_xlabel('') 

    plt.tight_layout()
    plt.show()

def grafico_histograma_assistencias(assistencias_tipicas):
    sns.set_theme(style="whitegrid", context="notebook") 
    
    grafico = sns.jointplot(
        data=assistencias_tipicas,
        x='y', 
        y='x', 
        kind='hist',
        binwidth=5,                               
        binrange=((0, 100), (50, 100)),           
        cmap='YlGn',         
        height=9,            
        ratio=5,             
        space=0,                                  
        marginal_kws=dict(binwidth=5, color='#2e5c46', alpha=0.8) 
    )
    
    # Eixos do Campo
    grafico.ax_joint.set_xlabel('Largura do Campo (metros)', fontsize=11, fontweight='bold', labelpad=10)
    grafico.ax_joint.set_ylabel('Profundidade (Rumo ao Gol no topo)', fontsize=11, fontweight='bold', labelpad=10)
    grafico.ax_joint.set_xlim(0, 100)
    grafico.ax_joint.set_ylim(50, 100)
    
    grafico.ax_joint.set_xticks(range(0, 101, 5))
    grafico.ax_joint.set_yticks(range(50, 101, 5))
    grafico.ax_joint.grid(True, linestyle='--', alpha=0.4)
    grafico.ax_joint.tick_params(labelsize=9)


    grafico.ax_marg_x.tick_params(axis='y', left=True, labelleft=True, labelsize=8)
    grafico.ax_marg_x.set_ylabel('Qtd', fontsize=9, fontweight='bold')
    grafico.ax_marg_x.set_title('Volume de Assistências por Corredor do Campo', fontsize=10, fontweight='bold', color='#2e5c46', pad=10)
    grafico.ax_marg_x.grid(True, axis='y', linestyle=':', alpha=0.6)

    grafico.ax_marg_y.tick_params(axis='x', bottom=True, labelbottom=True, labelsize=8)
    grafico.ax_marg_y.set_xlabel('Qtd', fontsize=9, fontweight='bold')
    grafico.ax_marg_y.set_ylabel('Volume de Assistências por Profundidade', fontsize=10, fontweight='bold', color='#2e5c46', rotation=270, labelpad=20)
    grafico.ax_marg_y.yaxis.set_label_position("right")
    grafico.ax_marg_y.grid(True, axis='x', linestyle=':', alpha=0.6)

    limites_x = range(0, 105, 5) 
    limites_y = range(50, 105, 5) 

    # 1. matriz_quantidades: Uma matriz dizendo quantas assistências caíram em cada quadrado.
    # 2. bordas_x: Uma lista com as linhas verticais do campo 
    # 3. bordas_y: Uma lista com as linhas horizontais do campo 
    matriz_quantidades, bordas_x, bordas_y = np.histogram2d(
        assistencias_tipicas['y'], 
        assistencias_tipicas['x'], 
        bins=[limites_x, limites_y]
    )
    
    maximo_absoluto = matriz_quantidades.max()
    # O primeiro 'for' anda pelas colunas (largura do campo).
    # O segundo 'for' anda pelas linhas (profundidade do campo).
    for i in range(len(bordas_x) - 1):
        for j in range(len(bordas_y) - 1):
            #número exato de assistências desse quadrado
            quantidade = int(matriz_quantidades[i, j])
            
            if quantidade > 0:
                centro_x = bordas_x[i] + 2.5
                centro_y = bordas_y[j] + 2.5
                
                cor_texto = 'white' if quantidade > (maximo_absoluto * 0.5) else '#1a1a1a'
                
                grafico.ax_joint.text(
                    centro_x, centro_y, 
                    str(quantidade),
                    ha='center', va='center', 
                    fontsize=9, fontweight='bold', color=cor_texto
                )

    grafico.figure.suptitle('ZONAS DE ASSISTÊNCIA: VISUALIZAÇÃO TÁTICA COMPLETA', 
                             fontsize=16, fontweight='bold', color='#1a1a1a', y=0.98)
    
    # impede o título de desaparecer
    grafico.figure.subplots_adjust(top=0.88) 
    
    plt.show()

gols_tipicos, metricas, outliers = gols_por_localizacao(CSV)
grafico_gols_tipicos(gols_tipicos, metricas, outliers)

assistencias_tipicas, outliers = assistencias_por_localizacao(CSV)
grafico_assistencias(assistencias_tipicas, outliers)
grafico_histograma_assistencias(assistencias_tipicas)

estatisticas, gols = gols_por_partida_time(CSV)
grafico_gols_por_time(estatisticas)
grafico_boxplot_gols_por_time(estatisticas, gols)

eficiencia = eficiencia_ataque_por_time(CSV)
grafico_eficiencia_ataque(eficiencia)