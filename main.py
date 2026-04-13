##script para pegar o webscrapping via soccerdata para o site WhoScored 

import soccerdata as sd
import json
import os
import pandas as pd
from glob import glob
from tqdm import tqdm

PATH_BASE = r"C:\Users\heito\soccerdata\data\WhoScored"
PATH_MATCHES = os.path.join(PATH_BASE, "matches")
PATH_EVENTS = os.path.join(PATH_BASE, "events", "ENG-Premier League_2526")

#Salva na pasta "C:\Users\heito\soccerdata\" os jsons extraidos no website do Whoscored
def salvarWebscrapeWhoScored():
    wsdata = sd.WhoScored(leagues='ENG-Premier League', seasons=[2526], headless=True)
    wsdata.read_events()

#Pega os jsons salvos e transforma-os em tabelas
def extrairJsonPraTabelas():
    files_matches = glob(os.path.join(PATH_MATCHES, "**", "*.json"), recursive=True)
    lista_de_partidas = []

    for f in files_matches:
        with open(f, 'r', encoding='utf-8') as file:
            data = json.load(file)
            if data and 'tournaments' in data:
                for torneio in data['tournaments']:
                    if 'matches' in torneio:
                        lista_de_partidas.extend(torneio['matches'])
    df_matches = pd.DataFrame(lista_de_partidas)
  
    files_events = glob(os.path.join(PATH_EVENTS, "*.json"))
    lista_de_eventos = []
    player_map = {}

    for f in tqdm(files_events):
        with open(f, 'r', encoding='utf-8') as file:
            data = json.load(file)
            if data is not None:
                if 'events' in data:
                    df_temp = pd.json_normalize(data['events'])
                    #transforma o id da partida no nome do arquivo de json, para os eventos referenciarem corretamente
                    df_temp['match_id'] = os.path.basename(f).replace('.json', '')
                    lista_de_eventos.append(df_temp)

                if 'playerIdNameDictionary' in data:
                    dicionario = {int(k): v for k, v in data['playerIdNameDictionary'].items()}
                    player_map.update(dicionario)
    
    df_events = pd.concat(lista_de_eventos, ignore_index=True)
    
    return df_events, df_matches, player_map
    
def linkarDataframes(df_events, df_matches):

    df_events['match_id'] = pd.to_numeric(df_events['match_id'])
    df_matches['id'] = pd.to_numeric(df_matches['id'])

    df = pd.merge(
        df_events,
        df_matches[['id', 'homeTeamName', 'awayTeamName', 'startTime', 'homeScore', 'awayScore']],
        left_on='match_id',
        right_on='id',
        #how agrupa apenas as partidas que tem ids tanto em matches quanto em events
        how='inner',
    )

    return df
#Coloca nomes nos IDs e limpa colunas inúteis
def limparERenomearColunas(df, player_map):
    
    print(f"Tamanho do DF ANTES da limpeza: ",df.size, "de dados.")
    mapa_times = df.groupby('teamId')['homeTeamName'].agg(
        lambda x: x.mode()[0] if not x.mode().empty else "Time Desconhecido"
    ).to_dict()

    # Aplica nomes
    df['playerName'] = df['playerId'].map(player_map).fillna("Jogador Desconhecido")
    df['teamName'] = df['teamId'].map(mapa_times)
    
    colunas_finais = [
        'match_id', 'teamName', 'playerName', 'type.displayName', 
        'outcomeType.displayName', 'minute', 'second', 'x', 'y', 
        'isGoal', 'isShot', 'homeScore', 'awayScore'
    ]
    df = df[[c for c in colunas_finais if c in df.columns]].copy()

    # Formatação de booleanos
    for col in ['isGoal', 'isShot', 'isOwnGoal', 'isTouch']:
        if col in df.columns:
            df[col] = df[col].fillna(False).astype(bool)
    
    print(f"Tamanho do DF DEPOIS da limpeza: ",df.size, "de dados.")
    return df


## FUNCOES PARA MOSTRAR O FUNCIONAMENTO DO DATASET
def rankingArtilharia():
    df = pd.read_csv("dataset.csv")

    golsMarcados = df[df['isGoal'] == True].copy()

    stats_gols = golsMarcados.groupby(['playerName', 'teamName']).size().reset_index(name='qtd_gols')

    ranking = stats_gols.sort_values(by='qtd_gols', ascending=False)

    return ranking

def nomeJogadoresEmOrdemAlfabetica():
    df = pd.read_csv("dataset.csv")
    
    nomes_unicos = df['playerName'].dropna().unique()
    
    nomes_ordenados = sorted(nomes_unicos)
    
    return nomes_ordenados

def verificaArtilheirosDeUmClube(nomeTime):
    df = pd.read_csv("dataset.csv")

    gols_clube = df[(df['teamName'] == nomeTime) & (df['isGoal'] == True)].copy()

    artilharia = gols_clube.groupby('playerName').size().reset_index(name='gols')

    artilharia = artilharia.sort_values(by='gols', ascending=False)

    return artilharia

##RODE ESSA FUNÇÃO PARA INICIAR O SISTEMA
def fazFuncionar():
    salvarWebscrapeWhoScored()
    df_events, df_matches, player_map = extrairJsonPraTabelas()
    df = limparERenomearColunas(linkarDataframes(df_events, df_matches), player_map)

    df.to_csv("dataset.csv", index=False)

fazFuncionar()