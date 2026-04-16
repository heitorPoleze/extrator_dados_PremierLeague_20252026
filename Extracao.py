import os
import soccerdata as sd
import json
import pandas as pd
from glob import glob
from tqdm import tqdm

class Extracao:
    
    def __init__(self, liga, temporada):
        self.liga = liga
        self.temporada = temporada

        self.path_base = r"C:\Users\heito\soccerdata\data\WhoScored"
        self.csv = "dataset.csv"
        self.path_matches = os.path.join(self.path_base, "matches")
        self.patch_events = os.path.join(self.path_base, "events", f"{self.liga}_{self.temporada}")


    def salvarWebscrapeWhoScored(self):
        qtd_arquivos_minimos = 200
        if not os.path.exists(self.path_matches):
            qtd_jsons = 0
        else: 
            arquivos_cache = glob(os.path.join(self.patch_events, "*.json"))
            qtd_jsons = len(arquivos_cache)
        
        if(qtd_jsons < qtd_arquivos_minimos):
            print("Cachê insuficiente. Começando a Extração dos dados da web")    
            wsdata = sd.WhoScored(leagues=self.liga, seasons=[self.temporada], headless=True)
            wsdata.read_events()
        else:
            print("Cachê suficiente. Começando extração do Json")

    #Pega os jsons salvos e transforma-os em tabelas
    def extrairJsonPraTabelas(self):
        files_matches = glob(os.path.join(self.path_matches, "**", "*.json"), recursive=True)
        lista_de_partidas = []

        for f in files_matches:
            with open(f, 'r', encoding='utf-8') as file:
                data = json.load(file)
                if data and 'tournaments' in data:
                    for torneio in data['tournaments']:
                        if 'matches' in torneio:
                            lista_de_partidas.extend(torneio['matches'])
        df_matches = pd.DataFrame(lista_de_partidas)
    
        files_events = glob(os.path.join(self.patch_events, "*.json"))
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
        
    def linkarDataframes(self, df_events, df_matches):

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
    def limparERenomearColunas(self, df, player_map):
        
        print(f"Tamanho do DF ANTES da limpeza: ",df.size, "de dados.")
        mapa_times = df.groupby('teamId')['homeTeamName'].agg(
            lambda x: x.mode()[0] if not x.mode().empty else "Time Desconhecido"
        ).to_dict()

        # Aplica nomes
        df['playerName'] = df['playerId'].map(player_map).fillna("Jogador Desconhecido")
        df['teamName'] = df['teamId'].map(mapa_times)
        
        colunas_finais = [
            'match_id', 'teamName', 'playerName', 'type.displayName', 
            'outcomeType.displayName', 'minute', 'second', 'x', 'endX', 'y', 'endY', 
            'isGoal', 'isOwnGoal', 'isShot', 'cardType.displayName', 'homeScore', 'awayScore', 'homeTeamName', 'awayTeamName'
        ]
        df = df[[c for c in colunas_finais if c in df.columns]].copy()

        # Formatação de booleanos
        for col in ['isGoal', 'isShot', 'isOwnGoal']:
            if col in df.columns:
                df[col] = df[col].fillna(False).astype(bool)
        
        for col in [ 'endX', 'endY']:
            if col in df.columns:
                df[col] = df[col].fillna(0).astype(float)

        print(f"Tamanho do DF DEPOIS da limpeza: ",df.size, " dados.")
        return df

    ##RODE ESSA FUNÇÃO PARA INICIAR O SISTEMA
    def fazFuncionar(self):
        self.salvarWebscrapeWhoScored()
        df_events, df_matches, player_map = self.extrairJsonPraTabelas()
        df_sujo = self.linkarDataframes(df_events, df_matches)
        df = self.limparERenomearColunas(df_sujo, player_map)
        df.to_csv(self.csv, index=False, sep=';', encoding='utf-8-sig')
