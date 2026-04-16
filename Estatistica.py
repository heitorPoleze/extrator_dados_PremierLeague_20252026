import pandas as pd

class Estatistica:
    
    def __init__(self, csv):
        self.csv = csv
        
    def rankingArtilharia(self):
        df = pd.read_csv(self.csv, sep=';')

        golsMarcados = df[df['isGoal'] == True].copy()

        stats_gols = golsMarcados.groupby(['playerName', 'teamName']).size().reset_index(name='qtd_gols')

        ranking = stats_gols.sort_values(by='qtd_gols', ascending=False)

        return ranking

    def nomeJogadoresEmOrdemAlfabetica(self):
        df = pd.read_csv(self.csv, sep=';')
        
        nomes_unicos = df['playerName'].dropna().unique()
        
        nomes_ordenados = sorted(nomes_unicos)
        
        return nomes_ordenados

    def verificaArtilheirosDeUmClube(self, nomeTime):
        df = pd.read_csv(self.csv, sep=';')

        gols_clube = df[(df['teamName'] == nomeTime) & (df['isGoal'] == True)].copy()

        artilharia = gols_clube.groupby('playerName').size().reset_index(name='gols')

        artilharia = artilharia.sort_values(by='gols', ascending=False)

        return artilharia