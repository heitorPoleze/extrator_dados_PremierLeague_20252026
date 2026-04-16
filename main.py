##script para pegar o webscrapping via soccerdata para o site WhoScored 
from Extracao import Extracao
from Estatistica import Estatistica

extracao = Extracao("ENG-Premier League", 2526)
estatisticas = Estatistica(extracao.csv)

print(estatisticas.rankingArtilharia())