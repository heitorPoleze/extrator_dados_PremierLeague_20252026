import numpy as np
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import StratifiedKFold
from sklearn.metrics import roc_auc_score, brier_score_loss
import pandas as pd
import matplotlib.pyplot as plt
import shap
import joblib
import matplotlib.colors as mcolors

CSV = "dataset_refatorado.csv"
CSV_SAIDA = "dataset_sequencias_xG.csv"
GOAL_X = 100.0
GOAL_Y_CENTER = 50.0
POST1_Y = 50.0 - (7.32 / 2)
POST2_Y = 50.0 + (7.32 / 2)
BOLAS_PARADAS = ['Cornerawarded', 'CornerAwarded', 'Foul', 'Free kick', 'FreeKick', 'Throw in', 'Penalty']

def calcular_importancia_partida(df):
    standings = {}       # Tabela do campeonato
    match_context = {}   # Guarda a nota final de importância de cada partida
    
    resumo_partidas = df.groupby('match_id').agg({
        'startTime': 'first',
        'homeTeamName': 'first',
        'awayTeamName': 'first',
        'homeScore': 'max',
        'awayScore': 'max'
    }).reset_index()
    
    resumo_partidas = resumo_partidas.sort_values(by='startTime')

    for _, row in resumo_partidas.iterrows():
        partida_id = row['match_id']
        time_Casa = row['homeTeamName']
        time_Fora = row['awayTeamName']
        gols_Casa = row['homeScore']
        gols_Fora = row['awayScore']
        
        ## Registra os times na tabela no primeiro jogo deles
        for time in [time_Casa, time_Fora]:
            if pd.isna(time): continue
            if time not in standings:
                standings[time] = {'pontos': 0, 'jogos': 0}
        ## Se faltar o nome de um dos times, nota neutra
        if pd.isna(time_Casa) or pd.isna(time_Fora):
            match_context[partida_id] = 2.0
            continue
                
        pts_Casa = standings[time_Casa]['pontos']
        pts_Fora = standings[time_Fora]['pontos']
        rodadas = max(standings[time_Casa]['jogos'], standings[time_Fora]['jogos'])
        
        # Ordena a tabela por pontos para saber as posições
        tabela_ordenada = sorted(standings.items(), key=lambda x: x[1]['pontos'], reverse=True)
        
        # Função interna que calcula quantos pontos faltam para alcançar o time acima
        def pontos_acima(time_alvo, pts_atuais):
            for i, (t, stats) in enumerate(tabela_ordenada):
                if t == time_alvo:
                    if i == 0: return 0 # Se for o líder, a distância é 0
                    return tabela_ordenada[i-1][1]['pontos'] - pts_atuais
            return 0
            
        dist_acima_Casa = pontos_acima(time_Casa, pts_Casa)
        dist_acima_Fora = pontos_acima(time_Fora, pts_Fora)
        
        diferenca_pontos = abs(pts_Casa - pts_Fora)
        

        importancia = 2.0 # Jogo Base 
        
        # Fator 1: Rodadas Disputadas (Fim de campeonato = maior peso)
        importancia += min(rodadas / 38.0, 1.0) * 1.5 
        
        # Aguarda 3 rodadas para a tabela começar a fazer sentido lógico
        if rodadas > 3: 
            # Fator 2: Confronto Direto
            if diferenca_pontos <= 3:
                importancia += 1.0 # Jogo de "6 pontos" (rival direto)
            elif diferenca_pontos <= 6:
                importancia += 0.5
                
            # Fator 3: Briga por Posição
            if dist_acima_Casa <= 3 or dist_acima_Fora <= 3:
                importancia += 0.5 # Vitória garante ultrapassar o rival acima
                
        # Trava os limites
        importancia = max(min(importancia, 5.0), 1.0)
        match_context[partida_id] = round(importancia, 2)

        standings[time_Casa]['jogos'] += 1
        standings[time_Fora]['jogos'] += 1
        
        if gols_Casa > gols_Fora:
            standings[time_Casa]['pontos'] += 3
        elif gols_Fora > gols_Casa:
            standings[time_Fora]['pontos'] += 3
        else:
            standings[time_Casa]['pontos'] += 1
            standings[time_Fora]['pontos'] += 1
    
    df['match_importance'] = df['match_id'].map(match_context)
    return df

def preparar_dataset_sequencial():
    df = pd.read_csv(CSV, sep=';', encoding='utf-8-sig')
    
    df = df.sort_values(by=['match_id', 'minute', 'second']).copy()
    
    # NORMALIZANDO VARIÁVEIS DE CONTEXTO PSICOLÓGICO
    df['homeScore'] = df['homeScore'].fillna(0)
    df['awayScore'] = df['awayScore'].fillna(0)
    
    df['is_goal_num'] = df['golValido'].fillna(False).astype(int) 
    df['gols_totais_live'] = df.groupby('match_id')['is_goal_num'].cumsum() - df['is_goal_num']
    # Gols da equipe até aquele momento
    df['gols_time_live'] = df.groupby(['match_id', 'teamName'])['is_goal_num'].cumsum() - df['is_goal_num']
    # Gols do adversário até aquele momento
    df['gols_adversario_live'] = df['gols_totais_live'] - df['gols_time_live']
    
    # goal_diff real no exato momento da finalização
    df['goal_diff'] = df['gols_time_live'] - df['gols_adversario_live']
    
    df = calcular_importancia_partida(df)
    
    # 1. Distância euclidiana até o centro do gol
    df['distance'] = np.sqrt((GOAL_X - df['x'])**2 + (GOAL_Y_CENTER - df['y'])**2)
    
    # 2. Ângulo de abertura em relação às duas traves
    dx = GOAL_X - df['x']
    dx = np.where(dx == 0, 1e-5, dx) # Se distância = 0, vira distancia = 0,00001
    angle_post1 = np.arctan2(POST1_Y - df['y'], dx)
    angle_post2 = np.arctan2(POST2_Y - df['y'], dx)
    angle_rad = np.abs(angle_post1 - angle_post2)
    # Se ângulo maior que 180 graus, faz o cálculo por 360 graus
    angle_rad = np.where(angle_rad > np.pi, 2 * np.pi - angle_rad, angle_rad)
    df['angle'] = np.degrees(angle_rad)
    
    # 3. Duração do evento
    total_seconds = df['minute'] * 60 + df['second']
    df['duration'] = total_seconds.shift(-1) - total_seconds
    
    # Se a partida mudar na próxima linha, zera a duração
    df.loc[df['match_id'] != df['match_id'].shift(-1), 'duration'] = 0.0
    # Se houve perda de posse (roubada de bola, interceptação, passe errado), o tempo vira negativo
    mudou_posse = df['teamName'] != df['teamName'].shift(-1)
    df.loc[mudou_posse, 'duration'] *= -1
    df.loc[df['isShot'] == True, 'duration'] = 0.0 
    df['duration'] = df['duration'].clip(-60, 60) #normaliza outliers

    colunas_contexto = ['distance', 'angle', 'duration', 'x', 'y', 'playerName', 'teamName', 'type.displayName', 'match_id']
    # Garante que a linha sequencial de eventos pegue a mesma partida
    grouped = df.groupby('match_id')
    
    # pega as partidas anteriores e retrasadas
    for lag in [1, 2]:
        for col in colunas_contexto:
            df[f'n{lag}_{col}'] = grouped[col].shift(lag)
            
    df_sequences = df[df['isShot'] == True].copy()
    
    # Remove sequências com bola parada (faltas, escanteios, laterais, pênaltis)
    colunas_bolas_paradas = ['type.displayName', 'n1_type.displayName', 'n2_type.displayName']
    for col in colunas_bolas_paradas:
        df_sequences = df_sequences[~df_sequences[col].isin(BOLAS_PARADAS)]

    # Remove finalizações que não possuem 2 eventos anteriores
    df_sequences = df_sequences.dropna(subset=[f'n2_{col}' for col in ['distance', 'angle', 'duration']])
    df_sequences = df_sequences[(df_sequences['match_id'] == df_sequences['n1_match_id']) & 
                                (df_sequences['match_id'] == df_sequences['n2_match_id'])]
    
    colunas_renomear = ['distance', 'angle', 'duration', 'x', 'y', 'playerName', 'teamName', 'type.displayName']
    df_sequences = df_sequences.rename(columns={col: f'n0_{col}' for col in colunas_renomear})
    
    colunas_finais = [
        'match_id', 'golValido', 'minute', 'goal_diff', 'match_importance',
        'n2_teamName', 'n2_playerName', 'n2_type.displayName', 'n2_distance', 'n2_angle', 'n2_duration', 
        'n1_teamName', 'n1_playerName', 'n1_type.displayName', 'n1_distance', 'n1_angle', 'n1_duration',
        'n0_teamName', 'n0_playerName', 'n0_type.displayName', 'n0_distance', 'n0_angle'
    ]
    
    return df_sequences[[c for c in colunas_finais if c in df_sequences.columns]]

def treinar_modelo_avaliar_xG(df_sequences):
    features = [
        'n0_distance', 'n0_angle',
        'n1_distance', 'n1_angle', 'n1_duration',
        'n2_distance', 'n2_angle', 'n2_duration',
        'minute', 'goal_diff', 'match_importance'
    ]
    
    df_clean = df_sequences.dropna(subset=['golValido']).copy()

    X = df_clean[features].apply(pd.to_numeric, errors='coerce').fillna(0).values
    y = df_clean['golValido'].astype(int).values
    
    idx_gols = np.where(y == 1)[0]
    idx_nao_gols = np.where(y == 0)[0]
    
    skf = StratifiedKFold(n_splits=5, shuffle=True, random_state=67)

    list_roc_auc, list_brier = [], []
    
    # Validação Cruzada
    # A IA treina com train_idx e faz o teste em val_idx, que ela não conhece.
    # O loop faz esse rodízio 5 vezes (n_splits).
    for fold, (train_idx, val_idx) in enumerate(skf.split(X, y), 1):
        train_gols = np.intersect1d(train_idx, idx_gols)
        train_nao_gols = np.intersect1d(train_idx, idx_nao_gols)
        
        np.random.seed(67 + fold)
        amostra_nao_gols = np.random.choice(train_nao_gols, size=len(train_gols), replace=False)
        
        train_bal_idx = np.concatenate([train_gols, amostra_nao_gols])
        np.random.shuffle(train_bal_idx)
        
        rf_cv = RandomForestClassifier(n_estimators=100, random_state=67, n_jobs=-1)
        rf_cv.fit(X[train_bal_idx], y[train_bal_idx])
        
        probs = rf_cv.predict_proba(X[val_idx])[:, 1]
        list_roc_auc.append(roc_auc_score(y[val_idx], probs))
        list_brier.append(brier_score_loss(y[val_idx], probs))
        print(f" -> Fold {fold}: ROC-AUC = {list_roc_auc[-1]:.4f} | Brier = {list_brier[-1]:.4f}")
    
    print(f"\nROC-AUC Médio: {np.mean(list_roc_auc):.4f} | Brier Score Médio: {np.mean(list_brier):.4f}")

    #ponto de partida de aleatoriedade
    np.random.seed(67)
    
    final_nao_gols = np.random.choice(idx_nao_gols, size=len(idx_gols), replace=False)
    final_idx = np.concatenate([idx_gols, final_nao_gols])
    np.random.shuffle(final_idx)
        # Aplica random forest com 100 arvores de decisão, utilizando todos os núcleos do processador     
    rf_final = RandomForestClassifier(n_estimators=100, random_state=67, n_jobs=-1)
    rf_final.fit(X[final_idx], y[final_idx])
    joblib.dump(rf_final, 'modelo_xg.pkl')
    X_completo = df_sequences[features].apply(pd.to_numeric, errors='coerce').fillna(0).values
    df_sequences['expected_goals_xG'] = rf_final.predict_proba(X_completo)[:, 1]
    
    return df_sequences

def mineracao_xg():
    try:
        df_sequences = preparar_dataset_sequencial()
        df_final = treinar_modelo_avaliar_xG(df_sequences)
        
        df_final.to_csv(CSV_SAIDA, index=False, sep=';', encoding='utf-8-sig', decimal=',')
        print(f"Pipeline finalizada com sucesso. Arquivo em: {CSV_SAIDA}")
        
    except Exception as e:
        print(f"Erro na pipeline de mineração: {e}")


def visualizar_SHAP():
    # SHapley Additive exPlanations
    # gera um gráfico de dispersão onde cada ponto é um chute do seu dataset. Ele ordena as variáveis da mais importante para a menos importante
    model = joblib.load('modelo_xg.pkl')
    print(model)
    df = pd.read_csv('dataset_sequencias_xG.csv', sep=';', encoding='utf-8-sig', decimal=',')
    
    features = [    
    'n0_distance', 'n0_angle',
    'n1_distance', 'n1_angle', 'n1_duration',
    'n2_distance', 'n2_angle', 'n2_duration',
    'minute', 'goal_diff', 'match_importance'
    ]

    dicionario_traducao = {
        'n0_distance': 'Distância do Chute',
        'n0_angle': 'Ângulo do Chute',
        'n1_distance': 'Distância da Ação Anterior',
        'n1_angle': 'Ângulo da Ação Anterior',
        'n1_duration': 'Duração da Ação Anterior',
        'n2_distance': 'Distância da Penúltima Ação',
        'n2_angle': 'Ângulo da Penúltima Ação',
        'n2_duration': 'Duração da Penúltima Ação',
        'minute': 'Minuto do Jogo',
        'goal_diff': 'Saldo de Gols no Momento',
        'match_importance': 'Importância da Partida'
    }

    X = df[features].apply(pd.to_numeric, errors='coerce').fillna(0)
    X_pt = X.rename(columns=dicionario_traducao)
    # Destrinchando combinações das variáveis para chegar a uma conclusão do impacto de cada uma, a partir da consulta da estrutura das árvores de decisão do modelo.
    explainer = shap.TreeExplainer(model)
    shap_values_obj = explainer(X_pt, check_additivity=False)

    if len(shap_values_obj.shape) == 3:
        explicacoes_gol = shap_values_obj[:, :, 1]
    else:
        explicacoes_gol = shap_values_obj

    print("Gerando gráfico...")    
    fig, ax = plt.subplots(figsize=(12, 8))
    
    shap.plots.beeswarm(
        explicacoes_gol, 
        max_display=len(features), 
        color_bar=False,
        show=False
    )

    plt.tight_layout(rect=[0, 0.05, 1, 0.95])

    cax = fig.add_axes([0.35, 0.03, 0.4, 0.02]) 
    
    cmap = shap.plots.colors.red_blue
    norm = mcolors.Normalize(vmin=-1, vmax=1)
    sm = plt.cm.ScalarMappable(cmap=cmap, norm=norm)
    sm.set_array([])
    
    cbar = fig.colorbar(sm, cax=cax, orientation='horizontal')
    cbar.set_label("Valor Real da Variável", fontsize=11)
    cbar.set_ticks([-1, 1])
    cbar.set_ticklabels(["Baixo", "Alto"])

    plt.suptitle("Impacto das Variáveis no xG", fontsize=16, y=0.98)
    
    plt.show()
    
mineracao_xg()
visualizar_SHAP()