import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA
from sklearn.cluster import KMeans
from math import pi

CSV = "dataset_sequencias_xG.csv"

def extrair_features_booleanas(df):
    df['type.displayName'] = df['type.displayName'].fillna('')
    df['outcomeType.displayName'] = df['outcomeType.displayName'].fillna('')

    df['passe_certo'] = (
        (df['type.displayName'].str.contains('Pass', case=False)) & 
        (df['outcomeType.displayName'] == 'Successful')
    ).astype(int)

    estatisticas_base = df.groupby('playerName').agg(
        total_finalizacoes=('isShot', 'sum'),
        total_gols_flag=('golValido', 'sum'),
        total_assistencias=('isAssistencia', 'sum'),
        total_passes_certos=('passe_certo', 'sum')
    )
    return estatisticas_base

def extrair_matriz_eventos(df):
    # Soma de Ações realizadas por jogador
    df_frequencia = pd.crosstab(index=df['playerName'], columns=df['type.displayName']).fillna(0)
    
    df_sucesso = df[df['outcomeType.displayName'] == 'Successful']
    df_acertos = pd.crosstab(index=df_sucesso['playerName'], columns=df_sucesso['type.displayName']).fillna(0)
    
    features_alvo = [
        'Save', 'Savedshot', 'Keeperpickup', 'Claim', 'Punch', 'Keepersweeper', 'Smother', 'Penaltyfaced',
        'Tackle', 'Interception', 'Clearance', 'Ballrecovery', 'Challenge',
        'Pass', 'Takeon', 'Goodskill',
        'Goal', 'Missedshots', 'Shotonpost', 'Chancemissed',
        'Aerial', 'Shieldballopp', 'Foul', 'Card', 'Offsidegiven', 'Offsideprovoked', 'Dispossessed',
        'Balltouch'
    ]
            
    df_features = df_frequencia[features_alvo].copy()
    
    df_features['taxa_acerto_passe'] = np.where(df_features['Pass'] > 0, df_acertos['Pass'] / df_features['Pass'], 0)
    df_features['taxa_sucesso_drible'] = np.where(df_features['Takeon'] > 0, df_acertos['Takeon'] / df_features['Takeon'], 0)
    df_features['taxa_acerto_desarme'] = np.where(df_features['Tackle'] > 0, df_acertos['Tackle'] / df_features['Tackle'], 0)
    df_features['taxa_distribuicao'] = np.where(df_features['Balltouch'] > 0, df_features['Pass'] / df_features['Balltouch'], 0)
    
    df_features.drop(columns=['Balltouch'], inplace=True)
    
    return df_features

def aplicar_regras_de_nulidade(df: pd.DataFrame, limite_drop: float = 0.90, limite_fill: float = 0.15) -> pd.DataFrame:
    """
    Remove colunas com taxa de nulos acima do limite_drop.
    Preenche com 0 as colunas numéricas com taxa de nulos abaixo do limite_fill.
    """
    df_limpo = df.copy()
    total_linhas = len(df_limpo)
    
    qtd_maxima_drop = total_linhas * limite_drop
    qtd_maxima_fill = total_linhas * limite_fill

    colunas_para_remover = []
    
    for coluna in df_limpo.columns:
        contagem_nulos = df_limpo[coluna].isnull().sum()
        
        # Drop
        if contagem_nulos > qtd_maxima_drop:
            colunas_para_remover.append(coluna)
            
        # Fill
        elif 0 < contagem_nulos <= qtd_maxima_fill:
            if pd.api.types.is_numeric_dtype(df_limpo[coluna]):
                df_limpo[coluna] = df_limpo[coluna].fillna(0)
                
    df_limpo.drop(columns=colunas_para_remover, inplace=True)
    return df_limpo


def compilar_dataset_jogadores(df_eventos_brutos):
    df_trabalho = df_eventos_brutos.copy()
    
    df_booleanas = extrair_features_booleanas(df_trabalho)
    df_eventos_matriz = extrair_matriz_eventos(df_trabalho)
    
    df_consolidado = pd.merge(df_booleanas, df_eventos_matriz, left_index=True, right_index=True)
    
    df_final = aplicar_regras_de_nulidade(df_consolidado)
    
    return df_final

def aplicar_reducao_dimensionalidade(df_jogadores, n_componentes = 3):
    """Padronização Z-Score e redução (PCA)."""
    #explique linha a linha dessa funcao
    escalonador = StandardScaler()
    atributos_escalonados = escalonador.fit_transform(df_jogadores)
    
    pca = PCA(n_components=n_componentes, random_state=42)
    atributos_pca = pca.fit_transform(atributos_escalonados)
    
    df_jogadores['PCA1'] = atributos_pca[:, 0]
    df_jogadores['PCA2'] = atributos_pca[:, 1]
    
    return df_jogadores, atributos_pca

def treinar_clusters(atributos_pca, max_k = 10, k_ideal = 4):
    """Treinamento do K-Means e extração da inércia."""
    #explique linha a linha
    inercias = []
    for k in range(1, max_k + 1):
        kmeans_temporario = KMeans(n_clusters=k, random_state=42, n_init='auto')
        kmeans_temporario.fit(atributos_pca)
        inercias.append(kmeans_temporario.inertia_)

    kmeans_final = KMeans(n_clusters=k_ideal, random_state=42, n_init='auto')
    rotulos_clusters = kmeans_final.fit_predict(atributos_pca)
    
    return rotulos_clusters, inercias

def plotar_perfilamento(df_jogadores, inercias, k_ideal):
    sns.set_theme(style="whitegrid")
    fig, axes = plt.subplots(2, 2, figsize=(16, 14))
    
    # 1. Elbow Method
    intervalo_k = range(1, len(inercias) + 1)
    sns.lineplot(x=intervalo_k, y=inercias, marker='o', ax=axes[0, 0], color='#2E86C1')
    axes[0, 0].set_title('Método do Cotovelo (Elbow Method)')
    axes[0, 0].set_xlabel('Número de Clusters (k)')
    axes[0, 0].set_ylabel('Inércia (Distortion Score)')
    axes[0, 0].axvline(x=k_ideal, color='r', linestyle='--')

    # 2. Scatter Plot PCA
    sns.scatterplot(
        data=df_jogadores, x='PCA1', y='PCA2', hue='Cluster', 
        palette='tab10', ax=axes[0, 1], s=80, alpha=0.7
    )
    axes[0, 1].set_title('Distribuição dos Jogadores no Espaço PCA')

    # 3. Análise de Atributos Defensivos vs Construtores
    medias_taticas = df_jogadores.groupby('Cluster')[['Interception', 'Takeon']].mean().reset_index()
    medias_taticas = medias_taticas.rename(columns={
        'Interception': 'Interceptação',
        'Takeon': 'Drible'
    })
    medias_taticas_formatadas = medias_taticas.melt(id_vars='Cluster', var_name='Ação', value_name='Média por Partida')
    
    sns.barplot(
        data=medias_taticas_formatadas, x='Cluster', y='Média por Partida', hue='Ação', 
        palette='magma', ax=axes[1, 0]
    )
    axes[1, 0].set_title('Perfil Tático: Interceptações vs Dribles')

    # 4. Análise de Eficiência: Taxa de Acerto de Passe vs Gols
    medias_eficiencia = df_jogadores.groupby('Cluster')[['taxa_acerto_passe', 'total_gols_flag']].mean().reset_index()
    ax_twin = axes[1, 1].twinx()
    
    sns.barplot(data=medias_eficiencia, x='Cluster', y='total_gols_flag', color='lightgray', ax=axes[1, 1], label='Gols')
    sns.lineplot(data=medias_eficiencia, x='Cluster', y='taxa_acerto_passe', color='darkblue', marker='o', markersize=8, ax=ax_twin, label='Acerto de Passe')
    
    axes[1, 1].set_title('Volume de Gols vs Eficiência de Passe por Cluster')
    axes[1, 1].set_ylabel('Média de Gols')
    ax_twin.set_ylabel('Taxa de Acerto de Passes (%)')
    
    plt.tight_layout(pad=3.0, w_pad=4.0, h_pad=5.0)
    plt.show()

def executar_pipeline_perfilamento(df_eventos_brutos, n_clusters = 4):
    print("Compilando Matriz de Features com Taxas de Acerto...")
    df_jogadores = compilar_dataset_jogadores(df_eventos_brutos)
    
    print(f"Redução Dimensional (Shape atual: {df_jogadores.shape})...")
    df_jogadores, matriz_pca = aplicar_reducao_dimensionalidade(df_jogadores)
    
    print("Aplicando Algoritmo K-Means...")
    rotulos, inercias = treinar_clusters(matriz_pca, k_ideal=n_clusters)
    df_jogadores['Cluster'] = rotulos
    
    print("Renderizando Gráficos...")
    plotar_perfilamento(df_jogadores, inercias, k_ideal=n_clusters)
    print("Pipeline finalizada.")
    return df_jogadores


df = pd.read_csv("dataset_refatorado.csv", sep=';', encoding='utf-8-sig')
executar_pipeline_perfilamento(df)