import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.figure_factory as ff
import io
from sklearn.ensemble import IsolationForest
import numpy as np

# Configuration de la page Streamlit
st.set_page_config(layout="wide", page_title="Analyse de Performance Système")

# Titre principal de l'application
st.title("Tableau de Bord d'Analyse de Performance Système")

# Fonction pour charger les données
@st.cache_data
def load_data():
    """
    Charge les fichiers Excel nécessaires pour l'application.
    Utilise st.cache_data pour mettre en cache les données et améliorer la performance.
    """
    dfs = {}
    try:
        # Chemin vers les fichiers Excel (à adapter si les fichiers sont ailleurs)
        dfs['memory'] = pd.read_excel("Memory_cleaned.xlsx")
        dfs['usertcode'] = pd.read_excel("USERTCODE_cleaned.xlsx")
        dfs['times'] = pd.read_excel("Times_final_cleaned_clean.xlsx")
        dfs['tasktimes'] = pd.read_excel("TASKTIMES_final_cleaned_clean.xlsx")
        dfs['hitlist_db'] = pd.read_excel("Hitlist_DB_cleaned.xlsx")
        dfs['performance'] = pd.read_excel("AL_GET_PERFORMANCE_cleaned.xlsx")
        dfs['sql_trace_summary'] = pd.read_excel("performance_trace_summary_final_cleaned_clean.xlsx")

        # Conversion des colonnes de date si elles existent
        if 'FULL_DATETIME' in dfs['usertcode'].columns:
            dfs['usertcode']['FULL_DATETIME'] = pd.to_datetime(dfs['usertcode']['FULL_DATETIME'], errors='coerce')
        if 'GLTGB_DATE' in dfs['usr02'].columns:
            dfs['usr02']['GLTGB_DATE'] = pd.to_datetime(dfs['usr02']['GLTGB_DATE'], format='%Y%m%d', errors='coerce')

        st.success("Tous les fichiers ont été chargés avec succès.")
    except FileNotFoundError as e:
        st.error(f"Erreur: Le fichier {e.filename} n'a pas été trouvé. Assurez-vous que tous les fichiers Excel sont dans le même répertoire que le script.")
        st.stop() # Arrête l'exécution de l'application si un fichier est manquant
    except Exception as e:
        st.error(f"Une erreur est survenue lors du chargement des données : {e}")
        st.stop()
    return dfs

# Charger tous les DataFrames
dfs = load_data()

# Initialisation de l'état de la session Streamlit
if 'current_section' not in st.session_state:
    st.session_state.current_section = "Vue d'Ensemble"

# Sidebar pour la navigation et les filtres
st.sidebar.title("Navigation et Filtres")

# Boutons de navigation
if st.sidebar.button("📊 Vue d'Ensemble"):
    st.session_state.current_section = "Vue d'Ensemble"
if st.sidebar.button("💾 Analyse Mémoire"):
    st.session_state.current_section = "Analyse Mémoire"
if st.sidebar.button("👤 Transactions Utilisateurs"):
    st.session_state.current_section = "Transactions Utilisateurs"
if st.sidebar.button("⏰ Statistiques Horaires"):
    st.session_state.current_section = "Statistiques Horaires"
if st.sidebar.button("⚙️ Décomposition des Tâches"):
    st.session_state.current_section = "Décomposition des Tâches"
if st.sidebar.button("🔍 Insights Hitlist DB"):
    st.session_state.current_section = "Insights Hitlist DB"
if st.sidebar.button("⚡ Performance des Processus de Travail"):
    st.session_state.current_section = "Performance des Processus de Travail"
if st.sidebar.button("📊 Résumé des Traces de Performance SQL"):
    st.session_state.current_section = "Résumé des Traces de Performance SQL"
if st.sidebar.button("👥 Analyse des Utilisateurs"):
    st.session_state.current_section = "Analyse des Utilisateurs"

st.sidebar.markdown("---")
st.sidebar.subheader("Filtres Globaux")

# Filtres globaux
all_accounts = dfs['usertcode']['ACCOUNT'].dropna().unique() if 'ACCOUNT' in dfs['usertcode'].columns else []
selected_accounts = st.sidebar.multiselect("Sélectionner les Comptes Utilisateurs", all_accounts, key="account_filter")

all_tasktypes = dfs['usertcode']['TASKTYPE'].dropna().unique() if 'TASKTYPE' in dfs['usertcode'].columns else []
selected_tasktypes = st.sidebar.multiselect("Sélectionner les Types de Tâches", all_tasktypes, key="tasktype_filter")

all_reports = dfs['hitlist_db']['REPORT'].dropna().unique() if 'REPORT' in dfs['hitlist_db'].columns else []
selected_reports = st.sidebar.multiselect("Sélectionner les Rapports (Hitlist DB)", all_reports, key="report_filter")

all_wp_types = dfs['performance']['WP_TYP'].dropna().unique() if 'WP_TYP' in dfs['performance'].columns else []
selected_wp_types = st.sidebar.multiselect("Sélectionner les Types de Processus de Travail", all_wp_types, key="wp_type_filter")

# Option de détection d'anomalies
st.sidebar.markdown("---")
st.sidebar.subheader("Détection d'Anomalies")
enable_anomaly_detection = st.sidebar.checkbox("Activer la détection d'anomalies", value=False)
if enable_anomaly_detection:
    contamination_level = st.sidebar.slider("Niveau de contamination (pourcentage d'anomalies attendues)", 0.01, 0.1, 0.05)

# Fonction pour appliquer la détection d'anomalies
def apply_anomaly_detection(df, column, contamination):
    """
    Applique l'algorithme Isolation Forest pour détecter les anomalies dans une colonne numérique.
    Ajoute une colonne 'is_anomaly' au DataFrame.
    """
    if df.empty or column not in df.columns or df[column].isnull().all():
        return df

    # Convertir la colonne en numérique, gérer les erreurs et les NaN
    data = pd.to_numeric(df[column], errors='coerce').dropna()

    if data.empty or data.nunique() < 2: # Nécessite au moins 2 valeurs uniques pour IsolationForest
        df['is_anomaly'] = False
        return df

    # Reshape les données pour IsolationForest
    X = data.values.reshape(-1, 1)

    # Entraîner le modèle Isolation Forest
    model = IsolationForest(contamination=contamination, random_state=42)
    model.fit(X)

    # Prédire les anomalies (-1 pour les anomalies, 1 pour les inliers)
    df['is_anomaly'] = False # Initialiser la colonne
    df.loc[data.index, 'is_anomaly'] = model.predict(X) == -1
    return df

# Contenu principal de l'application
st.markdown("---")

if st.session_state.current_section == "Vue d'Ensemble":
    st.header("📊 Vue d'Ensemble du Système")

    st.markdown("""
    Bienvenue dans le tableau de bord d'analyse de performance système.
    Utilisez les onglets à gauche pour naviguer entre les différentes sections d'analyse.
    Les filtres globaux vous permettent d'affiner les données affichées.
    """)

    st.subheader("Résumé des Données Chargées")
    for key, df in dfs.items():
        st.write(f"- **{key}**: {len(df)} lignes, {len(df.columns)} colonnes")

    st.subheader("Statistiques Clés (Vue Rapide)")
    col1, col2, col3 = st.columns(3)

    # Exemple de métriques clés (à adapter selon vos données)
    if not dfs['usertcode'].empty and 'RESPTI' in dfs['usertcode'].columns:
        avg_resp_time = pd.to_numeric(dfs['usertcode']['RESPTI'], errors='coerce').mean() / 1000 if pd.to_numeric(dfs['usertcode']['RESPTI'], errors='coerce').sum() > 0 else 0
        col1.metric("Temps de Réponse Moyen (s)", f"{avg_resp_time:.2f}")
    else:
        col1.info("Temps de Réponse Moyen non disponible.")

    if not dfs['memory'].empty and 'USEDBYTES' in dfs['memory'].columns:
        total_mem_used = pd.to_numeric(dfs['memory']['USEDBYTES'], errors='coerce').sum() / (1024**3) if pd.to_numeric(dfs['memory']['USEDBYTES'], errors='coerce').sum() > 0 else 0
        col2.metric("Mémoire Totale Utilisée (Go)", f"{total_mem_used:.2f}")
    else:
        col2.info("Mémoire Totale Utilisée non disponible.")

    if not dfs['sql_trace_summary'].empty and 'TOTALEXEC' in dfs['sql_trace_summary'].columns:
        total_sql_exec = pd.to_numeric(dfs['sql_trace_summary']['TOTALEXEC'], errors='coerce').sum() if pd.to_numeric(dfs['sql_trace_summary']['TOTALEXEC'], errors='coerce').sum() > 0 else 0
        col3.metric("Total Exécutions SQL", f"{int(total_sql_exec):,}".replace(",", " "))
    else:
        col3.info("Total Exécutions SQL non disponible.")

    st.subheader("Recommandations Générales")
    st.info("""
    * **Surveillez les pics de temps de réponse :** Des augmentations soudaines peuvent indiquer des goulots d'étranglement.
    * **Optimisez les requêtes SQL lentes :** Concentrez-vous sur les requêtes avec un `EXECTIME` élevé ou un `TIMEPEREXE` élevé.
    * **Gérez l'utilisation de la mémoire :** Une `USEDBYTES` élevée peut nécessiter une optimisation des processus ou une augmentation des ressources.
    * **Examinez les redémarrages des processus :** Des redémarrages fréquents (`WP_IRESTRT`) peuvent signaler une instabilité.
    """)

elif st.session_state.current_section == "Analyse Mémoire":
    # --- Onglet 1: Analyse Mémoire (Memory_cleaned.xlsx) ---
    st.header("💾 Analyse Mémoire")
    df_mem = dfs['memory'].copy()

    # Appliquer les filtres globaux si disponibles
    if selected_accounts:
        if 'ACCOUNT' in df_mem.columns:
            df_mem = df_mem[df_mem['ACCOUNT'].isin(selected_accounts)]
        else:
            st.warning("La colonne 'ACCOUNT' est manquante dans les données mémoire pour le filtrage.")
    if selected_tasktypes:
        if 'TASKTYPE' in df_mem.columns:
            df_mem = df_mem[df_mem['TASKTYPE'].isin(selected_tasktypes)]
        else:
            st.warning("La colonne 'TASKTYPE' est manquante dans les données mémoire pour le filtrage.")

    if not df_mem.empty:
        st.subheader("Utilisation de la Mémoire par Compte Utilisateur (Top 10 USEDBYTES)")
        if 'ACCOUNT' in df_mem.columns and 'USEDBYTES' in df_mem.columns and df_mem['USEDBYTES'].sum() > 0:
            df_mem['USEDBYTES'] = pd.to_numeric(df_mem['USEDBYTES'], errors='coerce').fillna(0).astype(float)
            account_mem_summary = df_mem.groupby('ACCOUNT', as_index=False)['USEDBYTES'].sum().nlargest(10, 'USEDBYTES')
            if not account_mem_summary.empty and account_mem_summary['USEDBYTES'].sum() > 0:
                fig_account_mem = px.bar(account_mem_summary,
                                         x='ACCOUNT', y='USEDBYTES',
                                         title="Utilisation de la Mémoire par Compte Utilisateur (Top 10)",
                                         labels={'USEDBYTES': 'Utilisation Mémoire Totale (Octets)', 'ACCOUNT': 'Compte Utilisateur'},
                                         color='USEDBYTES', color_continuous_scale=px.colors.sequential.Plasma)
                st.plotly_chart(fig_account_mem, use_container_width=True)
            else:
                st.info("Pas de données valides pour l'utilisation de la mémoire par compte utilisateur après filtrage.")
        else:
            st.info("Colonnes 'ACCOUNT' ou 'USEDBYTES' manquantes ou USEDBYTES total est zéro/vide après filtrage.")

        st.subheader("Comparaison des Métriques Mémoire par Compte Utilisateur (Top 10 USEDBYTES)")
        mem_metrics_cols = ['USEDBYTES', 'MAXBYTES', 'PRIVSUM']
        # Vérifier si toutes les colonnes nécessaires sont présentes et ont des données
        if all(col in df_mem.columns for col in mem_metrics_cols) and df_mem[mem_metrics_cols].sum().sum() > 0:
            for col in mem_metrics_cols:
                df_mem[col] = pd.to_numeric(df_mem[col], errors='coerce').fillna(0).astype(float)

            # Grouper par ACCOUNT et sommer les métriques mémoire, puis prendre les 10 plus grands par USEDBYTES
            account_mem_summary = df_mem.groupby('ACCOUNT', as_index=False)[mem_metrics_cols].sum()
            account_mem_summary = account_mem_summary.nlargest(10, 'USEDBYTES')

            if not account_mem_summary.empty and account_mem_summary[mem_metrics_cols].sum().sum() > 0:
                fig_mem_comparison = px.bar(account_mem_summary,
                                            x='ACCOUNT', y=mem_metrics_cols,
                                            title="Comparaison des Métriques Mémoire par Compte Utilisateur (Top 10 USEDBYTES)",
                                            labels={'value': 'Quantité (Octets)', 'variable': 'Métrique Mémoire', 'ACCOUNT': 'Compte Utilisateur'},
                                            barmode='group',
                                            color_discrete_sequence=px.colors.qualitative.Pastel)
                st.plotly_chart(fig_mem_comparison, use_container_width=True)
            else:
                st.info("Pas de données valides pour la comparaison des métriques mémoire par compte utilisateur après filtrage.")
        else:
            st.info("Colonnes nécessaires (ACCOUNT, USEDBYTES, MAXBYTES, PRIVSUM) manquantes ou leurs totaux sont zéro/vides après filtrage pour la comparaison des métriques mémoire.")

        st.subheader("Top Types de Tâches (TASKTYPE) par Utilisation Mémoire (USEDBYTES)")
        if 'TASKTYPE' in df_mem.columns and 'USEDBYTES' in df_mem.columns and df_mem['USEDBYTES'].sum() > 0:
            # Ensure USEDBYTES is numeric here
            df_mem['USEDBYTES'] = pd.to_numeric(df_mem['USEDBYTES'], errors='coerce').fillna(0).astype(float)
            top_tasktype_mem = df_mem.groupby('TASKTYPE', as_index=False)['USEDBYTES'].sum().nlargest(3, 'USEDBYTES') # Ajout de 'USEDBYTES' comme critère
            if not top_tasktype_mem.empty and top_tasktype_mem['USEDBYTES'].sum() > 0:
                fig_top_tasktype_mem = px.bar(top_tasktype_mem,
                                              x='TASKTYPE', y='USEDBYTES',
                                              title="Top 3 Types de Tâches par Utilisation Mémoire (USEDBYTES)",
                                              labels={'USEDBYTES': 'Utilisation Mémoire Totale (Octets)', 'TASKTYPE': 'Type de Tâche'},
                                              color='USEDBYTES', color_continuous_scale=px.colors.sequential.Greys)
                st.plotly_chart(fig_top_tasktype_mem, use_container_width=True)
            else:
                st.info("Pas de données valides pour les Top Types de Tâches par Utilisation Mémoire après filtrage.")
        else:
            st.info("Colonnes 'TASKTYPE' ou 'USEDBYTES' manquantes ou USEDBYTES total est zéro/vide après filtrage pour les types de tâches mémoire.")

        st.subheader("Aperçu des Données Mémoire Filtrées")
        st.dataframe(df_mem.head())
    else:
        st.warning("Données mémoire non disponibles ou filtrées à vide.")

elif st.session_state.current_section == "Transactions Utilisateurs":
    # --- Onglet 2: Transactions Utilisateurs (USERTCODE_cleaned.xlsx) ---
    st.header("👤 Analyse des Transactions Utilisateurs")
    df_user = dfs['usertcode'].copy()
    if selected_accounts:
        if 'ACCOUNT' in df_user.columns:
            df_user = df_user[df_user['ACCOUNT'].isin(selected_accounts)]
        else:
            st.warning("La colonne 'ACCOUNT' est manquante dans les données utilisateurs pour le filtrage.")
    if selected_tasktypes:
        if 'TASKTYPE' in df_user.columns:
            df_user = df_user[df_user['TASKTYPE'].isin(selected_tasktypes)]
        else:
            st.warning("La colonne 'TASKTYPE' est manquante dans les données utilisateurs pour le filtrage.")

    if not df_user.empty:
        st.subheader("Top Types de Tâches (TASKTYPE) par Temps de Réponse Moyen")
        if 'TASKTYPE' in df_user.columns and 'RESPTI' in df_user.columns and df_user['RESPTI'].sum() > 0:
            # Ensure RESPTI is numeric before aggregation
            df_user['RESPTI'] = pd.to_numeric(df_user['RESPTI'], errors='coerce').fillna(0).astype(float)

            temp_top_tasktype_resp = df_user.groupby('TASKTYPE', as_index=False)['RESPTI'].mean()

            if not temp_top_tasktype_resp.empty and 'RESPTI' in temp_top_tasktype_resp.columns and pd.api.types.is_numeric_dtype(temp_top_tasktype_resp['RESPTI']):
                # Check if there are enough non-NaN values to perform nlargest
                if temp_top_tasktype_resp['RESPTI'].dropna().count() >= 6: # Check if at least 6 non-NaN values
                    top_tasktype_resp_intermediate = temp_top_tasktype_resp.nlargest(6, 'RESPTI').sort_values(by='RESPTI', ascending=False)

                    # Apply division only to the 'RESPTI' column
                    if not top_tasktype_resp_intermediate.empty and 'RESPTI' in top_tasktype_resp_intermediate.columns:
                        # Ensure the column is numeric before division
                        top_tasktype_resp_intermediate['RESPTI'] = pd.to_numeric(top_tasktype_resp_intermediate['RESPTI'], errors='coerce').fillna(0).astype(float)

                        # Apply division only to the numeric column
                        top_tasktype_resp = top_tasktype_resp_intermediate.copy() # Create a copy to avoid SettingWithCopyWarning
                        top_tasktype_resp['RESPTI'] = top_tasktype_resp['RESPTI'] / 1000.0

                        if not top_tasktype_resp.empty and top_tasktype_resp['RESPTI'].sum() > 0:
                            fig_top_tasktype_resp = px.bar(top_tasktype_resp,
                                                           x='TASKTYPE', y='RESPTI',
                                                           title="Top 6 TASKTYPE par Temps de Réponse Moyen (s)",
                                                           labels={'RESPTI': 'Temps de Réponse Moyen (s)', 'TASKTYPE': 'Type de Tâche'},
                                                           color='RESPTI', color_continuous_scale=px.colors.sequential.Oranges)
                            st.plotly_chart(fig_top_tasktype_resp, use_container_width=True)
                        else:
                            st.info("Pas de données valides pour les Top Types de Tâches par Temps de Réponse Moyen après filtrage et sélection des 6 plus grandes valeurs (résultat vide ou zéro après division).")
                    else:
                        st.info("Pas de données valides pour les Top Types de Tâches par Temps de Réponse Moyen après filtrage et sélection des 6 plus grandes valeurs (résultat intermédiaire vide).")
                else:
                    st.info("Pas assez de données valides dans 'RESPTI' pour déterminer les Top 6 Types de Tâches après filtrage.")
            else:
                st.info("Pas de données valides pour les Top Types de Tâches par Temps de Réponse Moyen après filtrage (la moyenne est vide ou non-numérique).")
        else:
            st.info("Colonnes 'TASKTYPE' ou 'RESPTI' manquantes ou RESPTI total est zéro/vide après filtrage.")

        transaction_types = ['COUNT', 'DCOUNT', 'UCOUNT', 'BCOUNT', 'ECOUNT', 'SCOUNT']
        available_trans_types = [col for col in transaction_types if col in df_user.columns]

        if available_trans_types and not df_user.empty and df_user[available_trans_types].sum().sum() > 0:
            # Ensure numeric types for transaction counts
            for col in available_trans_types:
                df_user[col] = pd.to_numeric(df_user[col], errors='coerce').fillna(0).astype(float)
            transactions_sum = df_user[available_trans_types].sum().sort_values(ascending=False)
            if not transactions_sum.empty and transactions_sum.sum() > 0:
                fig_transactions_sum = px.bar(transactions_sum.reset_index(),
                                              x='index', y=0,
                                              title="Nombre Total de Transactions par Type",
                                              labels={'index': 'Type de Transaction', '0': 'Nombre Total'},
                                              color=0, color_continuous_scale=px.colors.sequential.Blues)
                st.plotly_chart(fig_transactions_sum, use_container_width=True)
            else:
                st.info("Pas de données valides pour le nombre total de transactions par type après filtrage.")
        else:
            pass

        if 'RESPTI' in df_user.columns and 'ACCOUNT' in df_user.columns and 'ENTRY_ID' in df_user.columns and df_user['RESPTI'].sum() > 0:
            st.subheader("Top Comptes Utilisateurs et Opérations Associées aux Longues Durées")
            # Ensure RESPTI is numeric here
            df_user['RESPTI'] = pd.to_numeric(df_user['RESPTI'], errors='coerce').fillna(0).astype(float)
            response_time_threshold = df_user['RESPTI'].quantile(0.90)
            long_duration_users = df_user[df_user['RESPTI'] > response_time_threshold]

            if enable_anomaly_detection:
                df_user = apply_anomaly_detection(df_user, 'RESPTI', contamination_level)
                long_duration_users_anomalies = df_user[df_user['is_anomaly']]
                if not long_duration_users_anomalies.empty:
                    st.warning(f"Anomalies détectées dans le temps de réponse (RESPTI) : {len(long_duration_users_anomalies)} points.")
                    st.dataframe(long_duration_users_anomalies[['ACCOUNT', 'TASKTYPE', 'RESPTI', 'FULL_DATETIME']].head())
                else:
                    st.info("Aucune anomalie détectée dans le temps de réponse (RESPTI).")

            if not long_duration_users.empty:
                st.write(f"Seuil de temps de réponse élevé (90ème percentile) : {response_time_threshold / 1000:.2f} secondes")

                st.markdown("**Top Comptes (ACCOUNT) avec temps de réponse élevé :**")
                top_accounts_long_resp = long_duration_users['ACCOUNT'].value_counts().nlargest(10).reset_index()
                top_accounts_long_resp.columns = ['ACCOUNT', 'Occurrences']
                if not top_accounts_long_resp.empty and top_accounts_long_resp['Occurrences'].sum() > 0:
                    fig_top_acc_long = px.bar(top_accounts_long_resp, x='ACCOUNT', y='Occurrences',
                                              title="Top Comptes avec Temps de Réponse Élevé",
                                              color='Occurrences', color_continuous_scale=px.colors.sequential.Greens)
                    st.plotly_chart(fig_top_acc_long, use_container_width=True)
                else:
                    st.info("Pas de données pour les Top Comptes avec temps de réponse élevé après filtrage.")

                st.markdown("**Top Opérations (ENTRY_ID) avec temps de réponse élevé :**")
                top_entry_id_long_resp = long_duration_users['ENTRY_ID'].value_counts().nlargest(10).reset_index()
                top_entry_id_long_resp.columns = ['ENTRY_ID', 'Occurrences']
                if not top_entry_id_long_resp.empty and top_entry_id_long_resp['Occurrences'].sum() > 0:
                    fig_top_entry_long = px.bar(top_entry_id_long_resp, x='ENTRY_ID', y='Occurrences',
                                              title="Top ENTRY_ID avec Temps de Réponse Élevé",
                                              color='Occurrences', color_continuous_scale=px.colors.sequential.Teal)
                    st.plotly_chart(fig_top_entry_long, use_container_width=True)
                else:
                    st.info("Pas de données pour les Top Opérations avec temps de réponse élevé après filtrage.")
            else:
                st.info("Aucune transaction avec un temps de réponse élevé (au-dessus du 90ème percentile) après filtrage.")
        else:
            pass

        if 'FULL_DATETIME' in df_user.columns and pd.api.types.is_datetime64_any_dtype(df_user['FULL_DATETIME']) and not df_user['FULL_DATETIME'].isnull().all() and 'RESPTI' in df_user.columns and df_user['RESPTI'].sum() > 0:
            st.subheader("Tendance du Temps de Réponse Moyen par Heure")
            # Ensure RESPTI is numeric here
            df_user['RESPTI'] = pd.to_numeric(df_user['RESPTI'], errors='coerce').fillna(0).astype(float)
            hourly_resp_time = df_user.set_index('FULL_DATETIME')['RESPTI'].resample('H').mean().dropna() / 1000.0
            if not hourly_resp_time.empty:
                fig_hourly_resp = px.line(hourly_resp_time.reset_index(), x='FULL_DATETIME', y='RESPTI',
                                          title="Tendance du Temps de Réponse Moyen par Heure (s)",
                                          labels={'FULL_DATETIME': 'Heure', 'RESPTI': 'Temps de Réponse Moyen (s)'},
                                          color_discrete_sequence=['red'])
                fig_hourly_resp.update_xaxes(dtick="H1", tickformat="%H:%M")
                st.plotly_chart(fig_hourly_resp, use_container_width=True)
            else:
                st.info("Pas de données valides pour la tendance horaire du temps de réponse après filtrage.")
        else:
            st.info("Colonnes 'FULL_DATETIME' ou 'RESPTI' manquantes/invalides ou RESPTI total est zéro/vide après filtrage pour la tendance.")

        st.subheader("Corrélation entre Temps de Réponse et Temps CPU")
        st.markdown("""
            Ce graphique explore la relation entre le temps de réponse total d'une transaction et le temps CPU qu'elle consomme.
            * Chaque point représente une transaction.
            * Une tendance à la hausse (points allant de bas à gauche vers haut à droite) suggère que plus une transaction utilise de CPU, plus son temps de réponse est long.
            * Les points éloignés de la tendance peuvent indiquer d'autres facteurs influençant le temps de réponse (par exemple, des attentes E/S, des verrous, etc.).
            * La couleur des points indique le type de tâche, aidant à identifier les catégories de transactions qui se comportent différemment.
            """)

        hover_data_cols = []
        if 'ACCOUNT' in df_user.columns:
            hover_data_cols.append('ACCOUNT')
        if 'TASKTYPE' in df_user.columns:
            hover_data_cols.append('TASKTYPE')
        if 'ENTRY_ID' in df_user.columns:
            hover_data_cols.append('ENTRY_ID')

        if 'RESPTI' in df_user.columns and 'CPUTI' in df_user.columns and df_user['CPUTI'].sum() > 0 and df_user['RESPTI'].sum() > 0:
            # Ensure numeric types here
            df_user['RESPTI'] = pd.to_numeric(df_user['RESPTI'], errors='coerce').fillna(0).astype(float)
            df_user['CPUTI'] = pd.to_numeric(df_user['CPUTI'], errors='coerce').fillna(0).astype(float)

            # Apply anomaly detection to RESPTI and CPUTI if enabled
            if enable_anomaly_detection:
                df_user = apply_anomaly_detection(df_user, 'RESPTI', contamination_level)
                df_user = apply_anomaly_detection(df_user, 'CPUTI', contamination_level)
                # Combine anomalies for visualization
                df_user['is_anomaly_combined'] = df_user['is_anomaly_x'] | df_user['is_anomaly_y'] if 'is_anomaly_x' in df_user.columns and 'is_anomaly_y' in df_user.columns else df_user['is_anomaly']
                # For scatter plots, it's often useful to highlight anomalies
                color_col = 'is_anomaly_combined' if 'is_anomaly_combined' in df_user.columns else ('TASKTYPE' if 'TASKTYPE' in df_user.columns else None)
                color_map = {True: 'red', False: 'blue'} if 'is_anomaly_combined' in df_user.columns else None
            else:
                color_col = 'TASKTYPE' if 'TASKTYPE' in df_user.columns else None
                color_map = None

            fig_resp_cpu_corr = px.scatter(df_user, x='CPUTI', y='RESPTI',
                                           title="Temps de Réponse vs. Temps CPU",
                                           labels={'CPUTI': 'Temps CPU (ms)', 'RESPTI': 'Temps de Réponse (ms)'},
                                           hover_data=hover_data_cols,
                                           color=color_col,
                                           color_discrete_map=color_map, # Use color_discrete_map for boolean colors
                                           log_x=True,
                                           log_y=True,
                                           color_discrete_sequence=px.colors.qualitative.Alphabet if not enable_anomaly_detection else None) # Use default sequence if no anomaly color map

            st.plotly_chart(fig_resp_cpu_corr, use_container_width=True)
        else:
            st.info("Colonnes 'RESPTI' ou 'CPUTI' manquantes ou leurs totaux sont zéro/vide après filtrage pour la corrélation.")

        io_detailed_metrics_counts = ['READDIRCNT', 'READSEQCNT', 'CHNGCNT', 'PHYREADCNT']
        if 'TASKTYPE' in df_user.columns and all(col in df_user.columns for col in io_detailed_metrics_counts) and df_user[io_detailed_metrics_counts].sum().sum() > 0:
            st.subheader("Total des Opérations de Lecture/Écriture (Comptes) par Type de Tâche")
            st.markdown("""
                Ce graphique présente le total des opérations de lecture et d'écriture par type de tâche.
                * **READDIRCNT** : Nombre de lectures directes (accès spécifiques à des blocs de données).
                * **READSEQCNT** : Nombre de lectures séquentielles (accès consécutives aux données).
                * **CHNGCNT** : Nombre de changements (écritures) d'enregistrements.
                * **PHYREADCNT** : Nombre total de lectures physiques (lectures réelles depuis le disque).
                Ces métriques sont cruciales pour comprendre l'intensité des interactions de chaque tâche avec la base de données ou le système de fichiers.
                """)
            # Ensure numeric types here
            for col in io_detailed_metrics_counts:
                df_user[col] = pd.to_numeric(df_user[col], errors='coerce').fillna(0).astype(float)
            df_io_counts = df_user.groupby('TASKTYPE', as_index=False)[io_detailed_metrics_counts].sum().nlargest(10, 'PHYREADCNT')
            if not df_io_counts.empty and df_io_counts['PHYREADCNT'].sum() > 0: # Check sum of the column used for nlargest
                fig_io_counts = px.bar(df_io_counts, x='TASKTYPE', y=io_detailed_metrics_counts,
                                       title="Total des Opérations de Lecture/Écriture (Comptes) par Type de Tâche (Top 10)",
                                       labels={'value': 'Nombre d\'Opérations', 'variable': 'Type d\'Opération', 'TASKTYPE': 'Type de Tâche'},
                                       barmode='group', color_discrete_sequence=px.colors.sequential.Blues)
                st.plotly_chart(fig_io_counts, use_container_width=True)
            else:
                st.info("Données insuffisantes pour les opérations de lecture/écriture (comptes) après filtrage.")
        else:
            pass

        io_detailed_metrics_buffers_records = ['READDIRBUF', 'READDIRREC', 'READSEQBUF', 'READSEQREC', 'CHNGREC', 'PHYCHNGREC']
        if 'TASKTYPE' in df_user.columns and all(col in df_user.columns for col in io_detailed_metrics_buffers_records) and df_user[io_detailed_metrics_buffers_records].sum().sum() > 0:
            st.subheader("Utilisation des Buffers et Enregistrements par Type de Tâche")
            st.markdown("""
                Ce graphique détaille l'efficacité des opérations d'E/S en montrant l'utilisation des tampons et le nombre d'enregistrements traités.
                * **READDIRBUF** : Nombre de lectures directes via buffer.
                * **READDIRREC** : Nombre d'enregistrements lus directement.
                * **READSEQBUF** : Nombre de lectures séquentielles via buffer.
                * **READSEQREC** : Nombre d'enregistrements lus séquentiellement.
                * **CHNGREC** : Nombre d'enregistrements modifiés.
                * **PHYCHNGREC** : Nombre total d'enregistrements physiquement modifiés.
                Ces métriques aident à évaluer si les tâches tirent parti de la mise en cache (buffers) et l'ampleur des données traitées.
                """)
            # Ensure numeric types here
            for col in io_detailed_metrics_buffers_records:
                df_user[col] = pd.to_numeric(df_user[col], errors='coerce').fillna(0).astype(float)
            df_io_buffers_records = df_user.groupby('TASKTYPE', as_index=False)[io_detailed_metrics_buffers_records].sum().nlargest(10, 'READDIRREC')
            if not df_io_buffers_records.empty and df_io_buffers_records['READDIRREC'].sum() > 0: # Check sum of the column used for nlargest
                fig_io_buffers_records = px.bar(df_io_buffers_records, x='TASKTYPE', y=io_detailed_metrics_buffers_records,
                                                title="Utilisation des Buffers et Enregistrements par Type de Tâche (Top 10)",
                                                labels={'value': 'Nombre', 'variable': 'Métrique', 'TASKTYPE': 'Type de Tâche'},
                                                barmode='group', color_discrete_sequence=px.colors.sequential.Plasma)
                st.plotly_chart(fig_io_buffers_records, use_container_width=True)
            else:
                st.info("Données insuffisantes pour l'utilisation des buffers et enregistrements après filtrage.")
        else:
            pass


        comm_metrics_filtered = ['DSQLCNT', 'SLI_CNT']
        if 'TASKTYPE' in df_user.columns and all(col in df_user.columns for col in comm_metrics_filtered) and df_user[comm_metrics_filtered].sum().sum() > 0:
            st.subheader("Analyse des Communications et Appels Système par Type de Tâche (DSQLCNT et SLI_CNT)")
            st.markdown("""
                Ce graphique se concentre sur deux métriques clés pour les interactions des tâches avec d'autres systèmes :
                * **DSQLCNT** : Nombre d'appels SQL dynamiques (requêtes de base de données générées dynamiquement). Un nombre élevé peut indiquer une forte interaction avec la base de données.
                * **SLI_CNT** : Nombre d'appels SLI (System Level Interface). Ces appels représentent les interactions de bas niveau avec le système d'exploitation ou d'autres composants système.
                Ces métriques sont essentielles pour diagnostiquer les problèmes de communication ou les dépendances externes.
                """)
            # Ensure numeric types here
            for col in comm_metrics_filtered:
                df_user[col] = pd.to_numeric(df_user[col], errors='coerce').fillna(0).astype(float)
            df_comm_metrics = df_user.groupby('TASKTYPE', as_index=False)[comm_metrics_filtered].sum().nlargest(4, 'DSQLCNT')
            if not df_comm_metrics.empty and df_comm_metrics['DSQLCNT'].sum() > 0: # Check sum of the column used for nlargest
                fig_comm_metrics = px.bar(df_comm_metrics, x='TASKTYPE', y=comm_metrics_filtered,
                                          title="Communications et Appels Système par Type de Tâche (Top 4)",
                                          labels={'value': 'Nombre / Temps (ms)', 'variable': 'Métrique', 'TASKTYPE': 'Type de Tâche'},
                                          barmode='group', color_discrete_sequence=px.colors.qualitative.Bold)
                st.plotly_chart(fig_comm_metrics, use_container_width=True)
            else:
                st.info("Données insuffisantes pour les métriques de communication et d'appels système après filtrage.")
        else:
            st.info("Colonnes de communication (DSQLCNT, SLI_CNT) manquantes ou leurs sommes sont zéro/vides après filtrage.")


        st.subheader("Aperçu des Données Utilisateurs Filtrées")
        st.dataframe(df_user.head())
    else:
        st.warning("Données utilisateurs non disponibles ou filtrées à vide.")

elif st.session_state.current_section == "Statistiques Horaires":
    # --- Onglet 3: Statistiques Horaires (Times_final_cleaned_clean.xlsx) ---
    st.header("⏰ Statistiques Horaires du Système")
    df_times_data = dfs['times'].copy()
    if selected_tasktypes:
        if 'TASKTYPE' in df_times_data.columns:
            df_times_data = df_times_data[df_times_data['TASKTYPE'].isin(selected_tasktypes)]
        else:
            st.warning("La colonne 'TASKTYPE' est manquante dans les données horaires pour le filtrage.")

    if not df_times_data.empty:
        st.subheader("Évolution du Nombre Total d'Appels Physiques (PHYCALLS) par Tranche Horaire")
        if 'TIME' in df_times_data.columns and 'PHYCALLS' in df_times_data.columns and df_times_data['PHYCALLS'].sum() > 0:
            # Ensure PHYCALLS is numeric here
            df_times_data['PHYCALLS'] = pd.to_numeric(df_times_data['PHYCALLS'], errors='coerce').fillna(0).astype(float)
            df_times_data['HOUR_OF_DAY'] = df_times_data['TIME'].apply(lambda x: str(x).split(':')[0].zfill(2) if ':' in str(x) else str(x).zfill(2)[:2])

            # Appliquer fillna(0) sur la colonne numérique avant de grouper et de convertir en catégorie
            hourly_counts = df_times_data.groupby('HOUR_OF_DAY', as_index=False)['PHYCALLS'].sum().fillna(0)

            hourly_categories = [str(i).zfill(2) for i in range(24)] # Générer toutes les heures de 00 à 23
            hourly_counts['HOUR_OF_DAY'] = pd.Categorical(hourly_counts['HOUR_OF_DAY'], categories=hourly_categories, ordered=True)
            hourly_counts = hourly_counts.sort_values('HOUR_OF_DAY')

            if not hourly_counts.empty and hourly_counts['PHYCALLS'].sum() > 0:
                fig_phycalls = px.line(hourly_counts,
                                       x='HOUR_OF_DAY', y='PHYCALLS',
                                       title="Total Appels Physiques par Tranche Horaire",
                                       labels={'HOUR_OF_DAY': 'Tranche Horaire', 'PHYCALLS': 'Total Appels Physiques'},
                                       color_discrete_sequence=px.colors.sequential.Cividis,
                                       markers=True)
                st.plotly_chart(fig_phycalls, use_container_width=True)
            else:
                pass
        else:
            st.info("Colonnes 'TIME' ou 'PHYCALLS' manquantes ou PHYCALLS total est zéro/vide après filtrage.")

        st.subheader("Top 5 Tranches Horaires les plus Chargées (Opérations d'E/S)")
        io_cols = ['READDIRCNT', 'READSEQCNT', 'CHNGCNT']
        if all(col in df_times_data.columns for col in io_cols) and df_times_data[io_cols].sum().sum() > 0:
            # Ensure numeric types here
            for col in io_cols:
                df_times_data[col] = pd.to_numeric(df_times_data[col], errors='coerce').fillna(0).astype(float)
            df_times_data['TOTAL_IO'] = df_times_data['READDIRCNT'] + df_times_data['READSEQCNT'] + df_times_data['CHNGCNT']
            top_io_times = df_times_data.groupby('TIME', as_index=False)['TOTAL_IO'].sum().nlargest(5, 'TOTAL_IO').sort_values(by='TOTAL_IO', ascending=False)
            if not top_io_times.empty and top_io_times['TOTAL_IO'].sum() > 0:
                fig_top_io = px.bar(top_io_times,
                                    x='TIME', y='TOTAL_IO',
                                    title="Top 5 Tranches Horaires par Total Opérations I/O",
                                    labels={'TIME': 'Tranche Horaire', 'TOTAL_IO': 'Total Opérations I/O'},
                                    color='TOTAL_IO', color_continuous_scale=px.colors.sequential.Inferno)
                st.plotly_chart(fig_top_io, use_container_width=True)
            else:
                st.info("Pas de données valides pour les opérations I/O après filtrage.")
        else:
            st.info("Colonnes I/O manquantes (READDIRCNT, READSEQCNT, CHNGCNT) ou leur somme est zéro/vide après filtrage.")

        st.subheader("Temps Moyen de Réponse / CPU / Traitement par Tranche Horaire")
        perf_cols = ["RESPTI", "CPUTI", "PROCTI"]
        if all(col in df_times_data.columns for col in perf_cols) and df_times_data[perf_cols].sum().sum() > 0:
            # Ensure columns are numeric here too
            for col in perf_cols:
                df_times_data[col] = pd.to_numeric(df_times_data[col], errors='coerce').fillna(0).astype(float)

            avg_times_by_hour_temp = df_times_data.groupby("TIME", as_index=False)[perf_cols].mean()

            if not avg_times_by_hour_temp.empty and avg_times_by_hour_temp[perf_cols].sum().sum() > 0: # Check before division
                # Apply division and fillna(0) only to the numeric columns
                avg_times_by_hour = avg_times_by_hour_temp.copy() # Create a copy
                for col in perf_cols:
                    avg_times_by_hour[col] = (avg_times_by_hour[col] / 1000.0).fillna(0) # Apply fillna here

                hourly_categories_times = [
                    '00--06', '06--07', '07--08', '08--09', '09--10', '10--11', '11--12', '12--13',
                    '13--14', '14--15', '15--16', '16--17', '17--18', '18--19', '19--20', '20--21',
                    '21--22', '22--23', '23--00'
                ]
                # Convert 'TIME' to categorical AFTER numeric columns are handled
                avg_times_by_hour['TIME'] = pd.Categorical(avg_times_by_hour['TIME'], categories=hourly_categories_times, ordered=True)
                avg_times_by_hour = avg_times_by_hour.sort_values('TIME') # Removed .fillna(0) from here

                if not avg_times_by_hour.empty and avg_times_by_hour[perf_cols].sum().sum() > 0:
                    fig_avg_times = px.line(avg_times_by_hour,
                                            x='TIME', y=perf_cols,
                                            title="Temps Moyen (s) par Tranche Horaire",
                                            labels={'value': 'Temps Moyen (s)', 'variable': 'Métrique', 'TIME': 'Tranche Horaire'},
                                            color_discrete_sequence=px.colors.qualitative.Set1,
                                            markers=True)
                    st.plotly_chart(fig_avg_times, use_container_width=True)
                else:
                    st.info("Pas de données valides pour les temps moyens après filtrage.")
            else:
                st.info("Pas de données valides pour les temps moyens après filtrage (la moyenne est vide ou zéro).")
        else:
            st.info("Colonnes nécessaires (RESPTI, CPUTI, PROCTI, TIME) manquantes ou leur somme est zéro/vide après filtrage.")

        st.subheader("Aperçu des Données Horaires Filtrées")
        st.dataframe(df_times_data.head())
    else:
        st.warning("Données horaires (Times) non disponibles ou filtrées à vide.")

elif st.session_state.current_section == "Décomposition des Tâches":
    # --- Onglet 4: Décomposition des Tâches (TASKTIMES_final_cleaned_clean.xlsx) ---
    st.header("⚙️ Décomposition des Types de Tâches")
    df_task = dfs['tasktimes'].copy()
    if selected_tasktypes:
        if 'TASKTYPE' in df_task.columns:
            df_task = df_task[df_task['TASKTYPE'].isin(selected_tasktypes)]
        else:
            st.warning("La colonne 'TASKTYPE' est manquante dans les données de temps de tâches pour le filtrage.")


    if not df_task.empty:
        st.subheader("Répartition des Types de Tâches (TASKTYPE)")
        if 'TASKTYPE' in df_task.columns and 'COUNT' in df_task.columns and df_task['COUNT'].sum() > 0:
            # Ensure COUNT is numeric here
            df_task['COUNT'] = pd.to_numeric(df_task['COUNT'], errors='coerce').fillna(0).astype(float)
            task_counts = df_task.groupby('TASKTYPE', as_index=False)['COUNT'].sum()
            task_counts.columns = ['TASKTYPE', 'Count']

            min_count_for_pie = task_counts['Count'].sum() * 0.01
            significant_tasks = task_counts[task_counts['Count'] >= min_count_for_pie].copy() # Use .copy() to avoid SettingWithCopyWarning
            other_tasks_count = task_counts[task_counts['Count'] < min_count_for_pie]['Count'].sum()

            if other_tasks_count > 0:
                significant_tasks = pd.concat([significant_tasks, pd.DataFrame([{'TASKTYPE': 'Autres Petites Tâches', 'Count': other_tasks_count}])])

            if not significant_tasks.empty and significant_tasks['Count'].sum() > 0:
                fig_task_dist = px.pie(significant_tasks, values='Count', names='TASKTYPE',
                                       title="Répartition des Types de Tâches",
                                       hole=0.3,
                                       color_discrete_sequence=px.colors.sequential.RdBu)
                st.plotly_chart(fig_task_dist, use_container_width=True)
            else:
                st.info("Pas de données valides pour la répartition des types de tâches après filtrage.")
        else:
            st.info("Colonnes 'TASKTYPE' ou 'COUNT' manquantes ou COUNT total est zéro/vide après filtrage.")

        st.subheader("Top 10 TASKTYPE par Temps de Réponse (RESPTI) et CPU (CPUTI)")
        perf_cols_task = ['RESPTI', 'CPUTI']
        if 'TASKTYPE' in df_task.columns and all(col in df_task.columns for col in perf_cols_task) and df_task[perf_cols_task].sum().sum() > 0:
            # Ensure columns are numeric here too
            for col in perf_cols_task:
                df_task[col] = pd.to_numeric(df_task[col], errors='coerce').fillna(0).astype(float)

            temp_task_perf = df_task.groupby('TASKTYPE', as_index=False)[perf_cols_task].mean()

            if not temp_task_perf.empty and 'RESPTI' in temp_task_perf.columns and pd.api.types.is_numeric_dtype(temp_task_perf['RESPTI']): # Check before nlargest and division
                if temp_task_perf['RESPTI'].dropna().count() >= 10: # Check if at least 10 non-NaN values
                    top_task_perf_intermediate = temp_task_perf.nlargest(10, 'RESPTI').sort_values(by='RESPTI', ascending=False)
                    if not top_task_perf_intermediate.empty and top_task_perf_intermediate['RESPTI'].sum() > 0:
                        # Ensure columns are numeric before division
                        for col in perf_cols_task:
                            top_task_perf_intermediate[col] = pd.to_numeric(top_task_perf_intermediate[col], errors='coerce').fillna(0).astype(float)

                        # Apply division only to the numeric column
                        task_perf = top_task_perf_intermediate.copy() # Create a copy
                        for col in perf_cols_task:
                            task_perf[col] = task_perf[col] / 1000.0

                        if not task_perf.empty and task_perf['RESPTI'].sum() > 0:
                            fig_task_perf = px.bar(task_perf,
                                                   x='TASKTYPE', y=perf_cols_task,
                                                   title="Top 10 TASKTYPE par Temps de Réponse et CPU (s)",
                                                   labels={'value': 'Temps Moyen (s)', 'variable': 'Métrique', 'TASKTYPE': 'Type de Tâche'},
                                                   barmode='group', color_discrete_sequence=px.colors.qualitative.Bold)
                            st.plotly_chart(fig_task_perf, use_container_width=True)
                        else:
                            st.info("Pas de données valides pour les temps de performance des tâches après filtrage et sélection des 10 plus grandes valeurs (résultat vide ou zéro après division).")
                    else:
                        st.info("Pas de données valides pour les temps de performance des tâches après filtrage et sélection des 10 plus grandes valeurs (résultat intermédiaire vide).")
                else:
                    st.info("Pas assez de données valides dans 'RESPTI' pour déterminer les Top 10 Types de Tâches après filtrage.")
            else:
                st.info("Pas de données valides pour les temps de performance des tâches après filtrage (la moyenne est vide ou non-numérique).")
        else:
            st.info("Colonnes 'TASKTYPE', 'RESPTI' ou 'CPUTI' manquantes ou leur somme est zéro/vide après filtrage.")

        st.subheader("Décomposition des Temps d'Attente et GUI par Type de Tâche")
        st.markdown("""
            Ce graphique détaille où le temps est passé au-delà du traitement CPU pour les tâches.
            * **QUEUETI (Temps d'Attente en File)** : Temps passé par la tâche en attente dans une file d'attente. Un temps élevé peut indiquer une surcharge du système ou des goulots d'étranglement.
            * **ROLLWAITTI (Temps d'Attente de Roll-in/out)** : Temps passé par la tâche en attente de chargement ou de déchargement de la mémoire (roll-in/out).
            * **GUITIME (Temps GUI)** : Temps passé par la tâche dans l'interface graphique utilisateur.
            * **GUINETTIME (Temps Réseau GUI)** : Temps passé sur le réseau pour les interactions de l'interface graphique utilisateur.
            Ces métriques aident à identifier les causes de lenteur qui ne sont pas directement liées au CPU, comme les attentes de ressources ou les problèmes réseau.
            """)
        wait_gui_metrics = ['QUEUETI', 'ROLLWAITTI', 'GUITIME', 'GUINETTIME']
        if 'TASKTYPE' in df_task.columns and all(col in df_task.columns for col in wait_gui_metrics) and df_task[wait_gui_metrics].sum().sum() > 0:
            # Ensure numeric types here
            for col in wait_gui_metrics:
                df_task[col] = pd.to_numeric(df_task[col], errors='coerce').fillna(0).astype(float)
            df_wait_gui = df_task.groupby('TASKTYPE', as_index=False)[wait_gui_metrics].sum().nlargest(10, 'QUEUETI')
            if not df_wait_gui.empty and df_wait_gui['QUEUETI'].sum() > 0:
                fig_wait_gui = px.bar(df_wait_gui, x='TASKTYPE',
                                      y=wait_gui_metrics,
                                      title="Temps d'Attente et GUI par Type de Tâche (Top 10)",
                                      labels={'value': 'Temps (ms)', 'variable': 'Métrique de Temps', 'TASKTYPE': 'Type de Tâche'},
                                      barmode='group', color_discrete_sequence=px.colors.qualitative.Pastel)
                st.plotly_chart(fig_wait_gui, use_container_width=True)
            else:
                st.info("Données insuffisantes pour la décomposition des temps d'attente et GUI après filtrage.")
        else:
            st.info("Colonnes d'attente/GUI manquantes ou leurs sommes sont zéro/vides après filtrage.")

        st.subheader("Analyse des Opérations d'E/S (Lectures/Écritures) par Type de Tâche")
        st.markdown("""
            Ce graphique fournit des détails sur les opérations d'entrée/sortie (E/S) spécifiques aux tâches.
            * **READDIRCNT (Lectures Directes)** : Nombre de lectures directes d'enregistrements.
            * **READSEQCNT (Lectures Séquentielles)** : Nombre de lectures séquentielles d'enregistrements.
            * **CHNGCNT (Changements)** : Nombre de changements (écritures) d'enregistrements.
            * **PHYREADCNT (Lectures Physiques)** : Nombre total de lectures physiques (sur le disque).
            * **PHYCHNGREC (Changements Physiques)** : Nombre total d'enregistrements physiquement modifiés.
            * **READDIRREC (Enregistrements Lus Directement)** : Nombre d'enregistrements lus directement.
            Ces métriques sont essentielles pour identifier les tâches gourmandes en E/S et évaluer l'efficacité de l'accès aux données.
            """)
        # FIX: Added 'READDIRREC' to the list so it's available for nlargest
        io_metrics_tasktimes = ['READDIRCNT', 'READSEQCNT', 'CHNGCNT', 'PHYREADCNT', 'PHYCHNGREC', 'READDIRREC']
        if 'TASKTYPE' in df_task.columns and all(col in df_task.columns for col in io_metrics_tasktimes) and df_task[io_metrics_tasktimes].sum().sum() > 0:
            # Ensure numeric types here
            for col in io_metrics_tasktimes:
                df_task[col] = pd.to_numeric(df_task[col], errors='coerce').fillna(0).astype(float)
            df_io_tasktimes = df_task.groupby('TASKTYPE', as_index=False)[io_metrics_tasktimes].sum().nlargest(10, 'READDIRREC')
            if not df_io_tasktimes.empty and df_io_tasktimes['READDIRREC'].sum() > 0:
                fig_io_tasktimes = px.bar(df_io_tasktimes, x='TASKTYPE', y=io_metrics_tasktimes,
                                          title="Opérations d'E/S par Type de Tâche (Top 10)",
                                          labels={'value': 'Nombre d\'Opérations', 'variable': 'Métrique E/S', 'TASKTYPE': 'Type de Tâche'},
                                          barmode='group', color_discrete_sequence=px.colors.sequential.Greens)
                st.plotly_chart(fig_io_tasktimes, use_container_width=True)
            else:
                st.info("Données insuffisantes pour l'analyse des opérations d'E/S après filtrage.")
        else:
            pass


        st.subheader("Aperçu des Données des Temps de Tâches Filtrées")
        st.dataframe(df_task.head())
    else:
        st.warning("Données des temps de tâches non disponibles ou filtrées à vide.")

elif st.session_state.current_section == "Insights Hitlist DB":
    # --- NOUVEL ONGLET: Insights Détaillés de la Base de Données (Hitlist DB) ---
    st.header("🔍 Insights Détaillés de la Base de Données (Hitlist DB)")
    df_hitlist = dfs['hitlist_db'].copy()

    # Appliquer les filtres globaux si disponibles
    if selected_accounts:
        if 'ACCOUNT' in df_hitlist.columns:
            df_hitlist = df_hitlist[df_hitlist['ACCOUNT'].isin(selected_accounts)]
        else:
            st.warning("La colonne 'ACCOUNT' est manquante dans les données Hitlist DB pour le filtrage.")
    if selected_reports:
        if 'REPORT' in df_hitlist.columns:
            df_hitlist = df_hitlist[df_hitlist['REPORT'].isin(selected_reports)]
        else:
            st.warning("La colonne 'REPORT' est manquante dans les données Hitlist DB pour le filtrage.")
    if selected_tasktypes:
        if 'TASKTYPE' in df_hitlist.columns:
            df_hitlist = df_hitlist[df_hitlist['TASKTYPE'].isin(selected_tasktypes)]
        else:
            st.warning("La colonne 'TASKTYPE' est manquante dans les données Hitlist DB pour le filtrage.")

    if not df_hitlist.empty:
        st.subheader("Top 10 Rapports par Temps de Réponse Moyen (RESPTI)")
        if 'REPORT' in df_hitlist.columns and 'RESPTI' in df_hitlist.columns and df_hitlist['RESPTI'].sum() > 0:
            df_hitlist['RESPTI'] = pd.to_numeric(df_hitlist['RESPTI'], errors='coerce').fillna(0).astype(float)
            top_reports_resp = df_hitlist.groupby('REPORT', as_index=False)['RESPTI'].mean().nlargest(10, 'RESPTI')
            if not top_reports_resp.empty and top_reports_resp['RESPTI'].sum() > 0:
                fig_top_reports_resp = px.bar(top_reports_resp,
                                              x='REPORT', y='RESPTI',
                                              title="Top 10 Rapports par Temps de Réponse Moyen (ms)",
                                              labels={'RESPTI': 'Temps de Réponse Moyen (ms)', 'REPORT': 'Rapport'},
                                              color='RESPTI', color_continuous_scale=px.colors.sequential.Sunset)
                st.plotly_chart(fig_top_reports_resp, use_container_width=True)
            else:
                st.info("Pas de données valides pour les Top 10 Rapports par Temps de Réponse Moyen après filtrage.")
        else:
            st.info("Colonnes 'REPORT' ou 'RESPTI' manquantes ou RESPTI total est zéro/vide après filtrage.")

        st.subheader("Top 10 Comptes par Nombre d'Appels Base de Données (DBCALLS)")
        if 'ACCOUNT' in df_hitlist.columns and 'DBCALLS' in df_hitlist.columns and df_hitlist['DBCALLS'].sum() > 0:
            df_hitlist['DBCALLS'] = pd.to_numeric(df_hitlist['DBCALLS'], errors='coerce').fillna(0).astype(float)
            top_accounts_db_calls = df_hitlist.groupby('ACCOUNT', as_index=False)['DBCALLS'].sum().nlargest(10, 'DBCALLS')
            if not top_accounts_db_calls.empty and top_accounts_db_calls['DBCALLS'].sum() > 0:
                fig_top_accounts_db_calls = px.bar(top_accounts_db_calls,
                                                   x='ACCOUNT', y='DBCALLS',
                                                   title="Top 10 Comptes par Nombre d'Appels Base de Données",
                                                   labels={'DBCALLS': 'Nombre Total d\'Appels DB', 'ACCOUNT': 'Compte Utilisateur'},
                                                   color='DBCALLS', color_continuous_scale=px.colors.sequential.Mint)
                st.plotly_chart(fig_top_accounts_db_calls, use_container_width=True)
            else:
                st.info("Pas de données valides pour les Top 10 Comptes par Nombre d'Appels Base de Données après filtrage.")
        else:
            st.info("Colonnes 'ACCOUNT' ou 'DBCALLS' manquantes ou DBCALLS total est zéro/vide après filtrage.")

        st.subheader("Distribution du Temps de Réponse (RESPTI) - Courbe de Densité")
        if 'RESPTI' in df_hitlist.columns and df_hitlist['RESPTI'].sum() > 0:
            df_hitlist['RESPTI'] = pd.to_numeric(df_hitlist['RESPTI'], errors='coerce').fillna(0).astype(float)
            if df_hitlist['RESPTI'].nunique() > 1:
                # Apply anomaly detection if enabled
                if enable_anomaly_detection:
                    df_hitlist = apply_anomaly_detection(df_hitlist, 'RESPTI', contamination_level)
                    anomalies_resp_time = df_hitlist[df_hitlist['is_anomaly']]
                    if not anomalies_resp_time.empty:
                        st.warning(f"Anomalies détectées dans la distribution du temps de réponse (RESPTI) : {len(anomalies_resp_time)} points.")
                        st.dataframe(anomalies_resp_time[['REPORT', 'RESPTI', 'FULL_DATETIME']].head())
                    else:
                        st.info("Aucune anomalie détectée dans la distribution du temps de réponse (RESPTI).")

                fig_dist_resp_time = ff.create_distplot([df_hitlist['RESPTI'].dropna()], ['RESPTI'],
                                                        bin_size=df_hitlist['RESPTI'].std()/5 if df_hitlist['RESPTI'].std() > 0 else 1,
                                                        show_rug=False, show_hist=False)
                fig_dist_resp_time.update_layout(title_text="Distribution du Temps de Réponse (RESPTI)",
                                                  xaxis_title='Temps de Réponse (ms)',
                                                  yaxis_title='Densité')
                fig_dist_resp_time.data[0].line.color = 'darkred'
                st.plotly_chart(fig_dist_resp_time, use_container_width=True)
            else:
                st.info("La colonne 'RESPTI' contient des valeurs uniques ou est vide après filtrage, impossible de créer une courbe de densité.")
        else:
            st.info("Colonne 'RESPTI' manquante ou total est zéro/vide après filtrage.")

        st.subheader("Tendance du Temps de Réponse Moyen par Heure (Hitlist DB)")
        if 'FULL_DATETIME' in df_hitlist.columns and pd.api.types.is_datetime64_any_dtype(df_hitlist['FULL_DATETIME']) and not df_hitlist['FULL_DATETIME'].isnull().all() and 'RESPTI' in df_hitlist.columns and df_hitlist['RESPTI'].sum() > 0:
            df_hitlist['RESPTI'] = pd.to_numeric(df_hitlist['RESPTI'], errors='coerce').fillna(0).astype(float)
            hourly_resp_time_hitlist = df_hitlist.set_index('FULL_DATETIME')['RESPTI'].resample('H').mean().dropna() / 1000.0
            if not hourly_resp_time_hitlist.empty:
                fig_hourly_resp_hitlist = px.line(hourly_resp_time_hitlist.reset_index(), x='FULL_DATETIME', y='RESPTI',
                                                  title="Tendance du Temps de Réponse Moyen par Heure (s) - Hitlist DB",
                                                  labels={'FULL_DATETIME': 'Heure', 'RESPTI': 'Temps de Réponse Moyen (s)'},
                                                  color_discrete_sequence=['blue'])
                fig_hourly_resp_hitlist.update_xaxes(dtick="H1", tickformat="%H:%M")
                st.plotly_chart(fig_hourly_resp_hitlist, use_container_width=True)
            else:
                st.info("Pas de données valides pour la tendance horaire du temps de réponse après filtrage.")
        else:
            st.info("Colonnes 'FULL_DATETIME' ou 'RESPTI' manquantes/invalides ou RESPTI total est zéro/vide après filtrage pour la tendance.")

        st.subheader("Aperçu des Données Hitlist DB Filtrées")
        st.dataframe(df_hitlist.head())
    else:
        st.warning("Données Hitlist DB non disponibles ou filtrées à vide.")

elif st.session_state.current_section == "Performance des Processus de Travail":
    # --- Onglet 6: Performance des Processus de Travail (AL_GET_PERFORMANCE) ---
    st.header("⚡ Performance des Processus de Travail")
    df_perf = dfs['performance'].copy()

    if selected_wp_types:
        if 'WP_TYP' in df_perf.columns:
            df_perf = df_perf[df_perf['WP_TYP'].isin(selected_wp_types)]
        else:
            st.warning("La colonne 'WP_TYP' est manquante dans les données de performance pour le filtrage.")

    if not df_perf.empty:
        st.subheader("Distribution du Temps CPU des Processus de Travail (en secondes)")
        if 'WP_CPU_SECONDS' in df_perf.columns and df_perf['WP_CPU_SECONDS'].sum() > 0:
            # Ensure WP_CPU_SECONDS is numeric here
            df_perf['WP_CPU_SECONDS'] = pd.to_numeric(df_perf['WP_CPU_SECONDS'], errors='coerce').fillna(0).astype(float)
            if df_perf['WP_CPU_SECONDS'].nunique() > 1:
                # Apply anomaly detection if enabled
                if enable_anomaly_detection:
                    df_perf = apply_anomaly_detection(df_perf, 'WP_CPU_SECONDS', contamination_level)
                    anomalies_cpu_seconds = df_perf[df_perf['is_anomaly']]
                    if not anomalies_cpu_seconds.empty:
                        st.warning(f"Anomalies détectées dans le temps CPU des processus de travail : {len(anomalies_cpu_seconds)} points.")
                        st.dataframe(anomalies_cpu_seconds[['WP_TYP', 'WP_CPU_SECONDS']].head())
                    else:
                        st.info("Aucune anomalie détectée dans le temps CPU des processus de travail.")

                fig_cpu_dist = ff.create_distplot([df_perf['WP_CPU_SECONDS'].dropna()], ['Temps CPU (s)'],
                                                  bin_size=df_perf['WP_CPU_SECONDS'].std()/5 if df_perf['WP_CPU_SECONDS'].std() > 0 else 1,
                                                  show_rug=False, show_hist=False)
                fig_cpu_dist.update_layout(title_text="Distribution du Temps CPU des Processus de Travail",
                                           xaxis_title='Temps CPU (secondes)',
                                           yaxis_title='Densité')
                fig_cpu_dist.data[0].line.color = 'darkblue'
                st.plotly_chart(fig_cpu_dist, use_container_width=True)
            else:
                st.info("La colonne 'WP_CPU_SECONDS' contient des valeurs uniques ou est vide après filtrage, impossible de créer une courbe de densité.")
        else:
            st.info("Colonne 'WP_CPU_SECONDS' manquante ou total est zéro/vide après filtrage.")

        st.subheader("Répartition des Processus de Travail par Statut (WP_STATUS)")
        if 'WP_STATUS' in df_perf.columns and not df_perf['WP_STATUS'].empty:
            status_counts = df_perf['WP_STATUS'].value_counts().reset_index()
            status_counts.columns = ['Statut', 'Count']
            if not status_counts.empty and status_counts['Count'].sum() > 0:
                fig_status_pie = px.pie(status_counts, values='Count', names='Statut',
                                        title="Répartition des Processus de Travail par Statut",
                                        hole=0.3, color_discrete_sequence=px.colors.qualitative.Pastel)
                st.plotly_chart(fig_status_pie, use_container_width=True)
            else:
                st.info("Pas de données valides pour la répartition par statut des processus de travail après filtrage.")
        else:
            st.info("Colonne 'WP_STATUS' manquante ou vide après filtrage.")

        st.subheader("Nombre de Processus de Travail par Type (WP_TYP)")
        if 'WP_TYP' in df_perf.columns and not df_perf['WP_TYP'].empty:
            type_counts = df_perf['WP_TYP'].value_counts().reset_index()
            type_counts.columns = ['Type', 'Count']
            if not type_counts.empty and type_counts['Count'].sum() > 0:
                fig_type_bar = px.bar(type_counts, x='Type', y='Count',
                                      title="Nombre de Processus de Travail par Type",
                                      labels={'Type': 'Type de Processus', 'Count': 'Nombre'},
                                      color='Count', color_continuous_scale=px.colors.sequential.Viridis)
                st.plotly_chart(fig_type_bar, use_container_width=True)
            else:
                st.info("Pas de données valides pour le nombre de processus de travail par type après filtrage.")
        else:
            st.info("Colonne 'WP_TYP' manquante ou vide après filtrage.")

        st.subheader("Temps CPU Moyen par Type de Processus de Travail (en secondes)")
        if 'WP_TYP' in df_perf.columns and 'WP_CPU_SECONDS' in df_perf.columns and df_perf['WP_CPU_SECONDS'].sum() > 0:
            # Ensure WP_CPU_SECONDS is numeric here
            df_perf['WP_CPU_SECONDS'] = pd.to_numeric(df_perf['WP_CPU_SECONDS'], errors='coerce').fillna(0).astype(float)
            avg_cpu_by_type = df_perf.groupby('WP_TYP', as_index=False)['WP_CPU_SECONDS'].mean()
            if not avg_cpu_by_type.empty and avg_cpu_by_type['WP_CPU_SECONDS'].sum() > 0:
                fig_avg_cpu_type = px.bar(avg_cpu_by_type, x='WP_TYP', y='WP_CPU_SECONDS',
                                          title="Temps CPU Moyen par Type de Processus de Travail",
                                          labels={'WP_TYP': 'Type de Processus', 'WP_CPU_SECONDS': 'Temps CPU Moyen (s)'},
                                          color='WP_CPU_SECONDS', color_continuous_scale=px.colors.sequential.Plasma)
                st.plotly_chart(fig_avg_cpu_type, use_container_width=True)
            else:
                st.info("Pas de données valides pour le temps CPU moyen par type de processus de travail après filtrage.")
        else:
            st.info("Colonnes 'WP_TYP' ou 'WP_CPU_SECONDS' manquantes ou total est zéro/vide après filtrage.")

        st.subheader("Nombre Total de Redémarrages par Type de Processus de Travail (WP_IRESTRT)")
        if 'WP_TYP' in df_perf.columns and 'WP_IRESTRT' in df_perf.columns and df_perf['WP_IRESTRT'].sum() > 0:
            # Ensure WP_IRESTRT is numeric here
            df_perf['WP_IRESTRT'] = pd.to_numeric(df_perf['WP_IRESTRT'], errors='coerce').fillna(0).astype(float)
            restarts_by_type = df_perf.groupby('WP_TYP', as_index=False)['WP_IRESTRT'].sum().nlargest(10, 'WP_IRESTRT')
            if not restarts_by_type.empty and restarts_by_type['WP_IRESTRT'].sum() > 0:
                fig_restarts_type = px.bar(restarts_by_type, x='WP_TYP', y='WP_IRESTRT',
                                           title="Nombre Total de Redémarrages par Type de Processus de Travail",
                                           labels={'WP_TYP': 'Type de Processus', 'WP_IRESTRT': 'Nombre Total de Redémarrages'},
                                           color='WP_IRESTRT', color_continuous_scale=px.colors.sequential.OrRd)
                st.plotly_chart(fig_restarts_type, use_container_width=True)
            else:
                st.info("Pas de données valides pour le nombre de redémarrages par type de processus de travail après filtrage.")
        else:
            st.info("Colonnes 'WP_TYP' ou 'WP_IRESTRT' manquantes ou total est zéro/vide après filtrage.")

        st.subheader("Aperçu des Données de Performance Filtrées")
        st.dataframe(df_perf.head())
    else:
        st.warning("Données de performance non disponibles ou filtrées à vide.")

elif st.session_state.current_section == "Résumé des Traces de Performance SQL":
    # --- Onglet 7: Résumé des Traces de Performance SQL (performance_trace_summary_final_cleaned_clean.xlsx) ---
    st.header("📊 Résumé des Traces de Performance SQL")
    df_sql_trace = dfs['sql_trace_summary'].copy()

    if not df_sql_trace.empty:
        st.subheader("Top 10 Requêtes SQL par Temps d'Exécution Total (EXECTIME)")
        st.markdown("""
            Ce graphique identifie les 10 requêtes SQL qui ont consommé le plus de temps d'exécution cumulé.
            Il est crucial pour repérer les goulots d'étranglement globaux en termes de performance.
            """)
        if 'SQLSTATEM' in df_sql_trace.columns and 'EXECTIME' in df_sql_trace.columns and df_sql_trace['EXECTIME'].sum() > 0:
            # Ensure EXECTIME is numeric here
            df_sql_trace['EXECTIME'] = pd.to_numeric(df_sql_trace['EXECTIME'], errors='coerce').fillna(0).astype(float)
            top_sql_by_exectime = df_sql_trace.groupby('SQLSTATEM', as_index=False)['EXECTIME'].sum().nlargest(10, 'EXECTIME')
            top_sql_by_exectime['SQLSTATEM_SHORT'] = top_sql_by_exectime['SQLSTATEM'].apply(lambda x: x[:70] + '...' if len(x) > 70 else x)
            if not top_sql_by_exectime.empty and top_sql_by_exectime['EXECTIME'].sum() > 0:
                fig_top_sql_exectime = px.bar(top_sql_by_exectime, y='SQLSTATEM_SHORT', x='EXECTIME', orientation='h',
                                              title="Top 10 Requêtes SQL par Temps d'Exécution Total",
                                              labels={'SQLSTATEM_SHORT': 'Instruction SQL', 'EXECTIME': 'Temps d\'Exécution Total'},
                                              color='EXECTIME', color_continuous_scale=px.colors.sequential.Blues)
                fig_top_sql_exectime.update_yaxes(autorange="reversed")
                st.plotly_chart(fig_top_sql_exectime, use_container_width=True)
            else:
                st.info("Pas de données valides pour les Top 10 Requêtes SQL par Temps d'Exécution Total après filtrage.")
        else:
            st.info("Colonnes 'SQLSTATEM' ou 'EXECTIME' manquantes ou leur total est zéro/vide après filtrage.")

        st.subheader("Top 10 Requêtes SQL par Nombre Total d'Exécutions (TOTALEXEC)")
        st.markdown("""
            Ce graphique met en évidence les 10 requêtes SQL les plus fréquemment exécutées.
            Il est utile pour identifier les requêtes qui, même si elles ne sont pas individuellement lentes,
            peuvent avoir un impact significatif sur la performance globale en raison de leur volume d'exécution élevé.
            """)
        if 'SQLSTATEM' in df_sql_trace.columns and 'TOTALEXEC' in df_sql_trace.columns and df_sql_trace['TOTALEXEC'].sum() > 0:
            # Ensure TOTALEXEC is numeric here
            df_sql_trace['TOTALEXEC'] = pd.to_numeric(df_sql_trace['TOTALEXEC'], errors='coerce').fillna(0).astype(float)
            top_sql_by_totalexec = df_sql_trace.groupby('SQLSTATEM', as_index=False)['TOTALEXEC'].sum().nlargest(10, 'TOTALEXEC')
            top_sql_by_totalexec['SQLSTATEM_SHORT'] = top_sql_by_totalexec['SQLSTATEM'].apply(lambda x: x[:70] + '...' if len(x) > 70 else x)
            if not top_sql_by_totalexec.empty and top_sql_by_totalexec['TOTALEXEC'].sum() > 0:
                fig_top_sql_totalexec = px.bar(top_sql_by_totalexec, y='SQLSTATEM_SHORT', x='TOTALEXEC', orientation='h',
                                               title="Top 10 Requêtes SQL par Nombre Total d'Exécutions",
                                               labels={'SQLSTATEM_SHORT': 'Instruction SQL', 'TOTALEXEC': 'Nombre Total d\'Exécutions'},
                                               color='TOTALEXEC', color_continuous_scale=px.colors.sequential.Greens)
                fig_top_sql_totalexec.update_yaxes(autorange="reversed")
                st.plotly_chart(fig_top_sql_totalexec, use_container_width=True)
            else:
                st.info("Pas de données valides pour les Top 10 Requêtes SQL par Nombre Total d'Exécutions après filtrage.")
        else:
            st.info("Colonnes 'SQLSTATEM' ou 'TOTALEXEC' manquantes ou leur total est zéro/vide après filtrage.")

        st.subheader("Distribution du Temps par Exécution (TIMEPEREXE)")
        st.markdown("""
            Cette courbe de densité montre la répartition des temps d'exécution individuels par requête.
            Elle permet de comprendre si la plupart des exécutions sont rapides ou si certaines sont significativement plus lentes,
            indiquant des performances inégales.
            """)
        if 'TIMEPEREXE' in df_sql_trace.columns and df_sql_trace['TIMEPEREXE'].sum() > 0:
            # Ensure TIMEPEREXE is numeric here
            df_sql_trace['TIMEPEREXE'] = pd.to_numeric(df_sql_trace['TIMEPEREXE'], errors='coerce').fillna(0).astype(float)
            if df_sql_trace['TIMEPEREXE'].nunique() > 1:
                # Apply anomaly detection if enabled
                if enable_anomaly_detection:
                    df_sql_trace = apply_anomaly_detection(df_sql_trace, 'TIMEPEREXE', contamination_level)
                    anomalies_time_per_exe = df_sql_trace[df_sql_trace['is_anomaly']]
                    if not anomalies_time_per_exe.empty:
                        st.warning(f"Anomalies détectées dans la distribution du temps par exécution (TIMEPEREXE) : {len(anomalies_time_per_exe)} points.")
                        st.dataframe(anomalies_time_per_exe[['SQLSTATEM_SHORT', 'TIMEPEREXE']].head())
                    else:
                        st.info("Aucune anomalie détectée dans la distribution du temps par exécution (TIMEPEREXE).")

                fig_time_per_exe_dist = ff.create_distplot([df_sql_trace['TIMEPEREXE'].dropna()], ['TIMEPEREXE'],
                                                           bin_size=df_sql_trace['TIMEPEREXE'].std()/5 if df_sql_trace['TIMEPEREXE'].std() > 0 else 1,
                                                           show_rug=False, show_hist=False)
                fig_time_per_exe_dist.update_layout(title_text="Distribution du Temps par Exécution",
                                                    xaxis_title='Temps par Exécution',
                                                    yaxis_title='Densité')
                fig_time_per_exe_dist.data[0].line.color = 'darkgreen'
                st.plotly_chart(fig_time_per_exe_dist, use_container_width=True)
            else:
                st.info("La colonne 'TIMEPEREXE' contient des valeurs uniques ou est vide après filtrage, impossible de créer une courbe de densité.")
        else:
            st.info("Colonne 'TIMEPEREXE' manquante ou total est zéro/vide après filtrage.")

        st.subheader("Distribution du Temps Moyen par Enregistrement (AVGTPERREC) pour le serveur 'ECC-VE7-00'")
        st.markdown("""
            Cette courbe de densité montre la répartition du temps moyen par enregistrement spécifiquement pour le serveur "ECC-VE7-00".
            Elle permet d'analyser la cohérence des performances de ce serveur en termes de traitement des enregistrements.
            """)
        if 'SERVERNAME' in df_sql_trace.columns and 'AVGTPERREC' in df_sql_trace.columns:
            # Ensure AVGTPERREC is numeric here
            df_sql_trace['AVGTPERREC'] = pd.to_numeric(df_sql_trace['AVGTPERREC'], errors='coerce').fillna(0).astype(float)
            df_ecc_ve7_00 = df_sql_trace[df_sql_trace['SERVERNAME'].astype(str).str.contains('ECC-VE7-00', na=False, case=False)].copy()

            if not df_ecc_ve7_00.empty and df_ecc_ve7_00['AVGTPERREC'].sum() > 0:
                avg_t_per_rec_data = df_ecc_ve7_00['AVGTPERREC'].dropna()

                if avg_t_per_rec_data.nunique() > 1:
                    # Apply anomaly detection if enabled
                    if enable_anomaly_detection:
                        df_ecc_ve7_00 = apply_anomaly_detection(df_ecc_ve7_00, 'AVGTPERREC', contamination_level)
                        anomalies_avg_t_per_rec = df_ecc_ve7_00[df_ecc_ve7_00['is_anomaly']]
                        if not anomalies_avg_t_per_rec.empty:
                            st.warning(f"Anomalies détectées dans le temps moyen par enregistrement pour 'ECC-VE7-00' : {len(anomalies_avg_t_per_rec)} points.")
                            st.dataframe(anomalies_avg_t_per_rec[['SQLSTATEM_SHORT', 'AVGTPERREC']].head())
                        else:
                            st.info("Aucune anomalie détectée dans le temps moyen par enregistrement pour 'ECC-VE7-00'.")

                    fig_ecc_ve7_00_avg_time_dist = ff.create_distplot([avg_t_per_rec_data], ['AVGTPERREC'],
                                                                      bin_size=avg_t_per_rec_data.std()/5 if avg_t_per_rec_data.std() > 0 else 1,
                                                                      show_rug=False, show_hist=False)
                    fig_ecc_ve7_00_avg_time_dist.update_layout(title_text="Distribution du Temps Moyen par Enregistrement (AVGTPERREC) pour 'ECC-VE7-00'",
                                                               xaxis_title='Temps Moyen par Enregistrement',
                                                               yaxis_title='Densité')
                    fig_ecc_ve7_00_avg_time_dist.data[0].line.color = 'darkblue'
                    st.plotly_chart(fig_ecc_ve7_00_avg_time_dist, use_container_width=True)
                else:
                    st.info("Données insuffisantes ou valeurs uniques pour créer une courbe de densité pour 'ECC-VE7-00' (AVGTPERREC).")
            else:
                st.info("Aucune donnée valide pour le serveur 'ECC-VE7-00' ou la colonne 'AVGTPERREC' est vide/zéro après filtrage.")
        else:
            st.info("Colonnes 'SERVERNAME' ou 'AVGTPERREC' manquantes dans les données de traces SQL.")

        st.subheader("Top 10 Requêtes SQL par Temps Moyen par Exécution (TIMEPEREXE)")
        st.markdown("""
            Ce graphique identifie les 10 requêtes SQL qui prennent le plus de temps en moyenne à chaque exécution.
            Ceci est utile pour cibler les requêtes intrinsèquement lentes, même si elles ne sont pas exécutées très fréquemment.
            """)
        if 'SQLSTATEM' in df_sql_trace.columns and 'TIMEPEREXE' in df_sql_trace.columns and df_sql_trace['TIMEPEREXE'].sum() > 0:
            # Ensure TIMEPEREXE is numeric here
            df_sql_trace['TIMEPEREXE'] = pd.to_numeric(df_sql_trace['TIMEPEREXE'], errors='coerce').fillna(0).astype(float)
            top_sql_by_time_per_exe = df_sql_trace.groupby('SQLSTATEM', as_index=False)['TIMEPEREXE'].mean().nlargest(10, 'TIMEPEREXE')
            top_sql_by_time_per_exe['SQLSTATEM_SHORT'] = top_sql_by_time_per_exe['SQLSTATEM'].apply(lambda x: x[:70] + '...' if len(x) > 70 else x)
            if not top_sql_by_time_per_exe.empty and top_sql_by_time_per_exe['TIMEPEREXE'].sum() > 0:
                fig_top_sql_time_per_exe = px.bar(top_sql_by_time_per_exe, y='SQLSTATEM_SHORT', x='TIMEPEREXE', orientation='h',
                                                  title="Top 10 Requêtes SQL par Temps Moyen par Exécution",
                                                  labels={'SQLSTATEM_SHORT': 'Instruction SQL', 'TIMEPEREXE': 'Temps Moyen par Exécution'},
                                                  color='TIMEPEREXE', color_continuous_scale=px.colors.sequential.Oranges)
                fig_top_sql_time_per_exe.update_yaxes(autorange="reversed")
                st.plotly_chart(fig_top_sql_time_per_exe, use_container_width=True)
            else:
                st.info("Pas de données valides pour les Top 10 Requêtes SQL par Temps Moyen par Exécution après filtrage.")
        else:
            st.info("Colonnes 'SQLSTATEM' ou 'TIMEPEREXE' manquantes ou leur total est zéro/vide après filtrage.")

        st.subheader("Top 10 Requêtes SQL par Nombre d'Enregistrements Traités (RECPROCNUM)")
        st.markdown("""
            Ce graphique montre les 10 requêtes SQL qui traitent le plus grand nombre d'enregistrements.
            Cela peut indiquer des requêtes qui accèdent à de grandes quantités de données, potentiellement optimisables
            par l'ajout d'index ou la refonte de la logique de récupération des données.
            """)
        if 'SQLSTATEM' in df_sql_trace.columns and 'RECPROCNUM' in df_sql_trace.columns and df_sql_trace['RECPROCNUM'].sum() > 0:
            # Ensure RECPROCNUM is numeric here
            df_sql_trace['RECPROCNUM'] = pd.to_numeric(df_sql_trace['RECPROCNUM'], errors='coerce').fillna(0).astype(float)
            top_sql_by_recprocnum = df_sql_trace.groupby('SQLSTATEM', as_index=False)['RECPROCNUM'].sum().nlargest(10, 'RECPROCNUM')
            top_sql_by_recprocnum['SQLSTATEM_SHORT'] = top_sql_by_recprocnum['SQLSTATEM'].apply(lambda x: x[:70] + '...' if len(x) > 70 else x)
            if not top_sql_by_recprocnum.empty and top_sql_by_recprocnum['RECPROCNUM'].sum() > 0:
                fig_top_sql_recprocnum = px.bar(top_sql_by_recprocnum, y='SQLSTATEM_SHORT', x='RECPROCNUM', orientation='h',
                                                title="Top 10 Requêtes SQL par Nombre d'Enregistrements Traités",
                                                labels={'SQLSTATEM_SHORT': 'Instruction SQL', 'RECPROCNUM': 'Nombre d\'Enregistrements Traités'},
                                                color='RECPROCNUM', color_continuous_scale=px.colors.sequential.Purples)
                fig_top_sql_recprocnum.update_yaxes(autorange="reversed")
                st.plotly_chart(fig_top_sql_recprocnum, use_container_width=True)
            else:
                st.info("Colonnes 'SQLSTATEM' ou 'RECPROCNUM' manquantes ou leur total est zéro/vide après filtrage.")

        st.subheader("Aperçu des Données de Traces SQL Filtrées")
        st.dataframe(df_sql_trace.head())
    else:
        st.warning("Données de traces SQL non disponibles ou filtrées à vide.")

elif st.session_state.current_section == "Analyse des Utilisateurs":
    # --- Nouvelle section: Analyse des Utilisateurs (usr02_data.xlsx) ---
    st.header("👥 Analyse des Utilisateurs")
    df_usr02 = dfs['usr02'].copy()

    if not df_usr02.empty:
        st.subheader("Répartition des Utilisateurs par Type (USTYP)")
        if 'USTYP' in df_usr02.columns and not df_usr02['USTYP'].empty:
            user_type_counts = df_usr02['USTYP'].value_counts().reset_index()
            user_type_counts.columns = ['Type d\'Utilisateur', 'Nombre']
            if not user_type_counts.empty and user_type_counts['Nombre'].sum() > 0:
                fig_user_type_pie = px.pie(user_type_counts, values='Nombre', names='Type d\'Utilisateur',
                                           title="Répartition des Utilisateurs par Type",
                                           hole=0.3, color_discrete_sequence=px.colors.qualitative.Set3)
                st.plotly_chart(fig_user_type_pie, use_container_width=True)
            else:
                st.info("Pas de données valides pour la répartition des types d'utilisateurs après filtrage.")
        else:
            st.info("Colonne 'USTYP' manquante ou vide après filtrage.")

        st.subheader("Nombre d'Utilisateurs par Date de Dernier Logon (GLTGB)")
        st.markdown("""
            Ce graphique montre le nombre d'utilisateurs ayant enregistré leur dernière connexion à une date donnée.
            Les dates "00000000" (logon jamais enregistré) sont exclues de cette analyse.
            """)
        if 'GLTGB_DATE' in df_usr02.columns and not df_usr02['GLTGB_DATE'].isnull().all():
            df_valid_logons = df_usr02.dropna(subset=['GLTGB_DATE']).copy()
            if not df_valid_logons.empty:
                logon_counts = df_valid_logons['GLTGB_DATE'].dt.date.value_counts().sort_index().reset_index()
                logon_counts.columns = ['Date de Dernier Logon', 'Nombre d\'Utilisateurs']

                if not logon_counts.empty and logon_counts['Nombre d\'Utilisateurs'].sum() > 0:
                    fig_logon_dates = px.line(logon_counts, x='Date de Dernier Logon', y='Nombre d\'Utilisateurs',
                                              title="Nombre d'Utilisateurs par Date de Dernier Logon",
                                              labels={'Date de Dernier Logon': 'Date', 'Nombre d\'Utilisateurs': 'Nombre d\'Utilisateurs'},
                                              markers=True,
                                              color_discrete_sequence=['#6A0DAD'])

                    fig_logon_dates.update_xaxes(
                        tickangle=45,
                        rangeselector=dict(
                            buttons=list([
                                dict(count=1, label="1m", step="month", stepmode="backward"),
                                dict(count=6, label="6m", step="month", stepmode="backward"),
                                dict(count=1, label="YTD", step="year", stepmode="todate"),
                                dict(count=1, label="1y", step="year", stepmode="backward"),
                                dict(step="all")
                            ])
                        ),
                        rangeslider=dict(visible=True),
                        type="date"
                    )

                    st.plotly_chart(fig_logon_dates, use_container_width=True)
                else:
                    st.info("Aucune donnée de date de dernier logon valide après filtrage ou la somme des utilisateurs est zéro.")
            else:
                st.info("Aucune donnée de date de dernier logon valide après filtrage.")
        else:
            st.info("Colonne 'GLTGB_DATE' manquante ou ne contient pas de dates valides après filtrage.")

        st.subheader("Aperçu des Données Utilisateurs Filtrées")
        st.dataframe(df_usr02.head())
    else:
        st.warning("Données utilisateurs (USR02) non disponibles ou filtrées à vide.")

# Option pour afficher tous les DataFrames (utile pour le débogage)
with st.expander("🔍 Afficher tous les DataFrames chargés (pour débogage)"):
    for key, df in dfs.items():
        st.subheader(f"DataFrame: {key} (Taille: {len(df)} lignes)")
        st.dataframe(df.head())
        # Mise à jour de la checkbox avec une clé unique et un label plus clair
        if st.checkbox(f"Afficher les informations de '{key}' (df.info())", key=f"info_{key}"):
            buffer = io.StringIO()
            df.info(buf=buffer)
            st.text(buffer.getvalue())
            st.write(f"Description statistique pour {key}:")
            st.dataframe(df.describe())
