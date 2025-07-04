import streamlit as st
import pandas as pd
import plotly.express as px
import numpy as np
import io
import re # Importation nécessaire pour les expressions régulières
import plotly.figure_factory as ff # Importation ajoutée pour create_distplot
from sklearn.ensemble import IsolationForest # Importation de l'algorithme Isolation Forest

# --- Chemins vers vos fichiers de données ---
# IMPORTANT : Ces chemins sont maintenant RELATIFS au répertoire où se trouve ce script.
# Assurez-vous que vos fichiers de données (.xlsx) sont placés dans le même répertoire que ce fichier Python
# sur votre dépôt GitHub.
DATA_PATHS = {
    "memory": "memory_final_cleaned_clean.xlsx",
    "hitlist_db": "HITLIST_DATABASE_final_cleaned_clean.xlsx",
    "times": "Times_final_cleaned_clean.xlsx",
    "tasktimes": "TASKTIMES_final_cleaned_clean.xlsx",
    "usertcode": "USERTCODE_cleaned.xlsx",
    "performance": "AL_GET_PERFORMANCE_final_cleaned_clean.xlsx",
    "sql_trace_summary": "performance_trace_summary_final_cleaned_clean.xlsx",
    "usr02": "usr02_data.xlsx", # Assurez-vous que ce fichier est aussi dans le même répertoire
}

# --- Configuration de la page Streamlit ---
st.set_page_config(layout="wide", page_title="Dashboard SAP Complet Multi-Sources")

# --- Fonctions de Nettoyage et Chargement des Données (avec cache) ---

def clean_string_column(series, default_value="Non défini"):
    """
    Nettoie une série de type string : supprime espaces, remplace NaN/vides/caractères non imprimables.
    """
    cleaned_series = series.astype(str).str.strip()
    cleaned_series = cleaned_series.apply(lambda x: re.sub(r'[^\x20-\x7E\s]+', ' ', x).strip())
    cleaned_series = cleaned_series.replace({'nan': default_value, '': default_value, ' ': default_value})
    return cleaned_series

def clean_column_names(df):
    """
    Nettoyage des noms de colonnes : supprime les espaces, les caractères invisibles,
    et s'assure qu'ils sont valides pour l'accès.
    """
    new_columns = []
    for col in df.columns:
        cleaned_col = re.sub(r'[\x00-\x1F\x7F-\x9F]', '', str(col)).strip()
        cleaned_col = re.sub(r'[^a-zA-Z0-9_]', '_', cleaned_col)
        cleaned_col = re.sub(r'_+', '_', cleaned_col)
        cleaned_col = cleaned_col.strip('_')
        new_columns.append(cleaned_col)
    df.columns = new_columns
    return df

def convert_mm_ss_to_seconds(time_str):
    """
    Convertit une chaîne de caractères au format MM:SS en secondes.
    Gère les cas où les minutes ou secondes sont manquantes ou invalides.
    """
    if pd.isna(time_str) or not isinstance(time_str, str):
        return 0
    try:
        parts = time_str.split(':')
        if len(parts) == 2:
            minutes = float(parts[0])
            seconds = float(parts[1])
            return int(minutes * 60 + seconds)
        elif len(parts) == 1:
            return int(float(parts[0]))
        else:
            return 0
    except ValueError:
        return 0

def clean_numeric_with_comma(series):
    """
    Nettoyage d'une série de chaînes numériques qui peuvent contenir des virgules
    comme séparateurs de milliers ou décimaux, et conversion en float.
    """
    cleaned_series = series.astype(str).str.replace(' ', '').str.replace('.', '', regex=False).str.replace(',', '.', regex=False)
    return pd.to_numeric(cleaned_series, errors='coerce').fillna(0)


@st.cache_data
def load_and_process_data(file_key, path):
    """Charge et nettoie un fichier Excel/CSV."""
    df = pd.DataFrame()
    try:
        if path.lower().endswith('.xlsx'):
            df = pd.read_excel(path)
        elif path.lower().endswith('.csv'):
            df = pd.read_csv(path)
        else:
            st.error(f"Format de fichier non supporté pour {file_key}: {path}")
            return pd.DataFrame()

        df = clean_column_names(df.copy())

        # --- Gestion spécifique des types de données et valeurs manquantes ---
        if file_key == "memory":
            numeric_cols = ['MEMSUM', 'PRIVSUM', 'USEDBYTES', 'MAXBYTES', 'MAXBYTESDI', 'PRIVCOUNT', 'RESTCOUNT', 'COUNTER']
            for col in numeric_cols:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
            
            if 'ACCOUNT' in df.columns:
                df['ACCOUNT'] = clean_string_column(df['ACCOUNT'], 'Compte Inconnu')
            if 'MANDT' in df.columns:
                df['MANDT'] = clean_string_column(df['MANDT'], 'MANDT Inconnu')
            if 'TASKTYPE' in df.columns:
                df['TASKTYPE'] = clean_string_column(df['TASKTYPE'], 'Type de Tâche Inconnu')

            if 'ENDDATE' in df.columns and 'ENDTIME' in df.columns:
                df['ENDTIME_STR'] = df['ENDTIME'].astype(str).str.zfill(6)
                df['FULL_DATETIME'] = pd.to_datetime(df['ENDDATE'].astype(str) + df['ENDTIME_STR'], format='%Y%m%d%H%M%S', errors='coerce')
                df.drop(columns=['ENDTIME_STR'], inplace=True, errors='ignore')
            elif 'FULL_DATETIME' in df.columns and not pd.api.types.is_datetime64_any_dtype(df['FULL_DATETIME']):
                 df['FULL_DATETIME'] = pd.to_datetime(df['FULL_DATETIME'], errors='coerce')
            
            subset_cols_memory = []
            if 'USEDBYTES' in df.columns:
                subset_cols_memory.append('USEDBYTES')
            if 'ACCOUNT' in df.columns:
                subset_cols_memory.append('ACCOUNT')
            if subset_cols_memory:
                df.dropna(subset=subset_cols_memory, inplace=True)


        elif file_key == "hitlist_db":
            numeric_cols = [
                'GENERATETI', 'REPLOADTI', 'CUALOADTI', 'DYNPLOADTI', 'QUETI', 'DDICTI', 'CPICTI',
                'LOCKCNT', 'LOCKTI', 'BTCSTEPNR', 'RESPTI', 'PROCTI', 'CPUTI', 'QUEUETI', 'ROLLWAITTI',
                'GUITIME', 'GUICNT', 'GUINETTIME', 'DBP_COUNT', 'DBP_TIME', 'DSQLCNT', 'QUECNT',
                'CPICCNT', 'SLI_CNT', 'TAB1DIRCNT', 'TAB1SEQCNT', 'TAB1UPDCNT', 'TAB2DIRCNT',
                'TAB2SEQCNT', 'TAB2UPDCNT', 'TAB3DIRCNT', 'TAB3SEQCNT', 'TAB3UPDCNT', 'TAB4DIRCNT',
                'TAB4SEQCNT', 'TAB4UPDCNT', 'TAB5DIRCNT', 'TAB5SEQCNT', 'TAB5UPDCNT',
                'READDIRCNT', 'READDIRTI', 'READDIRBUF', 'READDIRREC', 'READSEQCNT', 'READSEQTI',
                'READSEQBUF', 'READSEQREC', 'PHYREADCNT', 'INSCNT', 'INSTI', 'INSREC', 'PHYINSCNT',
                'UPDCNT', 'UPDTI', 'UPDREC', 'PHYUPDCNT', 'DELCNT', 'DELTI', 'DELREC', 'PHYDELCNT',
                'DBCALLS', 'COMMITTI', 'INPUTLEN', 'OUTPUTLEN', 'MAXROLL', 'MAXPAGE',
                'ROLLINCNT', 'ROLLINTI', 'ROLLOUTCNT', 'ROLLOUTTI', 'ROLLED_OUT', 'PRIVSUM',
                'USEDBYTES', 'MAXBYTES', 'MAXBYTESDI', 'RFCRECEIVE', 'RFCSEND',
                'RFCEXETIME', 'RFCCALLTIM', 'RFCCALLS', 'VMC_CALL_COUNT', 'VMC_CPU_TIME', 'VMC_ELAP_TIME'
            ]
            for col in numeric_cols:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
            
            if 'ENDDATE' in df.columns and 'ENDTIME' in df.columns:
                df['ENDTIME_STR'] = df['ENDTIME'].astype(str).str.zfill(6)
                df['FULL_DATETIME'] = pd.to_datetime(df['ENDDATE'].astype(str) + df['ENDTIME_STR'], format='%Y%m%d%H%M%S', errors='coerce')
                df.drop(columns=['ENDTIME_STR'], inplace=True, errors='ignore')
            elif 'FULL_DATETIME' in df.columns and not pd.api.types.is_datetime64_any_dtype(df['FULL_DATETIME']):
                 df['FULL_DATETIME'] = pd.to_datetime(df['FULL_DATETIME'], errors='coerce')

            subset_cols_hitlist = []
            if 'RESPTI' in df.columns: subset_cols_hitlist.append('RESPTI')
            if 'PROCTI' in df.columns: subset_cols_hitlist.append('PROCTI')
            if 'CPUTI' in df.columns: subset_cols_hitlist.append('CPUTI')
            if 'DBCALLS' in df.columns: subset_cols_hitlist.append('DBCALLS')
            if subset_cols_hitlist:
                df.dropna(subset=subset_cols_hitlist, inplace=True)
            if 'FULL_DATETIME' in df.columns:
                df.dropna(subset=['FULL_DATETIME'], inplace=True)

            for col in ['WPID', 'ACCOUNT', 'REPORT', 'ROLLKEY', 'PRIVMODE', 'WPRESTART', 'TASKTYPE']:
                if col in df.columns:
                    df[col] = clean_string_column(df[col])


        elif file_key == "times":
            numeric_cols = [
                'COUNT', 'LUW_COUNT', 'RESPTI', 'PROCTI', 'CPUTI', 'QUEUETI', 'ROLLWAITTI',
                'GUITIME', 'GUICNT', 'GUINETTIME', 'DBP_COUNT', 'DBP_TIME', 'READDIRCNT',
                'READDIRTI', 'READDIRBUF', 'READDIRREC', 'READSEQCNT', 'READSEQTI',
                'READSEQBUF', 'READSEQREC', 'CHNGCNT', 'CHNGTI', 'CHNGREC', 'PHYREADCNT',
                'PHYCHNGREC', 'PHYCALLS', 'VMC_CALL_COUNT', 'VMC_CPU_TIME', 'VMC_ELAP_TIME'
            ]
            for col in numeric_cols:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
            
            subset_cols_times = []
            if 'RESPTI' in df.columns: subset_cols_times.append('RESPTI')
            if 'PHYCALLS' in df.columns: subset_cols_times.append('PHYCALLS')
            if 'COUNT' in df.columns: subset_cols_times.append('COUNT')
            if subset_cols_times:
                df.dropna(subset=subset_cols_times, inplace=True)
            
            if 'TIME' in df.columns:
                df['TIME'] = clean_string_column(df['TIME'])
            if 'TASKTYPE' in df.columns:
                df['TASKTYPE'] = clean_string_column(df['TASKTYPE'])
            if 'ENTRY_ID' in df.columns:
                df['ENTRY_ID'] = clean_string_column(df[col])

        elif file_key == "tasktimes":
            numeric_cols = [
                'COUNT', 'RESPTI', 'PROCTI', 'CPUTI', 'QUEUETI', 'ROLLWAITTI', 'GUITIME',
                'GUICNT', 'GUINETTIME', 'DBP_COUNT', 'DBP_TIME', 'READDIRCNT', 'READDIRTI',
                'READDIRBUF', 'READDIRREC', 'READSEQCNT', 'READSEQTI',
                'READSEQBUF', 'READSEQREC', 'CHNGCNT', 'CHNGTI', 'CHNGREC', 'PHYREADCNT',
                'PHYCHNGREC', 'PHYCALLS', 'CNT001', 'CNT002', 'CNT003', 'CNT004', 'CNT005', 'CNT006', 'CNT007', 'CNT008', 'CNT009'
            ]
            for col in numeric_cols:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
            
            subset_cols_tasktimes = []
            if 'COUNT' in df.columns: subset_cols_tasktimes.append('COUNT')
            if 'RESPTI' in df.columns: subset_cols_tasktimes.append('RESPTI')
            if 'CPUTI' in df.columns: subset_cols_tasktimes.append('CPUTI')
            if subset_cols_tasktimes:
                df.dropna(subset=subset_cols_tasktimes, inplace=True)
            
            if 'TASKTYPE' in df.columns:
                df['TASKTYPE'] = clean_string_column(df['TASKTYPE'], 'Type de tâche non spécifié')
            if 'TIME' in df.columns:
                df['TIME'] = clean_string_column(df['TIME'])


        elif file_key == "usertcode":
            numeric_cols = [
                'COUNT', 'DCOUNT', 'UCOUNT', 'BCOUNT', 'ECOUNT', 'SCOUNT', 'LUW_COUNT',
                'TMBYTESIN', 'TMBYTESOUT', 'RESPTI', 'PROCTI', 'CPUTI', 'QUEUETI',
                'ROLLWAITTI', 'GUITIME', 'GUICNT', 'GUINETTIME', 'DBP_COUNT', 'DBP_TIME',
                'READDIRCNT', 'READDIRTI', 'READDIRBUF', 'READDIRREC', 'READSEQCNT',
                'READSEQTI', 'READSEQBUF', 'READSEQREC', 'CHNGCNT', 'CHNGTI', 'CHNGREC',
                'PHYREADCNT', 'PHYCHNGREC', 'PHYCALLS', 'DSQLCNT', 'QUECNT', 'CPICCNT',
                'SLI_CNT', 'VMC_CALL_COUNT', 'VMC_CPU_TIME', 'VMC_ELAP_TIME'
            ]
            for col in numeric_cols:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
            
            if 'ENDDATE' in df.columns and 'ENDTIME' in df.columns:
                df['ENDTIME_STR'] = df['ENDTIME'].astype(str).str.zfill(6)
                df['FULL_DATETIME'] = pd.to_datetime(df['ENDDATE'].astype(str) + df['ENDTIME_STR'], format='%Y%m%d%H%M%S', errors='coerce')
                df.drop(columns=['ENDTIME_STR'], inplace=True, errors='ignore')
            elif 'FULL_DATETIME' in df.columns and not pd.api.types.is_datetime64_any_dtype(df['FULL_DATETIME']):
                 df['FULL_DATETIME'] = pd.to_datetime(df['FULL_DATETIME'], errors='coerce')

            critical_usertcode_cols = []
            if 'RESPTI' in df.columns: critical_usertcode_cols.append('RESPTI')
            if 'ACCOUNT' in df.columns: critical_usertcode_cols.append('ACCOUNT')
            if 'COUNT' in df.columns: critical_usertcode_cols.append('COUNT')
            
            if critical_usertcode_cols:
                df.dropna(subset=critical_usertcode_cols, inplace=True)
            
            for col in ['TASKTYPE', 'ENTRY_ID', 'ACCOUNT']:
                if col in df.columns:
                    df[col] = clean_string_column(df[col])

        elif file_key == "performance":
            if 'WP_CPU' in df.columns:
                df['WP_CPU_SECONDS'] = df['WP_CPU'].apply(convert_mm_ss_to_seconds)
            
            if 'WP_IWAIT' in df.columns:
                df['WP_IWAIT'] = pd.to_numeric(df['WP_IWAIT'], errors='coerce').fillna(0)
                df['WP_IWAIT_SECONDS'] = df['WP_IWAIT'] / 1000.0 
            else:
                df['WP_IWAIT_SECONDS'] = 0

            for col in ['WP_SEMSTAT', 'WP_IACTION', 'WP_ITYPE', 'WP_RESTART', 'WP_ISTATUS', 'WP_TYP', 'WP_STATUS']:
                if col in df.columns:
                    df[col] = clean_string_column(df[col])
            
            numeric_cols_perf = ['WP_NO', 'WP_IRESTRT', 'WP_PID', 'WP_INDEX']
            for col in numeric_cols_perf:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
            
            subset_cols_perf = []
            if 'WP_CPU_SECONDS' in df.columns: subset_cols_perf.append('WP_CPU_SECONDS')
            if 'WP_STATUS' in df.columns: subset_cols_perf.append('WP_STATUS')
            if subset_cols_perf:
                df.dropna(subset=subset_cols_perf, inplace=True)
        
        elif file_key == "sql_trace_summary":
            numeric_cols_sql = ['TOTALEXEC', 'IDENTSEL', 'EXECTIME', 'RECPROCNUM', 'TIMEPEREXE', 'RECPEREXE', 'AVGTPERREC', 'MINTPERREC']
            for col in numeric_cols_sql:
                if col in df.columns:
                    df[col] = clean_numeric_with_comma(df[col])
            
            for col in ['SQLSTATEM', 'SERVERNAME', 'TRANS_ID']:
                if col in df.columns:
                    df[col] = clean_string_column(df[col])
            
            subset_cols_sql = []
            if 'EXECTIME' in df.columns: subset_cols_sql.append('EXECTIME')
            if 'TOTALEXEC' in df.columns: subset_cols_sql.append('TOTALEXEC')
            if 'SQLSTATEM' in df.columns: subset_cols_sql.append('SQLSTATEM')
            if subset_cols_sql:
                df.dropna(subset=subset_cols_sql, inplace=True)

        elif file_key == "usr02": # Nouveau bloc pour usr02_data.xlsx
            for col in ['BNAME', 'USTYP']:
                if col in df.columns:
                    df[col] = clean_string_column(df[col])
            
            if 'GLTGB' in df.columns:
                # Replace '00000000' with NaN, then convert to datetime
                df['GLTGB'] = df['GLTGB'].astype(str).replace('00000000', np.nan)
                df['GLTGB_DATE'] = pd.to_datetime(df['GLTGB'], format='%Y%m%d', errors='coerce')
            else:
                df['GLTGB_DATE'] = pd.NaT # Assign Not a Time if column is missing

        st.success(f"'{file_key}' chargé avec succès. {len(df)} lignes après nettoyage.")
        return df

    except FileNotFoundError:
        st.error(f"Erreur: Le fichier '{path}' pour '{file_key}' est introuvable. Veuillez vérifier le chemin.")
        return pd.DataFrame()
    except Exception as e:
        st.error(f"Une erreur est survenue lors du traitement du fichier '{file_key}' : {e}. Détails : {e}")
        return pd.DataFrame()

# --- Chargement de TOUTES les données ---
dfs = {}
for key, path in DATA_PATHS.items():
    dfs[key] = load_and_process_data(key, path)

# --- Contenu principal du Dashboard ---
st.title("📊 Tableau de Bord SAP Complet Multi-Sources")
st.markdown("Explorez les performances, l'utilisation mémoire, les transactions utilisateurs et la santé du système à travers différentes sources de données.")

# --- Barre de navigation flexible ---
tab_titles = [
    "Analyse Mémoire",
    "Transactions Utilisateurs",
    "Statistiques Horaires",
    "Décomposition des Tâches",
    "Insights Hitlist DB",
    "Performance des Processus de Travail",
    "Résumé des Traces de Performance SQL",
    "Analyse des Utilisateurs", # Nouvelle section pour usr02
    "Détection d'Anomalies"
]

if 'current_section' not in st.session_state:
    st.session_state.current_section = tab_titles[0]

st.sidebar.header("Navigation Rapide")
selected_section = st.sidebar.radio(
    "Accéder à la section :",
    tab_titles,
    index=tab_titles.index(st.session_state.current_section)
)
st.session_state.current_section = selected_section

if all(df.empty for df in dfs.values()):
    st.error("Aucune source de données n'a pu être chargée. Le dashboard ne peut pas s'afficher. Veuillez vérifier les chemins et les fichiers.")
else:
    # --- Sidebar pour les filtres globaux ---
    st.sidebar.header("Filtres")

    all_accounts = pd.Index([])
    if not dfs['memory'].empty and 'ACCOUNT' in dfs['memory'].columns:
        all_accounts = all_accounts.union(dfs['memory']['ACCOUNT'].dropna().unique())
    if not dfs['usertcode'].empty and 'ACCOUNT' in dfs['usertcode'].columns:
        all_accounts = all_accounts.union(dfs['usertcode']['ACCOUNT'].dropna().unique())
    if not dfs['hitlist_db'].empty and 'ACCOUNT' in dfs['hitlist_db'].columns:
        all_accounts = all_accounts.union(dfs['hitlist_db']['ACCOUNT'].dropna().unique())
    
    selected_accounts = []
    if not all_accounts.empty:
        selected_accounts = st.sidebar.multiselect(
            "Sélectionner des Comptes",
            options=sorted(all_accounts.tolist()),
            default=[]
        )
        if selected_accounts:
            for key in ['memory', 'usertcode', 'hitlist_db']:
                if not dfs[key].empty and 'ACCOUNT' in dfs[key].columns:
                    dfs[key] = dfs[key][dfs[key]['ACCOUNT'].isin(selected_accounts)]

    selected_reports = []
    if not dfs['hitlist_db'].empty and 'REPORT' in dfs['hitlist_db'].columns:
        all_reports = dfs['hitlist_db']['REPORT'].dropna().unique().tolist()
        selected_reports = st.sidebar.multiselect(
            "Sélectionner des Rapports (Hitlist DB)",
            options=sorted(all_reports),
            default=[]
        )
        if selected_reports:
            dfs['hitlist_db'] = dfs['hitlist_db'][dfs['hitlist_db']['REPORT'].isin(selected_reports)]
    
    all_tasktypes = pd.Index([])
    if not dfs['usertcode'].empty and 'TASKTYPE' in dfs['usertcode'].columns:
        all_tasktypes = all_tasktypes.union(dfs['usertcode']['TASKTYPE'].dropna().unique())
    if not dfs['times'].empty and 'TASKTYPE' in dfs['times'].columns:
        all_tasktypes = all_tasktypes.union(dfs['times']['TASKTYPE'].dropna().unique())
    if not dfs['tasktimes'].empty and 'TASKTYPE' in dfs['tasktimes'].columns:
        all_tasktypes = all_tasktypes.union(dfs['tasktimes']['TASKTYPE'].dropna().unique())
    if not dfs['hitlist_db'].empty and 'TASKTYPE' in dfs['hitlist_db'].columns:
        all_tasktypes = all_tasktypes.union(dfs['hitlist_db']['TASKTYPE'].dropna().unique())

    selected_tasktypes = []
    if not all_tasktypes.empty:
        selected_tasktypes = st.sidebar.multiselect(
            "Sélectionner des Types de Tâches",
            options=sorted(all_tasktypes.tolist()),
            default=[]
        )
        if selected_tasktypes:
            for key in ['usertcode', 'times', 'tasktimes', 'hitlist_db']:
                if not dfs[key].empty and 'TASKTYPE' in dfs[key].columns:
                    dfs[key] = dfs[key][dfs[key]['TASKTYPE'].isin(selected_tasktypes)]
    
    selected_wp_types = []
    if not dfs['performance'].empty and 'WP_TYP' in dfs['performance'].columns:
        all_wp_types = dfs['performance']['WP_TYP'].dropna().unique().tolist()
        selected_wp_types = st.sidebar.multiselect(
            "Sélectionner des Types de Processus de Travail (Performance)",
            options=sorted(all_wp_types),
            default=[]
        )
        if selected_wp_types:
            dfs['performance'] = dfs['performance'][dfs['performance']['WP_TYP'].isin(selected_wp_types)]

    df_hitlist_filtered = dfs['hitlist_db'].copy()
    if not dfs['hitlist_db'].empty and 'FULL_DATETIME' in dfs['hitlist_db'].columns and \
       pd.api.types.is_datetime64_any_dtype(dfs['hitlist_db']['FULL_DATETIME']) and \
       not dfs['hitlist_db']['FULL_DATETIME'].isnull().all():
        
        min_date_data = dfs['hitlist_db']['FULL_DATETIME'].min()
        max_date_data = dfs['hitlist_db']['FULL_DATETIME'].max()

        if pd.notna(min_date_data) and pd.notna(max_date_data) and min_date_data.date() <= max_date_data.date():
            default_start_date = min_date_data.date()
            default_end_date = max_date_data.date()

            date_range_hitlist = st.sidebar.date_input(
                "Période pour Insights Hitlist DB et Anomalies", # Mis à jour pour inclure les anomalies
                value=(default_start_date, default_end_date),
                min_value=min_date_data.date(),
                max_value=max_date_data.date()
            )
            if len(date_range_hitlist) == 2:
                start_date_filter_dt, end_date_filter_dt = pd.to_datetime(date_range_hitlist[0]), pd.to_datetime(date_range_hitlist[1])
                df_hitlist_filtered = dfs['hitlist_db'][(dfs['hitlist_db']['FULL_DATETIME'] >= start_date_filter_dt) & 
                                                             (dfs['hitlist_db']['FULL_DATETIME'] <= end_date_filter_dt + pd.Timedelta(days=1, seconds=-1))]
            else:
                df_hitlist_filtered = dfs['hitlist_db'].copy()
        else:
            st.sidebar.warning("La colonne 'FULL_DATETIME' dans HITLIST_DATABASE ne contient pas de dates valides pour le filtre ou la plage est inversée.")
            df_hitlist_filtered = dfs['hitlist_db'].copy()
    else:
        st.sidebar.info("HITLIST_DATABASE ou colonne 'FULL_DATETIME' non disponible/valide pour le filtre de date.")
        df_hitlist_filtered = dfs['hitlist_db'].copy()

    # --- Contenu des sections basé sur la sélection de la barre latérale ---
    if st.session_state.current_section == "Analyse Mémoire":
        st.header("🧠 Analyse de l'Utilisation Mémoire")
        df_mem = dfs['memory'].copy()
        if selected_accounts:
            df_mem = df_mem[df_mem['ACCOUNT'].isin(selected_accounts)]

        if not df_mem.empty:
            st.subheader("Top 10 Utilisateurs par Utilisation Mémoire (USEDBYTES)")
            if all(col in df_mem.columns for col in ['ACCOUNT', 'USEDBYTES', 'MAXBYTES', 'PRIVSUM']) and df_mem['USEDBYTES'].sum() > 0:
                top_users_mem = df_mem.groupby('ACCOUNT')[['USEDBYTES', 'MAXBYTES', 'PRIVSUM']].sum().nlargest(10, 'USEDBYTES')
                fig_top_users_mem = px.bar(top_users_mem.reset_index(),
                                           x='ACCOUNT', y='USEDBYTES',
                                           title="Top 10 Comptes par USEDBYTES Total",
                                           labels={'USEDBYTES': 'Utilisation Mémoire (Octets)', 'ACCOUNT': 'Compte Utilisateur'},
                                           hover_data=['MAXBYTES', 'PRIVSUM'],
                                           color='USEDBYTES', color_continuous_scale=px.colors.sequential.Plasma)
                st.plotly_chart(fig_top_users_mem, use_container_width=True)
            else:
                st.info("Colonnes nécessaires (ACCOUNT, USEDBYTES, MAXBYTES, PRIVSUM) manquantes ou USEDBYTES total est zéro/vide après filtrage.")

            st.subheader("Moyenne de USEDBYTES par Client (ACCOUNT)")
            if 'ACCOUNT' in df_mem.columns and 'USEDBYTES' in df_mem.columns and df_mem['USEDBYTES'].sum() > 0:
                df_mem_account_clean = df_mem[df_mem['ACCOUNT'] != 'Compte Inconnu'].copy()
                
                if not df_mem_account_clean.empty:
                    df_mem_account_clean['ACCOUNT_DISPLAY'] = df_mem_account_clean['ACCOUNT'].astype(str)

                    account_counts = df_mem_account_clean['ACCOUNT_DISPLAY'].nunique()
                    if account_counts > 6:
                        top_accounts = df_mem_account_clean['ACCOUNT_DISPLAY'].value_counts().nlargest(6).index
                        df_mem_account_filtered_for_plot = df_mem_account_clean.loc[df_mem_account_clean['ACCOUNT_DISPLAY'].isin(top_accounts)].copy()
                    else:
                        df_mem_account_filtered_for_plot = df_mem_account_clean.copy()

                    avg_mem_account = df_mem_account_filtered_for_plot.groupby('ACCOUNT_DISPLAY')['USEDBYTES'].mean().sort_values(ascending=False)
                    if not avg_mem_account.empty and not avg_mem_account.sum() == 0:
                        fig_avg_mem_account = px.bar(avg_mem_account.reset_index(),
                                                x='ACCOUNT_DISPLAY', y='USEDBYTES',
                                                title="Moyenne de USEDBYTES par Client SAP (Top 6 ou tous)",
                                                labels={'USEDBYTES': 'Moyenne USEDBYTES (Octets)', 'ACCOUNT_DISPLAY': 'Client SAP'},
                                                color='USEDBYTES', color_continuous_scale=px.colors.sequential.Viridis)
                        fig_avg_mem_account.update_xaxes(type='category') 
                        st.plotly_chart(fig_avg_mem_account, use_container_width=True)
                    else:
                        st.info("Pas de données valides pour la moyenne de USEDBYTES par Client SAP après filtrage (peut-être tous 'Compte Inconnu' ou USEDBYTES est zéro).")
                else:
                    st.info("Aucune donnée valide pour les clients (ACCOUNT) après filtrage.")
            else:
                st.info("Colonnes 'ACCOUNT' ou 'USEDBYTES' manquantes ou USEDBYTES total est zéro/vide après filtrage.")

            st.subheader("Distribution de l'Utilisation Mémoire (USEDBYTES) - Courbe de Densité")
            if 'USEDBYTES' in df_mem.columns and df_mem['USEDBYTES'].sum() > 0:
                if df_mem['USEDBYTES'].nunique() > 1:
                    fig_dist_mem = ff.create_distplot([df_mem['USEDBYTES'].dropna()], ['USEDBYTES'],
                                                     bin_size=df_mem['USEDBYTES'].std()/5,
                                                     show_rug=False, show_hist=False)
                    fig_dist_mem.update_layout(title_text="Distribution de l'Utilisation Mémoire (USEDBYTES) - Courbe de Densité",
                                               xaxis_title='Utilisation Mémoire (Octets)',
                                               yaxis_title='Densité')
                    fig_dist_mem.data[0].line.color = 'lightcoral'
                    st.plotly_chart(fig_dist_mem, use_container_width=True)
                else:
                    st.info("La colonne 'USEDBYTES' contient des valeurs uniques ou est vide après filtrage, impossible de créer une courbe de densité.")
            else:
                st.info("Colonne 'USEDBYTES' manquante ou total est zéro/vide après filtrage.")

            if 'FULL_DATETIME' in df_mem.columns and pd.api.types.is_datetime64_any_dtype(df_mem['FULL_DATETIME']) and not df_mem['FULL_DATETIME'].isnull().all() and df_mem['USEDBYTES'].sum() > 0:
                st.subheader("Tendance Moyenne USEDBYTES par Heure")
                hourly_mem_usage = df_mem.set_index('FULL_DATETIME')['USEDBYTES'].resample('H').mean().dropna()
                if not hourly_mem_usage.empty:
                    fig_hourly_mem = px.line(hourly_mem_usage.reset_index(), x='FULL_DATETIME', y='USEDBYTES',
                                             title="Tendance Moyenne USEDBYTES par Heure",
                                             labels={'FULL_DATETIME': 'Heure', 'USEDBYTES': 'Moyenne USEDBYTES'},
                                             color_discrete_sequence=['purple'])
                    fig_hourly_mem.update_xaxes(dtick="H1", tickformat="%H:%M")
                    st.plotly_chart(fig_hourly_mem, use_container_width=True)
            
            st.subheader("Comparaison des Métriques Mémoire (USEDBYTES, MAXBYTES, PRIVSUM) par Compte Utilisateur")
            mem_metrics_cols = ['USEDBYTES', 'MAXBYTES', 'PRIVSUM']
            if all(col in df_mem.columns for col in mem_metrics_cols) and 'ACCOUNT' in df_mem.columns and df_mem[mem_metrics_cols].sum().sum() > 0:
                account_mem_summary = df_mem.groupby('ACCOUNT')[mem_metrics_cols].sum().nlargest(10, 'USEDBYTES').reset_index()
                
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
                top_tasktype_mem = df_mem.groupby('TASKTYPE')['USEDBYTES'].sum().nlargest(3).reset_index()
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
        st.header("👤 Analyse des Transactions Utilisateurs")
        df_user = dfs['usertcode'].copy()
        if selected_accounts:
            if 'ACCOUNT' in df_user.columns:
                df_user = df_user[df_user['ACCOUNT'].isin(selected_accounts)]
        if selected_tasktypes:
            if 'TASKTYPE' in df_user.columns:
                df_user = df_user[df_user['TASKTYPE'].isin(selected_tasktypes)]

        if not df_user.empty:
            if 'TASKTYPE' in df_user.columns and 'RESPTI' in df_user.columns and df_user['RESPTI'].sum() > 0:
                st.subheader("Top Types de Tâches (TASKTYPE) par Temps de Réponse Moyen")
                top_tasktype_resp = df_user.groupby('TASKTYPE')['RESPTI'].mean().nlargest(6).sort_values(ascending=False) / 1000.0
                if not top_tasktype_resp.empty:
                    fig_top_tasktype_resp = px.bar(top_tasktype_resp.reset_index(),
                                                   x='TASKTYPE', y='RESPTI',
                                                   title="Top 6 TASKTYPE par Temps de Réponse Moyen (s)",
                                                   labels={'RESPTI': 'Temps de Réponse Moyen (s)', 'TASKTYPE': 'Type de Tâche'},
                                                   color='RESPTI', color_continuous_scale=px.colors.sequential.Oranges)
                    st.plotly_chart(fig_top_tasktype_resp, use_container_width=True)
                else:
                    st.info("Pas de données valides pour les Top Types de Tâches par Temps de Réponse Moyen après filtrage.")
            else:
                st.info("Colonnes 'TASKTYPE' ou 'RESPTI' manquantes ou RESPTI total est zéro/vide après filtrage.")
            
            transaction_types = ['COUNT', 'DCOUNT', 'UCOUNT', 'BCOUNT', 'ECOUNT', 'SCOUNT']
            available_trans_types = [col for col in transaction_types if col in df_user.columns]

            if available_trans_types and not df_user.empty and df_user[available_trans_types].sum().sum() > 0:
                st.subheader("Nombre Total de Transactions par Type")
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
            
            if 'RESPTI' in df_user.columns and 'ACCOUNT' in df_user.columns and 'ENTRY_ID' in df_user.columns and df_user['RESPTI'].sum() > 0:
                st.subheader("Top Comptes Utilisateurs et Opérations Associées aux Longues Durées")
                response_time_threshold = df_user['RESPTI'].quantile(0.90)
                long_duration_users = df_user[df_user['RESPTI'] > response_time_threshold]

                if not long_duration_users.empty:
                    st.write(f"Seuil de temps de réponse élevé (90ème percentile) : {response_time_threshold / 1000:.2f} secondes")
                    
                    st.markdown("**Top Comptes (ACCOUNT) avec temps de réponse élevé :**")
                    top_accounts_long_resp = long_duration_users['ACCOUNT'].value_counts().nlargest(10).reset_index()
                    top_accounts_long_resp.columns = ['ACCOUNT', 'Occurrences']
                    fig_top_acc_long = px.bar(top_accounts_long_resp, x='ACCOUNT', y='Occurrences',
                                              title="Top Comptes avec Temps de Réponse Élevé",
                                              color='Occurrences', color_continuous_scale=px.colors.sequential.Greens)
                    st.plotly_chart(fig_top_acc_long, use_container_width=True)
                    
                    st.markdown("**Top Opérations (ENTRY_ID) avec temps de réponse élevé :**")
                    top_entry_id_long_resp = long_duration_users['ENTRY_ID'].value_counts().nlargest(10).reset_index()
                    top_entry_id_long_resp.columns = ['ENTRY_ID', 'Occurrences']
                    fig_top_entry_long = px.bar(top_entry_id_long_resp, x='ENTRY_ID', y='Occurrences',
                                                title="Top ENTRY_ID avec Temps de Réponse Élevé",
                                                color='Occurrences', color_continuous_scale=px.colors.sequential.Teal)
                    st.plotly_chart(fig_top_entry_long, use_container_width=True)
                else:
                    st.info("Aucune transaction avec un temps de réponse élevé (au-dessus du 90ème percentile) après filtrage.")
            
            if 'FULL_DATETIME' in df_user.columns and pd.api.types.is_datetime64_any_dtype(df_user['FULL_DATETIME']) and not df_user['FULL_DATETIME'].isnull().all() and df_user['RESPTI'].sum() > 0:
                st.subheader("Tendance du Temps de Réponse Moyen par Heure")
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
                fig_resp_cpu_corr = px.scatter(df_user, x='CPUTI', y='RESPTI',
                                               title="Temps de Réponse vs. Temps CPU",
                                               labels={'CPUTI': 'Temps CPU (ms)', 'RESPTI': 'Temps de Réponse (ms)'},
                                               hover_data=hover_data_cols,
                                               color='TASKTYPE' if 'TASKTYPE' in df_user.columns else None,
                                               log_x=True,
                                               log_y=True,
                                               trendline="ols",
                                               color_discrete_sequence=px.colors.qualitative.Alphabet)
                st.plotly_chart(fig_resp_cpu_corr, use_container_width=True)
            else:
                st.info("Colonnes 'RESPTI' ou 'CPUTI' manquantes ou leurs totaux sont zéro/vide après filtrage pour la corrélation.")
            
            io_detailed_metrics_counts = ['READDIRCNT', 'READSEQCNT', 'CHNGCNT', 'PHYREADCNT']
            io_detailed_metrics_buffers_records = ['READDIRBUF', 'READDIRREC', 'READSEQBUF', 'READSEQREC', 'CHNGREC', 'PHYCHNGREC']

            if 'TASKTYPE' in df_user.columns and all(col in df_user.columns for col in io_detailed_metrics_counts) and df_user[io_detailed_metrics_counts].sum().sum() > 0:
                st.subheader("Total des Opérations de Lecture/Écriture (Comptes) par Type de Tâche")
                st.markdown("""
                    Ce graphique présente le total des opérations de lecture et d'écriture par type de tâche.
                    * **READDIRCNT** : Nombre de lectures directes (accès spécifiques à des blocs de données).
                    * **READSEQCNT** : Nombre de lectures séquentielles (accès consécutifs aux données).
                    * **CHNGCNT** : Nombre de changements (écritures) d'enregistrements.
                    * **PHYREADCNT** : Nombre total de lectures physiques (lectures réelles depuis le disque).
                    Ces métriques sont cruciales pour comprendre l'intensité des interactions de chaque tâche avec la base de données ou le système de fichiers.
                    """)
                df_io_counts = df_user.groupby('TASKTYPE')[io_detailed_metrics_counts].sum().nlargest(10, 'PHYREADCNT').reset_index()
                if not df_io_counts.empty and df_io_counts[io_detailed_metrics_counts].sum().sum() > 0:
                    fig_io_counts = px.bar(df_io_counts, x='TASKTYPE', y=io_detailed_metrics_counts,
                                           title="Total des Opérations de Lecture/Écriture (Comptes) par Type de Tâche (Top 10)",
                                           labels={'value': 'Nombre d\'Opérations', 'variable': 'Type d\'Opération', 'TASKTYPE': 'Type de Tâche'},
                                           barmode='group', color_discrete_sequence=px.colors.sequential.Blues)
                    st.plotly_chart(fig_io_counts, use_container_width=True)
                else:
                    st.info("Données insuffisantes pour les opérations de lecture/écriture (comptes) après filtrage.")

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
                df_io_buffers_records = df_user.groupby('TASKTYPE')[io_detailed_metrics_buffers_records].sum().nlargest(10, 'READDIRREC').reset_index()
                if not df_io_buffers_records.empty and df_io_buffers_records[io_detailed_metrics_buffers_records].sum().sum() > 0:
                    fig_io_buffers_records = px.bar(df_io_buffers_records, x='TASKTYPE', y=io_detailed_metrics_buffers_records,
                                                     title="Utilisation des Buffers et Enregistrements par Type de Tâche (Top 10)",
                                                     labels={'value': 'Nombre', 'variable': 'Métrique', 'TASKTYPE': 'Type de Tâche'},
                                                     barmode='group', color_discrete_sequence=px.colors.sequential.Plasma)
                    st.plotly_chart(fig_io_buffers_records, use_container_width=True)
                else:
                    st.info("Données insuffisantes pour l'utilisation des buffers et enregistrements après filtrage.")

            comm_metrics_filtered = ['DSQLCNT', 'SLI_CNT']
            if 'TASKTYPE' in df_user.columns and all(col in df_user.columns for col in comm_metrics_filtered) and df_user[comm_metrics_filtered].sum().sum() > 0:
                st.subheader("Analyse des Communications et Appels Système par Type de Tâche (DSQLCNT et SLI_CNT)")
                st.markdown("""
                    Ce graphique se concentre sur deux métriques clés pour les interactions des tâches avec d'autres systèmes :
                    * **DSQLCNT** : Nombre d'appels SQL dynamiques (requêtes de base de données générées dynamiquement). Un nombre élevé peut indiquer une forte interaction avec la base de données.
                    * **SLI_CNT** : Nombre d'appels SLI (System Level Interface). Ces appels représentent les interactions de bas niveau avec le système d'exploitation ou d'autres composants système.
                    Ces métriques sont essentielles pour diagnostiquer les problèmes de communication ou les dépendances externes.
                    """)
                df_comm_metrics = df_user.groupby('TASKTYPE')[comm_metrics_filtered].sum().nlargest(4, 'DSQLCNT').reset_index()
                if not df_comm_metrics.empty and df_comm_metrics[comm_metrics_filtered].sum().sum() > 0:
                    fig_comm_metrics = px.bar(df_comm_metrics, x='TASKTYPE', y=comm_metrics_filtered,
                                              title="Communications et Appels Système par Type de Tâche (Top 4)",
                                              labels={'value': 'Nombre / Temps (ms)', 'variable': 'Métrique', 'TASKTYPE': 'Type de Tâche'},
                                              barmode='group', color_discrete_sequence=px.colors.qualitative.Bold)
                    st.plotly_chart(fig_comm_metrics, use_container_width=True)
                else:
                    st.info("Données insuffisantes pour les métriques de communication et d'appels système après filtrage.")

            st.subheader("Aperçu des Données Utilisateurs Filtrées")
            st.dataframe(df_user.head())
        else:
            st.warning("Données utilisateurs non disponibles ou filtrées à vide.")

    elif st.session_state.current_section == "Statistiques Horaires":
        st.header("⏰ Statistiques Horaires du Système")
        df_times_data = dfs['times'].copy()
        if selected_tasktypes:
            if 'TASKTYPE' in df_times_data.columns:
                df_times_data = df_times_data[df_times_data['TASKTYPE'].isin(selected_tasktypes)]
            
        if not df_times_data.empty:
            st.subheader("Évolution du Nombre Total d'Appels Physiques (PHYCALLS) par Tranche Horaire")
            if 'TIME' in df_times_data.columns and 'PHYCALLS' in df_times_data.columns and df_times_data['PHYCALLS'].sum() > 0:
                df_times_data['HOUR_OF_DAY'] = df_times_data['TIME'].apply(lambda x: str(x).split(':')[0].zfill(2) if ':' in str(x) else str(x).zfill(2)[:2])
                hourly_counts = df_times_data.groupby('HOUR_OF_DAY')['PHYCALLS'].sum().reindex([
                    '00--06', '06--07', '07--08', '08--09', '09--10', '10--11', '11--12', '12--13',
                    '13--14', '14--15', '15--16', '16--17', '17--18', '18--19', '19--20', '20--21',
                    '21--22', '22--23', '23--00'
                ], fill_value=0)
                if not hourly_counts.empty and hourly_counts.sum() > 0:
                    fig_phycalls = px.line(hourly_counts.reset_index(),
                                           x='HOUR_OF_DAY', y='PHYCALLS',
                                           title="Total Appels Physiques par Tranche Horaire",
                                           labels={'HOUR_OF_DAY': 'Tranche Horaire', 'PHYCALLS': 'Total Appels Physiques'},
                                           color_discrete_sequence=px.colors.sequential.Cividis,
                                           markers=True)
                    st.plotly_chart(fig_phycalls, use_container_width=True)
            else:
                st.info("Colonnes 'TIME' ou 'PHYCALLS' manquantes ou PHYCALLS total est zéro/vide après filtrage.")

            st.subheader("Top 5 Tranches Horaires les plus Chargées (Opérations d'E/S)")
            io_cols = ['READDIRCNT', 'READSEQCNT', 'CHNGCNT']
            if all(col in df_times_data.columns for col in io_cols) and df_times_data[io_cols].sum().sum() > 0:
                df_times_data['TOTAL_IO'] = df_times_data['READDIRCNT'] + df_times_data['READSEQCNT'] + df_times_data['CHNGCNT']
                top_io_times = df_times_data.groupby('TIME')['TOTAL_IO'].sum().nlargest(5).sort_values(ascending=False)
                if not top_io_times.empty and top_io_times.sum() > 0:
                    fig_top_io = px.bar(top_io_times.reset_index(),
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
                avg_times_by_hour = df_times_data.groupby("TIME")[perf_cols].mean() / 1000.0
                avg_times_by_hour = avg_times_by_hour.reindex([
                    '00--06', '06--07', '07--08', '08--09', '09--10', '10--11', '11--12', '12--13',
                    '13--14', '14--15', '15--16', '16--17', '17--18', '18--19', '19--20', '20--21',
                    '21--22', '22--23', '23--00'
                ], fill_value=0)
                
                if not avg_times_by_hour.empty and avg_times_by_hour.sum().sum() > 0:
                    fig_avg_times = px.line(avg_times_by_hour.reset_index(),
                                            x='TIME', y=perf_cols,
                                            title="Temps Moyen (s) par Tranche Horaire",
                                            labels={'value': 'Temps Moyen (s)', 'variable': 'Métrique', 'TIME': 'Tranche Horaire'},
                                            color_discrete_sequence=px.colors.qualitative.Set1,
                                            markers=True)
                    st.plotly_chart(fig_avg_times, use_container_width=True)
                else:
                    st.info("Pas de données valides pour les temps moyens après filtrage.")
            else:
                st.info("Colonnes nécessaires (RESPTI, CPUTI, PROCTI, TIME) manquantes ou leur somme est zéro/vide après filtrage.")
            
            st.subheader("Aperçu des Données Horaires Filtrées")
            st.dataframe(df_times_data.head())
        else:
            st.warning("Données horaires (Times) non disponibles ou filtrées à vide.")

    elif st.session_state.current_section == "Décomposition des Tâches":
        st.header("⚙️ Décomposition des Types de Tâches")
        df_task = dfs['tasktimes'].copy()
        if selected_tasktypes:
            if 'TASKTYPE' in df_task.columns:
                df_task = df_task[df_task['TASKTYPE'].isin(selected_tasktypes)]

        if not df_task.empty:
            st.subheader("Répartition des Types de Tâches (TASKTYPE)")
            if 'TASKTYPE' in df_task.columns and 'COUNT' in df_task.columns and df_task['COUNT'].sum() > 0:
                task_counts = df_task.groupby('TASKTYPE')['COUNT'].sum().reset_index()
                task_counts.columns = ['TASKTYPE', 'Count']
                
                min_count_for_pie = task_counts['Count'].sum() * 0.01
                significant_tasks = task_counts[task_counts['Count'] >= min_count_for_pie]
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
                task_perf = df_task.groupby('TASKTYPE')[perf_cols_task].mean().nlargest(10, 'RESPTI') / 1000.0
                if not task_perf.empty and task_perf.sum().sum() > 0:
                    fig_task_perf = px.bar(task_perf.reset_index(), x='TASKTYPE', y=perf_cols_task,
                                           title="Top 10 TASKTYPE par Temps de Réponse et CPU (s)",
                                           labels={'value': 'Temps Moyen (s)', 'variable': 'Métrique', 'TASKTYPE': 'Type de Tâche'},
                                           barmode='group', color_discrete_sequence=px.colors.qualitative.Bold)
                    st.plotly_chart(fig_task_perf, use_container_width=True)
                else:
                    st.info("Pas de données valides pour les temps de performance des tâches après filtrage.")
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
                df_wait_gui = df_task.groupby('TASKTYPE')[wait_gui_metrics].sum().nlargest(10, 'QUEUETI').reset_index()
                if not df_wait_gui.empty and df_wait_gui[wait_gui_metrics].sum().sum() > 0:
                    fig_wait_gui = px.bar(df_wait_gui, x='TASKTYPE', y=wait_gui_metrics,
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
                Ces métriques sont essentielles pour identifier les tâches gourmandes en E/S et évaluer l'efficacité de l'accès aux données.
                """)
            io_metrics_tasktimes = ['READDIRCNT', 'READSEQCNT', 'CHNGCNT', 'PHYREADCNT', 'PHYCHNGREC']
            if 'TASKTYPE' in df_task.columns and all(col in df_task.columns for col in io_metrics_tasktimes) and df_task[io_metrics_tasktimes].sum().sum() > 0:
                df_io_tasktimes = df_task.groupby('TASKTYPE')[io_metrics_tasktimes].sum().nlargest(10, 'PHYREADCNT').reset_index()
                if not df_io_tasktimes.empty and df_io_tasktimes[io_metrics_tasktimes].sum().sum() > 0:
                    fig_io_tasktimes = px.bar(df_io_tasktimes, x='TASKTYPE', y=io_metrics_tasktimes,
                                              title="Opérations d'E/S par Type de Tâche (Top 10)",
                                              labels={'value': 'Nombre d\'Opérations', 'variable': 'Métrique E/S', 'TASKTYPE': 'Type de Tâche'},
                                              barmode='group', color_discrete_sequence=px.colors.sequential.Greens)
                    st.plotly_chart(fig_io_tasktimes, use_container_width=True)
                else:
                    st.info("Données insuffisantes pour l'analyse des opérations d'E/S après filtrage.")
            else:
                st.info("Colonnes d'E/S manquantes ou leurs sommes sont zéro/vides après filtrage.")


            st.subheader("Aperçu des Données des Temps de Tâches Filtrées")
            st.dataframe(df_task.head())
        else:
            st.warning("Données des temps de tâches non disponibles ou filtrées à vide.")

    elif st.session_state.current_section == "Insights Hitlist DB":
        st.header("🔍 Insights de la Base de Données Hitlist")
        df_hitlist = df_hitlist_filtered.copy() # Utiliser le DF filtré par date
        if selected_accounts:
            df_hitlist = df_hitlist[df_hitlist['ACCOUNT'].isin(selected_accounts)]
        if selected_reports:
            df_hitlist = df_hitlist[df_hitlist['REPORT'].isin(selected_reports)]
        if selected_tasktypes:
            df_hitlist = df_hitlist[df_hitlist['TASKTYPE'].isin(selected_tasktypes)]

        if not df_hitlist.empty:
            if 'FULL_DATETIME' in df_hitlist.columns and pd.notna(df_hitlist['FULL_DATETIME'].min()) and pd.notna(df_hitlist['FULL_DATETIME'].max()):
                st.info(f"Données affichées pour la période: "
                        f"**{df_hitlist['FULL_DATETIME'].min().strftime('%Y-%m-%d %H:%M')}** à "
                        f"**{df_hitlist['FULL_DATETIME'].max().strftime('%Y-%m-%d %H:%M')}**")
            else:
                st.info("La plage de dates pour HITLIST_DATABASE n'a pas pu être déterminée ou est vide.")

            st.subheader("Tendance du Temps de Réponse Moyen et Temps CPU par Heure (Hitlist DB)")
            hitlist_perf_cols = ['RESPTI', 'CPUTI']
            if 'FULL_DATETIME' in df_hitlist.columns and all(col in df_hitlist.columns for col in hitlist_perf_cols) and pd.api.types.is_datetime64_any_dtype(df_hitlist['FULL_DATETIME']) and not df_hitlist['FULL_DATETIME'].isnull().all() and df_hitlist[hitlist_perf_cols].sum().sum() > 0:
                hourly_metrics = df_hitlist.set_index('FULL_DATETIME')[hitlist_perf_cols].resample('H').mean().dropna()
                if not hourly_metrics.empty and hourly_metrics.sum().sum() > 0:
                    fig_hourly_perf = px.line(hourly_metrics.reset_index(), x='FULL_DATETIME', y=hitlist_perf_cols,
                                              title="Tendance Horaire du Temps de Réponse et CPU (s)",
                                              labels={'FULL_DATETIME': 'Heure', 'value': 'Temps Moyen (s)', 'variable': 'Métrique'},
                                              color_discrete_sequence=px.colors.qualitative.Dark2)
                    fig_hourly_perf.update_xaxes(dtick="H1", tickformat="%H:%M")
                    st.plotly_chart(fig_hourly_perf, use_container_width=True)
                else:
                    st.info("Pas de données valides pour la tendance horaire de performance Hitlist DB après filtrage.")
            else:
                st.info("Colonnes 'FULL_DATETIME', 'RESPTI' ou 'CPUTI' manquantes/invalides dans Hitlist DB ou leurs totaux sont zéro/vide.")

            st.subheader("Top 10 Rapports (REPORT) par Appels Base de Données (DBCALLS)")
            if 'REPORT' in df_hitlist.columns and 'DBCALLS' in df_hitlist.columns and df_hitlist['DBCALLS'].sum() > 0:
                top_reports_dbcalls = df_hitlist.groupby('REPORT')['DBCALLS'].sum().nlargest(10)
                if not top_reports_dbcalls.empty and top_reports_dbcalls.sum() > 0:
                    fig_top_reports_db = px.bar(top_reports_dbcalls.reset_index(), x='REPORT', y='DBCALLS',
                                                title="Top 10 Rapports par Total Appels DB",
                                                labels={'REPORT': 'Rapport', 'DBCALLS': 'Total Appels DB'},
                                                color='DBCALLS', color_continuous_scale=px.colors.sequential.dense)
                    st.plotly_chart(fig_top_reports_db, use_container_width=True)
                else:
                    st.info("Pas de données valides pour les Top 10 Rapports par Appels DB Hitlist après filtrage.")
            else:
                st.info("Colonnes 'REPORT' ou 'DBCALLS' manquantes dans Hitlist DB ou DBCALLS total est zéro/vide après filtrage.")

            st.subheader("Temps Moyen de Traitement (PROCTI) par Top 5 Types de Tâches (TASKTYPE)")
            if 'TASKTYPE' in df_hitlist.columns and 'PROCTI' in df_hitlist.columns and df_hitlist['PROCTI'].sum() > 0:
                top_5_tasktypes = df_hitlist['TASKTYPE'].value_counts().nlargest(5).index.tolist()
                df_filtered_tasktype = df_hitlist.loc[df_hitlist['TASKTYPE'].isin(top_5_tasktypes)].copy()
                
                if not df_filtered_tasktype.empty:
                    avg_procti_by_tasktype = df_filtered_tasktype.groupby('TASKTYPE')['PROCTI'].mean().sort_values(ascending=False) / 1000.0
                    if not avg_procti_by_tasktype.empty and avg_procti_by_tasktype.sum() > 0:
                        fig_procti_bar = px.bar(avg_procti_by_tasktype.reset_index(), x='TASKTYPE', y='PROCTI',
                                                title="Temps Moyen de Traitement (s) par Top 5 TASKTYPE",
                                                labels={'TASKTYPE': 'Type de Tâche', 'PROCTI': 'Temps Moyen de Traitement (s)'},
                                                color='PROCTI', color_continuous_scale=px.colors.sequential.Sunset)
                        st.plotly_chart(fig_procti_bar, use_container_width=True)
                    else:
                        st.info("Pas de données valides pour le temps moyen de traitement par TASKTYPE après filtrage.")
                else:
                    st.info("Pas de données pour les Top 5 TASKTYPE pour le graphique (Hitlist DB) après filtrage.")
            else:
                st.info("Colonnes 'TASKTYPE' ou 'PROCTI' manquantes dans Hitlist DB ou PROCTI total est zéro/vide après filtrage.")
            
            st.subheader("Aperçu des Données Hitlist DB Filtrées")
            st.dataframe(df_hitlist.head())
        else:
            st.warning("Données Hitlist Database non disponibles ou filtrées à vide.")

    elif st.session_state.current_section == "Performance des Processus de Travail":
        st.header("📈 Performance des Processus de Travail (Work Processes)")
        df_perf = dfs['performance'].copy()
        if selected_wp_types:
            df_perf = df_perf[df_perf['WP_TYP'].isin(selected_wp_types)]

        if not df_perf.empty:
            st.subheader("Temps CPU Total (WP_CPU_SECONDS) par Type de Processus de Travail (WP_TYP)")
            if 'WP_TYP' in df_perf.columns and 'WP_CPU_SECONDS' in df_perf.columns and df_perf['WP_CPU_SECONDS'].sum() > 0:
                cpu_by_wp_type = df_perf.groupby('WP_TYP')['WP_CPU_SECONDS'].sum().sort_values(ascending=False)
                fig_cpu_wp_type = px.bar(cpu_by_wp_type.reset_index(),
                                         x='WP_TYP', y='WP_CPU_SECONDS',
                                         title="Temps CPU Total par Type de Processus de Travail",
                                         labels={'WP_CPU_SECONDS': 'Temps CPU Total (secondes)', 'WP_TYP': 'Type de Processus de Travail'},
                                         color='WP_CPU_SECONDS', color_continuous_scale=px.colors.sequential.Plasma)
                st.plotly_chart(fig_cpu_wp_type, use_container_width=True)
            else:
                st.info("Colonnes 'WP_TYP' ou 'WP_CPU_SECONDS' manquantes ou le temps CPU total est zéro/vide après filtrage.")

            st.subheader("Temps d'Attente I/O Total (WP_IWAIT_SECONDS) par Type de Processus de Travail (WP_TYP)")
            if 'WP_TYP' in df_perf.columns and 'WP_IWAIT_SECONDS' in df_perf.columns and df_perf['WP_IWAIT_SECONDS'].sum() > 0:
                iowait_by_wp_type = df_perf.groupby('WP_TYP')['WP_IWAIT_SECONDS'].sum().sort_values(ascending=False)
                fig_iowait_wp_type = px.bar(iowait_by_wp_type.reset_index(),
                                            x='WP_TYP', y='WP_IWAIT_SECONDS',
                                            title="Temps d'Attente I/O Total par Type de Processus de Travail",
                                            labels={'WP_IWAIT_SECONDS': "Temps d'Attente I/O Total (secondes)", 'WP_TYP': 'Type de Processus de Travail'},
                                            color='WP_IWAIT_SECONDS', color_continuous_scale=px.colors.sequential.Viridis)
                st.plotly_chart(fig_iowait_wp_type, use_container_width=True)
            else:
                st.info("Colonnes 'WP_TYP' ou 'WP_IWAIT_SECONDS' manquantes ou le temps d'attente I/O total est zéro/vide après filtrage.")

            st.subheader("Statut des Processus de Travail (WP_STATUS)")
            if 'WP_STATUS' in df_perf.columns and not df_perf.empty:
                status_counts = df_perf['WP_STATUS'].value_counts().reset_index()
                status_counts.columns = ['WP_STATUS', 'Count']
                fig_status = px.pie(status_counts, values='Count', names='WP_STATUS',
                                    title="Répartition des Statuts des Processus de Travail",
                                    color_discrete_sequence=px.colors.sequential.RdBu)
                st.plotly_chart(fig_status, use_container_width=True)
            else:
                st.info("Colonne 'WP_STATUS' manquante ou vide après filtrage.")
        else:
            st.warning("Le dataset 'performance' est vide ou ne contient pas les colonnes requises après filtrage.")

    elif st.session_state.current_section == "Résumé des Traces de Performance SQL":
        st.header("📊 Résumé des Traces de Performance SQL")
        df_sql = dfs['sql_trace_summary'].copy()

        if not df_sql.empty:
            st.subheader("Top 10 des Statements SQL par Temps d'Exécution Total (EXECTIME)")
            if all(col in df_sql.columns for col in ['SQLSTATEM', 'EXECTIME', 'TOTALEXEC', 'RECPROCNUM']) and df_sql['EXECTIME'].sum() > 0:
                top_sql_exec_time = df_sql.groupby('SQLSTATEM')[['EXECTIME', 'TOTALEXEC', 'RECPROCNUM']].sum().nlargest(10, 'EXECTIME')
                fig_top_sql_exec_time = px.bar(top_sql_exec_time.reset_index(),
                                               x='SQLSTATEM', y='EXECTIME',
                                               title="Top 10 Statements SQL par Temps d'Exécution Total",
                                               labels={'EXECTIME': "Temps d'Exécution Total (ms)", 'SQLSTATEM': 'Statement SQL'},
                                               hover_data=['TOTALEXEC', 'RECPROCNUM'],
                                               color='EXECTIME', color_continuous_scale=px.colors.sequential.Sunset)
                st.plotly_chart(fig_top_sql_exec_time, use_container_width=True)
            else:
                st.info("Colonnes nécessaires (SQLSTATEM, EXECTIME, TOTALEXEC, RECPROCNUM) manquantes ou le temps d'exécution total est zéro/vide après filtrage.")

            st.subheader("Top 10 des Statements SQL par Nombre Total d'Exécutions (TOTALEXEC)")
            if all(col in df_sql.columns for col in ['SQLSTATEM', 'TOTALEXEC', 'EXECTIME', 'RECPROCNUM']) and df_sql['TOTALEXEC'].sum() > 0:
                top_sql_total_exec = df_sql.groupby('SQLSTATEM')[['TOTALEXEC', 'EXECTIME', 'RECPROCNUM']].sum().nlargest(10, 'TOTALEXEC')
                fig_top_sql_total_exec = px.bar(top_sql_total_exec.reset_index(),
                                                x='SQLSTATEM', y='TOTALEXEC',
                                                title="Top 10 Statements SQL par Nombre Total d'Exécutions",
                                                labels={'TOTALEXEC': "Nombre Total d'Exécutions", 'SQLSTATEM': 'Statement SQL'},
                                                hover_data=['EXECTIME', 'RECPROCNUM'],
                                                color='TOTALEXEC', color_continuous_scale=px.colors.sequential.Plasma)
                st.plotly_chart(fig_top_sql_total_exec, use_container_width=True)
            else:
                st.info("Colonnes nécessaires (SQLSTATEM, TOTALEXEC, EXECTIME, RECPROCNUM) manquantes ou le total des exécutions est zéro/vide après filtrage.")

            st.subheader("Temps Moyen par Enregistrement (AVGTPERREC) par Serveur")
            if all(col in df_sql.columns for col in ['SERVERNAME', 'AVGTPERREC', 'RECPROCNUM']) and df_sql['AVGTPERREC'].sum() > 0:
                avg_tper_rec_server = df_sql.groupby('SERVERNAME')[['AVGTPERREC', 'RECPROCNUM']].mean().sort_values(by='AVGTPERREC', ascending=False)
                fig_avg_tper_rec_server = px.bar(avg_tper_rec_server.reset_index(),
                                                 x='SERVERNAME', y='AVGTPERREC',
                                                 title="Temps Moyen par Enregistrement (AVGTPERREC) par Serveur",
                                                 labels={'AVGTPERREC': 'Temps Moyen par Enregistrement (ms)', 'SERVERNAME': 'Nom du Serveur'},
                                                 hover_data=['RECPROCNUM'],
                                                 color='AVGTPERREC', color_continuous_scale=px.colors.sequential.Cividis)
                st.plotly_chart(fig_avg_tper_rec_server, use_container_width=True)
            else:
                st.info("Colonnes nécessaires (SERVERNAME, AVGTPERREC, RECPROCNUM) manquantes ou le temps moyen par enregistrement total est zéro/vide après filtrage.")
        else:
            st.warning("Le dataset 'sql_trace_summary' est vide ou ne contient pas les colonnes requises après filtrage.")

    elif st.session_state.current_section == "Analyse des Utilisateurs":
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
                    st.info("Aucune donnée de date de dernier logon valide après filtrage.")
            else:
                st.info("Colonne 'GLTGB_DATE' manquante ou ne contient pas de dates valides après filtrage.")

            st.subheader("Aperçu des Données Utilisateurs Filtrées")
            st.dataframe(df_usr02.head())
        else:
            st.warning("Données utilisateurs (USR02) non disponibles ou filtrées à vide.")

    elif st.session_state.current_section == "Détection d'Anomalies":
        st.header("🚨 Détection d'Anomalies (Temps de Réponse - Hitlist DB)")
        st.markdown("""
            Cette section utilise l'algorithme **Isolation Forest** pour détecter les anomalies dans les temps de réponse (`RESPTI`) des transactions SAP.
            Les anomalies sont des points de données qui s'écartent significativement du comportement normal.
            """)
        
        df_anomalies_hitlist = df_hitlist_filtered.copy() # Utiliser le DataFrame déjà filtré par date
        
        if not df_anomalies_hitlist.empty and 'RESPTI' in df_anomalies_hitlist.columns and 'FULL_DATETIME' in df_anomalies_hitlist.columns:
            df_anomalies_hitlist['RESPTI'] = pd.to_numeric(df_anomalies_hitlist['RESPTI'], errors='coerce')
            df_anomalies_hitlist = df_anomalies_hitlist[np.isfinite(df_anomalies_hitlist['RESPTI'])].copy()
            
            if not df_anomalies_hitlist.empty and df_anomalies_hitlist['RESPTI'].nunique() > 1:
                contamination_value = st.slider(
                    "Proportion attendue d'anomalies (contamination)",
                    min_value=0.001, max_value=0.1, value=0.01, step=0.001,
                    help="La proportion d'anomalies dans le dataset. Une valeur de 0.01 signifie 1% d'anomalies attendues."
                )

                X = df_anomalies_hitlist[['RESPTI']]

                model = IsolationForest(contamination=contamination_value, random_state=42)
                df_anomalies_hitlist['anomaly_score'] = model.decision_function(X)
                df_anomalies_hitlist['anomaly_prediction'] = model.predict(X)

                df_anomalies_hitlist['is_anomaly'] = df_anomalies_hitlist['anomaly_prediction'].apply(lambda x: "Oui" if x == -1 else "Non")

                st.write(f"Nombre total de transactions analysées : **{len(df_anomalies_hitlist)}**")
                num_anomalies = df_anomalies_hitlist[df_anomalies_hitlist['is_anomaly'] == 'Oui'].shape[0]
                st.write(f"Nombre d'anomalies détectées (avec contamination de {contamination_value*100:.1f}%) : **{num_anomalies}**")

                st.markdown("### Visualisation des Anomalies de Temps de Réponse")
                fig_scatter_anomalies = px.scatter(
                    df_anomalies_hitlist,
                    x='FULL_DATETIME',
                    y='RESPTI',
                    color='is_anomaly',
                    title='Temps de Réponse (RESPTI) avec Anomalies Détectées',
                    labels={'FULL_DATETIME': 'Horodatage', 'RESPTI': 'Temps de Réponse (ms)'},
                    hover_data=['tcode', 'program', 'user', 'respti', 'is_anomaly', 'anomaly_score'],
                    color_discrete_map={'Oui': 'red', 'Non': 'blue'}
                )
                fig_scatter_anomalies.update_traces(marker=dict(size=5, opacity=0.7))
                fig_scatter_anomalies.update_layout(hovermode="x unified")
                st.plotly_chart(fig_scatter_anomalies, use_container_width=True)

                st.markdown("### Top 10 des Transactions Anormales (par score d'anomalie le plus bas)")
                anomalies_df_display = df_anomalies_hitlist[df_anomalies_hitlist['is_anomaly'] == 'Oui'].sort_values(by='anomaly_score').head(10)
                if not anomalies_df_display.empty:
                    st.dataframe(anomalies_df_display[['FULL_DATETIME', 'tcode', 'program', 'user', 'RESPTI', 'anomaly_score']])
                else:
                    st.info("Aucune anomalie détectée avec les paramètres actuels et les données filtrées.")
            else:
                st.warning("Données insuffisantes ou valeurs uniques pour exécuter la détection d'anomalies sur 'RESPTI' après filtrage.")
        else:
            st.warning("Le dataset 'hitlist_db' n'est pas disponible, ou les colonnes 'RESPTI' ou 'FULL_DATETIME' sont manquantes/vides après filtrage pour la détection d'anomalies.")


# Option pour afficher tous les DataFrames (utile pour le débogage)
with st.expander("🔍 Afficher tous les DataFrames chargés (pour débogage)"):
    for key, df in dfs.items():
        st.subheader(f"DataFrame: {key} (Taille: {len(df)} lignes)")
        st.dataframe(df.head())
        if st.checkbox(f"Afficher les informations de '{key}' (df.info())", key=f"info_{key}"):
            buffer = io.StringIO()
            df.info(buf=buffer)
            st.text(buffer.getvalue())

st.markdown("---")
st.markdown("Développé avec ❤️ et Streamlit.")
