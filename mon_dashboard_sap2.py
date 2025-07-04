import streamlit as st
import pandas as pd
import plotly.express as px
import numpy as np
import io
import re # Importation nécessaire pour les expressions régulières
import plotly.figure_factory as ff # Importation ajoutée pour create_distplot

# --- Chemins vers vos fichiers de données ---
DATA_PATHS = {
    "memory": r"C:\Users\Farouha\memory_final_cleaned_clean.xlsx",
    "hitlist_db": r"C:\Users\Farouha\HITLIST_DATABASE_final_cleaned_clean.xlsx",
    "times": r"C:\Users\Farouha\Times_final_cleaned_clean.xlsx",
    "tasktimes": r"C:\Users\Farouha\TASKTIMES_final_cleaned_clean.xlsx",
    "usertcode": r"C:\Users\Farouha\USERTCODE_cleaned.xlsx",
    "performance": r"C:\Users\Farouha\AL_GET_PERFORMANCE_final_cleaned_clean.xlsx",
    "sql_trace_summary": r"C:\Users\Farouha\performance_trace_summary_final_cleaned_clean.xlsx",
    "usr02": r"C:\Users\Farouha\usr02_data.xlsx", # Nouveau dataset ajouté
}

# --- Configuration de la page Streamlit ---
# Pour une interface plus "blanche" et propre, Streamlit utilise par défaut un thème clair.
# Nous nous assurons que le layout est large pour une meilleure utilisation de l'espace.
st.set_page_config(layout="wide", page_title="Tableau de Bord SAP Complet Multi-Sources")

# --- Fonctions de Nettoyage et Chargement des Données (avec cache) ---

def clean_string_column(series, default_value="Non défini"):
    """
    Nettoie une série de type string : supprime espaces, remplace NaN/vides/caractères non imprimables.
    """
    # Convertir en string, supprimer les espaces blancs, remplacer les NaN et les chaînes vides
    cleaned_series = series.astype(str).str.strip()
    # Remplacer les caractères non imprimables et les espaces multiples par un seul espace, puis strip
    # Utilisation de re.sub pour la compatibilité avec les expressions régulières sur des chaînes
    cleaned_series = cleaned_series.apply(lambda x: re.sub(r'[^\x20-\x7E\s]+', ' ', x).strip()) # Plus agressif sur les non-ASCII et espaces
    # Remplacer 'nan' (string) et les chaînes vides résultantes par la valeur par défaut
    cleaned_series = cleaned_series.replace({'nan': default_value, '': default_value, ' ': default_value})
    return cleaned_series

def clean_column_names(df):
    """
    Nettoie les noms de colonnes : supprime les espaces, les caractères invisibles,
    et s'assure qu'ils sont valides pour l'accès.
    """
    new_columns = []
    for col in df.columns:
        # Step 1: Remove non-printable ASCII characters (control characters) using re.sub
        cleaned_col = re.sub(r'[\x00-\x1F\x7F-\x9F]', '', str(col)).strip()
        # Step 2: Replace any character that is not alphanumeric or underscore with an underscore
        cleaned_col = re.sub(r'[^a-zA-Z0-9_]', '_', cleaned_col)
        # Step 3: Replace multiple underscores with a single underscore
        cleaned_col = re.sub(r'_+', '_', cleaned_col)
        # Step 4: Remove leading/trailing underscores
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
        elif len(parts) == 1: # Handle cases like '0' or '123' (assume seconds if no colon)
            return int(float(parts[0]))
        else:
            return 0 # Invalid format
    except ValueError:
        return 0 # Handle non-numeric parts

def clean_numeric_with_comma(series):
    """
    Nettoie une série de strings numériques qui peuvent contenir des virgules
    comme séparateurs de milliers ou décimaux, et la convertit en float.
    """
    # Convertir en string, supprimer les espaces, et remplacer la virgule décimale par un point
    # Supprimer les points comme séparateurs de milliers (s'ils existent)
    cleaned_series = series.astype(str).str.replace(' ', '').str.replace('.', '', regex=False).str.replace(',', '.', regex=False)
    return pd.to_numeric(cleaned_series, errors='coerce').fillna(0)


@st.cache_data
def load_and_process_data(file_key, path):
    """Charge et nettoie un fichier Excel/CSV."""
    df = pd.DataFrame() # Initialiser df comme DataFrame vide
    try:
        if path.lower().endswith('.xlsx'):
            df = pd.read_excel(path)
        elif path.lower().endswith('.csv'):
            df = pd.read_csv(path)
        else:
            st.error(f"Format de fichier non supporté pour {file_key}: {path}")
            return pd.DataFrame()

        df = clean_column_names(df.copy()) # Nettoyer les noms de colonnes dès le chargement

        # --- Gestion spécifique des types de données et valeurs manquantes ---
        if file_key == "memory":
            numeric_cols = ['MEMSUM', 'PRIVSUM', 'USEDBYTES', 'MAXBYTES', 'MAXBYTESDI', 'PRIVCOUNT', 'RESTCOUNT', 'COUNTER']
            for col in numeric_cols:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0) # Remplacer NaN par 0 pour num
            
            if 'ACCOUNT' in df.columns:
                df['ACCOUNT'] = clean_string_column(df['ACCOUNT'], 'Compte Inconnu')
            if 'MANDT' in df.columns:
                df['MANDT'] = clean_string_column(df['MANDT'], 'MANDT Inconnu')
            if 'TASKTYPE' in df.columns: # Added for new visualization
                df['TASKTYPE'] = clean_string_column(df['TASKTYPE'], 'Type de Tâche Inconnu')


            # Ne pas tenter de créer FULL_DATETIME si les colonnes ne sont pas là pour 'memory'
            # et ne pas afficher de message si elles sont absentes.
            if 'ENDDATE' in df.columns and 'ENDTIME' in df.columns:
                df['ENDTIME_STR'] = df['ENDTIME'].astype(str).str.zfill(6)
                df['FULL_DATETIME'] = pd.to_datetime(df['ENDDATE'].astype(str) + df['ENDTIME_STR'], format='%Y%m%d%H%M%S', errors='coerce')
                df.drop(columns=['ENDTIME_STR'], inplace=True, errors='ignore')
                
                # Diagnostic uniquement si FULL_DATETIME est créé mais contient des NaN
                if df['FULL_DATETIME'].isnull().any() and not df['FULL_DATETIME'].isnull().all():
                    st.info(f"Info: La colonne 'FULL_DATETIME' pour '{file_key}' contient des valeurs non valides ({df['FULL_DATETIME'].isnull().sum()} NaN) après conversion. Les lignes concernées seront ignorées pour la tendance.")
            elif 'FULL_DATETIME' in df.columns and pd.api.types.is_datetime64_any_dtype(df['FULL_DATETIME']):
                 # Already datetime, just check for NaNs
                 if df['FULL_DATETIME'].isnull().any() and not df['FULL_DATETIME'].isnull().all():
                    st.info(f"Info: La colonne 'FULL_DATETIME' pour '{file_key}' contient des valeurs non valides ({df['FULL_DATETIME'].isnull().sum()} NaN) après reconversion. Les lignes concernées seront ignorées pour la tendance.")
            elif 'FULL_DATETIME' in df.columns: # If it exists but is not datetime, try converting
                 df['FULL_DATETIME'] = pd.to_datetime(df['FULL_DATETIME'], errors='coerce')
                 if df['FULL_DATETIME'].isnull().any() and not df['FULL_DATETIME'].isnull().all():
                    st.info(f"Info: La colonne 'FULL_DATETIME' pour '{file_key}' contient des valeurs non valides ({df['FULL_DATETIME'].isnull().sum()} NaN) après reconversion. Les lignes concernées seront ignorées pour la tendance.")
            
            # S'assurer que les colonnes critiques existent avant de les utiliser pour dropna
            subset_cols_memory = []
            if 'USEDBYTES' in df.columns:
                subset_cols_memory.append('USEDBYTES')
            if 'ACCOUNT' in df.columns:
                subset_cols_memory.append('ACCOUNT')
            if subset_cols_memory: # Seulement si au moins une colonne critique est présente
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
            elif 'FULL_DATETIME' in df.columns and pd.api.types.is_datetime64_any_dtype(df['FULL_DATETIME']):
                 # Already datetime, just check for NaNs
                 pass
            elif 'FULL_DATETIME' in df.columns: # If it exists but is not datetime, try converting
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
            # Removed 'PHYCALLS' from critical columns as per user request
            if 'COUNT' in df.columns: subset_cols_times.append('COUNT')
            if subset_cols_times:
                df.dropna(subset=subset_cols_times, inplace=True)
            
            if 'TIME' in df.columns:
                df['TIME'] = clean_string_column(df['TIME'])
            if 'TASKTYPE' in df.columns:
                df['TASKTYPE'] = clean_string_column(df['TASKTYPE'])
            if 'ENTRY_ID' in df.columns:
                df['ENTRY_ID'] = clean_string_column(df['ENTRY_ID'])

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
            
            # Add FULL_DATETIME creation for usertcode
            if 'ENDDATE' in df.columns and 'ENDTIME' in df.columns:
                df['ENDTIME_STR'] = df['ENDTIME'].astype(str).str.zfill(6)
                df['FULL_DATETIME'] = pd.to_datetime(df['ENDDATE'].astype(str) + df['ENDTIME_STR'], format='%Y%m%d%H%M%S', errors='coerce')
                df.drop(columns=['ENDTIME_STR'], inplace=True, errors='ignore')
                if df['FULL_DATETIME'].isnull().any() and not df['FULL_DATETIME'].isnull().all():
                    st.info(f"Info: La colonne 'FULL_DATETIME' pour '{file_key}' contient des valeurs non valides ({df['FULL_DATETIME'].isnull().sum()} NaN) après conversion. Les lignes concernées seront ignorées pour la tendance.")
            elif 'FULL_DATETIME' in df.columns and pd.api.types.is_datetime64_any_dtype(df['FULL_DATETIME']):
                 # Already datetime, just check for NaNs
                 pass
            elif 'FULL_DATETIME' in df.columns: # If it exists but is not datetime, try converting
                 df['FULL_DATETIME'] = pd.to_datetime(df['FULL_DATETIME'], errors='coerce')
                 if df['FULL_DATETIME'].isnull().any() and not df['FULL_DATETIME'].isnull().all():
                    st.info(f"Info: La colonne 'FULL_DATETIME' pour '{file_key}' contient des valeurs non valides ({df['FULL_DATETIME'].isnull().sum()} NaN) après reconversion. Les lignes concernées seront ignorées pour la tendance.")

            critical_usertcode_cols = []
            if 'RESPTI' in df.columns: critical_usertcode_cols.append('RESPTI')
            if 'ACCOUNT' in df.columns: critical_usertcode_cols.append('ACCOUNT')
            if 'COUNT' in df.columns: critical_usertcode_cols.append('COUNT')
            
            if critical_usertcode_cols:
                df.dropna(subset=critical_usertcode_cols, inplace=True)
            
            for col in ['TASKTYPE', 'ENTRY_ID', 'ACCOUNT']:
                if col in df.columns:
                    df[col] = clean_string_column(df[col])

        elif file_key == "performance": # Nouveau bloc pour AL_GET_PERFORMANCE
            # Convertir WP_CPU de MM:SS en secondes
            if 'WP_CPU' in df.columns:
                df['WP_CPU_SECONDS'] = df['WP_CPU'].apply(convert_mm_ss_to_seconds)
            
            # Convertir WP_IWAIT en secondes (s'il est en ms, diviser par 1000)
            if 'WP_IWAIT' in df.columns:
                # Assurez-vous que WP_IWAIT est numérique avant de diviser
                df['WP_IWAIT'] = pd.to_numeric(df['WP_IWAIT'], errors='coerce').fillna(0)
                # Si WP_IWAIT est en ms, le convertir en secondes
                # C'est une hypothèse, à ajuster si les données sont déjà en secondes
                df['WP_IWAIT_SECONDS'] = df['WP_IWAIT'] / 1000.0 
            else:
                df['WP_IWAIT_SECONDS'] = 0 # Default if column is missing

            # Nettoyage des colonnes string
            for col in ['WP_SEMSTAT', 'WP_IACTION', 'WP_ITYPE', 'WP_RESTART', 'WP_ISTATUS', 'WP_TYP', 'WP_STATUS']:
                if col in df.columns:
                    df[col] = clean_string_column(df[col])
            
            # Nettoyage des colonnes numériques
            numeric_cols_perf = ['WP_NO', 'WP_IRESTRT', 'WP_PID', 'WP_INDEX']
            for col in numeric_cols_perf:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
            
            # Supprimer les lignes avec des valeurs critiques manquantes si nécessaire
            subset_cols_perf = []
            if 'WP_CPU_SECONDS' in df.columns: subset_cols_perf.append('WP_CPU_SECONDS')
            if 'WP_STATUS' in df.columns: subset_cols_perf.append('WP_STATUS')
            if subset_cols_perf:
                df.dropna(subset=subset_cols_perf, inplace=True)
        
        elif file_key == "sql_trace_summary": # Nouveau bloc pour performance_trace_summary
            # Nettoyage des colonnes numériques avec virgule/espace
            numeric_cols_sql = ['TOTALEXEC', 'IDENTSEL', 'EXECTIME', 'RECPROCNUM', 'TIMEPEREXE', 'RECPEREXE', 'AVGTPERREC', 'MINTPERREC']
            for col in numeric_cols_sql:
                if col in df.columns:
                    df[col] = clean_numeric_with_comma(df[col])
            
            # Nettoyage des colonnes string
            for col in ['SQLSTATEM', 'SERVERNAME', 'TRANS_ID']:
                if col in df.columns:
                    df[col] = clean_string_column(df[col])
            
            # Supprimer les lignes avec des valeurs critiques manquantes si nécessaire
            subset_cols_sql = []
            if 'EXECTIME' in df.columns: subset_cols_sql.append('EXECTIME')
            if 'TOTALEXEC' in df.columns: subset_cols_sql.append('TOTALEXEC')
            if 'SQLSTATEM' in df.columns: subset_cols_sql.append('SQLSTATEM')
            if subset_cols_sql:
                df.dropna(subset=subset_cols_sql, inplace=True)

        elif file_key == "usr02": # Nouveau bloc pour usr02_data.xlsx
            # Conversion de GLTGB_DATE
            if 'GLTGB_DATE' in df.columns:
                # Convertir en string d'abord pour s'assurer du format YYYYMMDD, puis en datetime
                df['GLTGB_DATE'] = pd.to_datetime(df['GLTGB_DATE'].astype(str), format='%Y%m%d', errors='coerce')
                # Supprimer les dates '00000000' qui deviennent NaT
                df.dropna(subset=['GLTGB_DATE'], inplace=True)
            
            # Nettoyage des colonnes string
            for col in ['BNAME', 'USTYP', 'CLASS', 'ACCNT_TYP']:
                if col in df.columns:
                    df[col] = clean_string_column(df[col])
            
            # Supprimer les lignes avec des valeurs critiques manquantes si nécessaire
            subset_cols_usr02 = []
            if 'BNAME' in df.columns: subset_cols_usr02.append('BNAME')
            if 'USTYP' in df.columns: subset_cols_usr02.append('USTYP')
            if subset_cols_usr02:
                df.dropna(subset=subset_cols_usr02, inplace=True)


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
tab_titles = ["Analyse Mémoire", "Transactions Utilisateurs", "Statistiques Horaires", "Décomposition des Tâches", "Insights Hitlist DB", "Performance des Processus de Travail", "Résumé des Traces de Performance SQL", "Analyse des Utilisateurs"]

# Initialize session state for current_section if it doesn't exist
if 'current_section' not in st.session_state:
    st.session_state.current_section = tab_titles[0] # Default to the first section

st.sidebar.header("Navigation Rapide")
# Use st.sidebar.radio for navigation
selected_section = st.sidebar.radio(
    "Accéder à la section :",
    tab_titles,
    index=tab_titles.index(st.session_state.current_section) # Set initial selection
)

# Update session state based on radio selection
st.session_state.current_section = selected_section

# Vérifier si au moins une source de données a été chargée pour afficher le dashboard
if all(df.empty for df in dfs.values()):
    st.error("Aucune source de données n'a pu être chargée. Le dashboard ne peut pas s'afficher. Veuillez vérifier les chemins et les fichiers.")
else:
    # --- Sidebar pour les filtres globaux ---
    st.sidebar.header("Filtres")

    # Filtre par ACCOUNT (commun à memory, usertcode, hitlist_db)
    all_accounts = pd.Index([])
    if not dfs['memory'].empty and 'ACCOUNT' in dfs['memory'].columns:
        all_accounts = all_accounts.union(dfs['memory']['ACCOUNT'].dropna().unique())
    if not dfs['usertcode'].empty and 'ACCOUNT' in dfs['usertcode'].columns:
        all_accounts = all_accounts.union(dfs['usertcode']['ACCOUNT'].dropna().unique())
    if not dfs['hitlist_db'].empty and 'ACCOUNT' in dfs['hitlist_db'].columns:
        all_accounts = all_accounts.union(dfs['hitlist_db']['ACCOUNT'].dropna().unique())
    
    selected_accounts = []
    if not all_accounts.empty:
        # Exclure 'Compte Inconnu' des options de sélection si présent
        filtered_accounts_options = [acc for acc in all_accounts.tolist() if acc != 'Compte Inconnu']
        selected_accounts = st.sidebar.multiselect(
            "Sélectionner des Comptes",
            options=sorted(filtered_accounts_options),
            default=[]
        )
        if selected_accounts:
            for key in ['memory', 'usertcode', 'hitlist_db']:
                if not dfs[key].empty and 'ACCOUNT' in dfs[key].columns:
                    dfs[key] = dfs[key][dfs[key]['ACCOUNT'].isin(selected_accounts)]

    # Filtre par REPORT (commun à hitlist_db)
    selected_reports = []
    if not dfs['hitlist_db'].empty and 'REPORT' in dfs['hitlist_db'].columns:
        all_reports = dfs['hitlist_db']['REPORT'].dropna().unique().tolist()
        filtered_reports_options = [rep for rep in all_reports if rep != 'N/A']
        selected_reports = st.sidebar.multiselect(
            "Sélectionner des Rapports (Hitlist DB)",
            options=sorted(filtered_reports_options),
            default=[]
        )
        if selected_reports:
            dfs['hitlist_db'] = dfs['hitlist_db'][dfs['hitlist_db']['REPORT'].isin(selected_reports)]
    
    # Filtre par TASKTYPE (commun à usertcode, times, tasktimes, hitlist_db)
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
        filtered_tasktypes_options = [tt for tt in all_tasktypes.tolist() if tt not in ['Non défini', 'Type de tâche non spécifié', 'N/A']]
        selected_tasktypes = st.sidebar.multiselect(
            "Sélectionner des Types de Tâches",
            options=sorted(filtered_tasktypes_options),
            default=[]
        )
        if selected_tasktypes:
            for key in ['usertcode', 'times', 'tasktimes', 'hitlist_db']:
                if not dfs[key].empty and 'TASKTYPE' in dfs[key].columns:
                    dfs[key] = dfs[key][dfs[key]['TASKTYPE'].isin(selected_tasktypes)]
    
    # Filtre spécifique pour WP_TYP dans le dataset 'performance'
    selected_wp_types = []
    if not dfs['performance'].empty and 'WP_TYP' in dfs['performance'].columns:
        all_wp_types = dfs['performance']['WP_TYP'].dropna().unique().tolist()
        filtered_wp_types_options = [wpt for wpt in all_wp_types if wpt != 'Non défini']
        selected_wp_types = st.sidebar.multiselect(
            "Sélectionner des Types de Processus de Travail (Performance)",
            options=sorted(filtered_wp_types_options),
            default=[]
        )
        if selected_wp_types:
            dfs['performance'] = dfs['performance'][dfs['performance']['WP_TYP'].isin(selected_wp_types)]

    # Filtre de date pour HITLIST_DATABASE (maintenu et amélioré)
    df_hitlist_filtered = dfs['hitlist_db'].copy()
    if not dfs['hitlist_db'].empty and 'FULL_DATETIME' in dfs['hitlist_db'].columns and \
       pd.api.types.is_datetime64_any_dtype(dfs['hitlist_db']['FULL_DATETIME']) and \
       not dfs['hitlist_db']['FULL_DATETIME'].isnull().all(): # Vérifier que FULL_DATETIME n'est pas entièrement NaN
        
        min_date_data = dfs['hitlist_db']['FULL_DATETIME'].min()
        max_date_data = dfs['hitlist_db']['FULL_DATETIME'].max()

        if pd.notna(min_date_data) and pd.notna(max_date_data) and min_date_data.date() <= max_date_data.date():
            default_start_date = min_date_data.date()
            default_end_date = max_date_data.date()

            date_range_hitlist = st.sidebar.date_input(
                "Période pour Insights Hitlist DB",
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
        # --- Onglet 1: Analyse Mémoire (memory_final_cleaned_clean.xlsx) ---
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
                hourly_mem_usage = df_mem.set_index('FULL_DATETIME')['USEDBYTES'].resample('H').mean().reset_index()
                if not hourly_mem_usage.empty:
                    fig_hourly_mem = px.line(hourly_mem_usage, x='FULL_DATETIME', y='USEDBYTES',
                                             title="Tendance Moyenne de l'Utilisation Mémoire (USEDBYTES) par Heure",
                                             labels={'FULL_DATETIME': 'Heure', 'USEDBYTES': 'Moyenne USEDBYTES (Octets)'},
                                             markers=True)
                    st.plotly_chart(fig_hourly_mem, use_container_width=True)
                else:
                    st.info("Pas de données valides pour la tendance horaire de USEDBYTES après filtrage.")
            else:
                st.info("Colonnes 'FULL_DATETIME' ou 'USEDBYTES' manquantes/invalides ou USEDBYTES total est zéro/vide après filtrage pour la tendance.")

        else:
            st.warning("Le dataset 'memory' est vide ou ne contient pas les colonnes requises après filtrage.")

    elif st.session_state.current_section == "Transactions Utilisateurs":
        # --- Onglet 2: Transactions Utilisateurs (USERTCODE_cleaned.xlsx) ---
        st.header("👤 Analyse des Transactions Utilisateurs")
        df_user = dfs['usertcode'].copy()
        if selected_accounts:
            df_user = df_user[df_user['ACCOUNT'].isin(selected_accounts)]
        if selected_tasktypes:
            df_user = df_user[df_user['TASKTYPE'].isin(selected_tasktypes)]

        if not df_user.empty:
            st.subheader("Top 10 Comptes par Nombre Total de Transactions (COUNT)")
            if 'ACCOUNT' in df_user.columns and 'COUNT' in df_user.columns and df_user['COUNT'].sum() > 0:
                top_accounts_transactions = df_user.groupby('ACCOUNT')['COUNT'].sum().nlargest(10)
                fig_top_accounts_transactions = px.bar(top_accounts_transactions.reset_index(),
                                                       x='ACCOUNT', y='COUNT',
                                                       title="Top 10 Comptes par Nombre Total de Transactions",
                                                       labels={'COUNT': 'Nombre Total de Transactions', 'ACCOUNT': 'Compte Utilisateur'},
                                                       color='COUNT', color_continuous_scale=px.colors.sequential.Sunset)
                st.plotly_chart(fig_top_accounts_transactions, use_container_width=True)
            else:
                st.info("Colonnes 'ACCOUNT' ou 'COUNT' manquantes ou le total des transactions est zéro/vide après filtrage.")

            st.subheader("Temps de Réponse Moyen (RESPTI) par Type de Tâche")
            if 'TASKTYPE' in df_user.columns and 'RESPTI' in df_user.columns and df_user['RESPTI'].sum() > 0:
                avg_resp_time_tasktype = df_user.groupby('TASKTYPE')['RESPTI'].mean().sort_values(ascending=False)
                fig_avg_resp_tasktype = px.bar(avg_resp_time_tasktype.reset_index(),
                                               x='TASKTYPE', y='RESPTI',
                                               title="Temps de Réponse Moyen (RESPTI) par Type de Tâche",
                                               labels={'RESPTI': 'Temps de Réponse Moyen (ms)', 'TASKTYPE': 'Type de Tâche'},
                                               color='RESPTI', color_continuous_scale=px.colors.sequential.Viridis)
                st.plotly_chart(fig_avg_resp_tasktype, use_container_width=True)
            else:
                st.info("Colonnes 'TASKTYPE' ou 'RESPTI' manquantes ou le temps de réponse total est zéro/vide après filtrage.")

            st.subheader("Distribution du Temps de Réponse (RESPTI) par Type de Tâche - Box Plot")
            if 'TASKTYPE' in df_user.columns and 'RESPTI' in df_user.columns and not df_user['RESPTI'].empty:
                fig_boxplot_resp_time = px.box(df_user, x='TASKTYPE', y='RESPTI',
                                               title="Distribution du Temps de Réponse (RESPTI) par Type de Tâche",
                                               labels={'RESPTI': 'Temps de Réponse (ms)', 'TASKTYPE': 'Type de Tâche'},
                                               color='TASKTYPE')
                st.plotly_chart(fig_boxplot_resp_time, use_container_width=True)
            else:
                st.info("Colonnes 'TASKTYPE' ou 'RESPTI' manquantes ou vides après filtrage pour le Box Plot.")

            st.subheader("Temps CPU Moyen (CPUTI) par Compte")
            if 'ACCOUNT' in df_user.columns and 'CPUTI' in df_user.columns and df_user['CPUTI'].sum() > 0:
                avg_cpu_time_account = df_user.groupby('ACCOUNT')['CPUTI'].mean().nlargest(10).sort_values(ascending=False)
                fig_avg_cpu_account = px.bar(avg_cpu_time_account.reset_index(),
                                             x='ACCOUNT', y='CPUTI',
                                             title="Temps CPU Moyen (CPUTI) par Compte Utilisateur (Top 10)",
                                             labels={'CPUTI': 'Temps CPU Moyen (ms)', 'ACCOUNT': 'Compte Utilisateur'},
                                             color='CPUTI', color_continuous_scale=px.colors.sequential.Plasma)
                st.plotly_chart(fig_avg_cpu_account, use_container_width=True)
            else:
                st.info("Colonnes 'ACCOUNT' ou 'CPUTI' manquantes ou le temps CPU total est zéro/vide après filtrage.")
            
            st.subheader("Tendance du Temps de Réponse Moyen (RESPTI) par Heure")
            if 'FULL_DATETIME' in df_user.columns and pd.api.types.is_datetime64_any_dtype(df_user['FULL_DATETIME']) and not df_user['FULL_DATETIME'].isnull().all() and df_user['RESPTI'].sum() > 0:
                hourly_resp_time = df_user.set_index('FULL_DATETIME')['RESPTI'].resample('H').mean().reset_index()
                if not hourly_resp_time.empty:
                    fig_hourly_resp = px.line(hourly_resp_time, x='FULL_DATETIME', y='RESPTI',
                                              title="Tendance du Temps de Réponse Moyen (RESPTI) par Heure",
                                              labels={'FULL_DATETIME': 'Heure', 'RESPTI': 'Temps de Réponse Moyen (ms)'},
                                              markers=True)
                    st.plotly_chart(fig_hourly_resp, use_container_width=True)
                else:
                    st.info("Pas de données valides pour la tendance horaire du temps de réponse après filtrage.")
            else:
                st.info("Colonnes 'FULL_DATETIME' ou 'RESPTI' manquantes/invalides ou RESPTI total est zéro/vide après filtrage pour la tendance.")

        else:
            st.warning("Le dataset 'usertcode' est vide ou ne contient pas les colonnes requises après filtrage.")

    elif st.session_state.current_section == "Statistiques Horaires":
        # --- Onglet 3: Statistiques Horaires (Times_final_cleaned_clean.xlsx) ---
        st.header("⏰ Statistiques Horaires des Transactions")
        df_times = dfs['times'].copy()
        if selected_tasktypes:
            df_times = df_times[df_times['TASKTYPE'].isin(selected_tasktypes)]

        if not df_times.empty:
            st.subheader("Nombre Total de Transactions par Heure de la Journée")
            if 'TIME' in df_times.columns and 'COUNT' in df_times.columns and df_times['COUNT'].sum() > 0:
                # Assurez-vous que la colonne 'TIME' est bien formatée pour l'heure
                df_times['HOUR_OF_DAY'] = df_times['TIME'].apply(lambda x: str(x).split(':')[0].zfill(2) if ':' in str(x) else str(x).zfill(2)[:2])
                hourly_counts = df_times.groupby('HOUR_OF_DAY')['COUNT'].sum().sort_index()
                fig_hourly_counts = px.bar(hourly_counts.reset_index(),
                                           x='HOUR_OF_DAY', y='COUNT',
                                           title="Nombre Total de Transactions par Heure de la Journée",
                                           labels={'HOUR_OF_DAY': 'Heure de la Journée', 'COUNT': 'Nombre Total de Transactions'},
                                           color='COUNT', color_continuous_scale=px.colors.sequential.Blues)
                st.plotly_chart(fig_hourly_counts, use_container_width=True)
            else:
                st.info("Colonnes 'TIME' ou 'COUNT' manquantes ou le total des transactions est zéro/vide après filtrage.")

            st.subheader("Temps de Réponse Moyen (RESPTI) par Heure de la Journée")
            if 'TIME' in df_times.columns and 'RESPTI' in df_times.columns and df_times['RESPTI'].sum() > 0:
                df_times['HOUR_OF_DAY'] = df_times['TIME'].apply(lambda x: str(x).split(':')[0].zfill(2) if ':' in str(x) else str(x).zfill(2)[:2])
                hourly_resp_avg = df_times.groupby('HOUR_OF_DAY')['RESPTI'].mean().sort_index()
                fig_hourly_resp_avg = px.line(hourly_resp_avg.reset_index(),
                                              x='HOUR_OF_DAY', y='RESPTI',
                                              title="Temps de Réponse Moyen (RESPTI) par Heure de la Journée",
                                              labels={'HOUR_OF_DAY': 'Heure de la Journée', 'RESPTI': 'Temps de Réponse Moyen (ms)'},
                                              markers=True)
                st.plotly_chart(fig_hourly_resp_avg, use_container_width=True)
            else:
                st.info("Colonnes 'TIME' ou 'RESPTI' manquantes ou le temps de réponse total est zéro/vide après filtrage.")

            st.subheader("Distribution des Temps de Réponse (RESPTI) par Type de Tâche")
            if 'TASKTYPE' in df_times.columns and 'RESPTI' in df_times.columns and not df_times['RESPTI'].empty:
                fig_resp_dist_task = px.histogram(df_times, x='RESPTI', color='TASKTYPE',
                                                  title="Distribution des Temps de Réponse (RESPTI) par Type de Tâche",
                                                  labels={'RESPTI': 'Temps de Réponse (ms)', 'TASKTYPE': 'Type de Tâche'},
                                                  marginal="box", # Ajoute un box plot en marge pour la distribution
                                                  hover_data=df_times.columns)
                st.plotly_chart(fig_resp_dist_task, use_container_width=True)
            else:
                st.info("Colonnes 'TASKTYPE' ou 'RESPTI' manquantes ou vides après filtrage pour la distribution.")
            
        else:
            st.warning("Le dataset 'times' est vide ou ne contient pas les colonnes requises après filtrage.")

    elif st.session_state.current_section == "Décomposition des Tâches":
        # --- Onglet 4: Décomposition des Tâches (TASKTIMES_final_cleaned_clean.xlsx) ---
        st.header("⚙️ Décomposition Détaillée des Tâches")
        df_task = dfs['tasktimes'].copy()
        if selected_tasktypes:
            df_task = df_task[df_task['TASKTYPE'].isin(selected_tasktypes)]

        if not df_task.empty:
            st.subheader("Temps de Traitement Moyen (PROCTI) par Type de Tâche")
            if 'TASKTYPE' in df_task.columns and 'PROCTI' in df_task.columns and df_task['PROCTI'].sum() > 0:
                avg_procti_tasktype = df_task.groupby('TASKTYPE')['PROCTI'].mean().sort_values(ascending=False)
                fig_avg_procti_tasktype = px.bar(avg_procti_tasktype.reset_index(),
                                                 x='TASKTYPE', y='PROCTI',
                                                 title="Temps de Traitement Moyen (PROCTI) par Type de Tâche",
                                                 labels={'PROCTI': 'Temps de Traitement Moyen (ms)', 'TASKTYPE': 'Type de Tâche'},
                                                 color='PROCTI', color_continuous_scale=px.colors.sequential.Oranges)
                st.plotly_chart(fig_avg_procti_tasktype, use_container_width=True)
            else:
                st.info("Colonnes 'TASKTYPE' ou 'PROCTI' manquantes ou le temps de traitement total est zéro/vide après filtrage.")

            st.subheader("Temps CPU Moyen (CPUTI) par Type de Tâche")
            if 'TASKTYPE' in df_task.columns and 'CPUTI' in df_task.columns and df_task['CPUTI'].sum() > 0:
                avg_cputi_tasktype = df_task.groupby('TASKTYPE')['CPUTI'].mean().sort_values(ascending=False)
                fig_avg_cputi_tasktype = px.bar(avg_cputi_tasktype.reset_index(),
                                                x='TASKTYPE', y='CPUTI',
                                                title="Temps CPU Moyen (CPUTI) par Type de Tâche",
                                                labels={'CPUTI': 'Temps CPU Moyen (ms)', 'TASKTYPE': 'Type de Tâche'},
                                                color='CPUTI', color_continuous_scale=px.colors.sequential.Greens)
                st.plotly_chart(fig_avg_cputi_tasktype, use_container_width=True)
            else:
                st.info("Colonnes 'TASKTYPE' ou 'CPUTI' manquantes ou le temps CPU total est zéro/vide après filtrage.")
            
            st.subheader("Temps d'Attente en File d'Attente Moyen (QUEUETI) par Type de Tâche")
            if 'TASKTYPE' in df_task.columns and 'QUEUETI' in df_task.columns and df_task['QUEUETI'].sum() > 0:
                avg_queueti_tasktype = df_task.groupby('TASKTYPE')['QUEUETI'].mean().sort_values(ascending=False)
                fig_avg_queueti_tasktype = px.bar(avg_queueti_tasktype.reset_index(),
                                                  x='TASKTYPE', y='QUEUETI',
                                                  title="Temps d'Attente en File d'Attente Moyen (QUEUETI) par Type de Tâche",
                                                  labels={'QUEUETI': "Temps d'Attente Moyen (ms)", 'TASKTYPE': 'Type de Tâche'},
                                                  color='QUEUETI', color_continuous_scale=px.colors.sequential.Purples)
                st.plotly_chart(fig_avg_queueti_tasktype, use_container_width=True)
            else:
                st.info("Colonnes 'TASKTYPE' ou 'QUEUETI' manquantes ou le temps d'attente total est zéro/vide après filtrage.")

        else:
            st.warning("Le dataset 'tasktimes' est vide ou ne contient pas les colonnes requises après filtrage.")

    elif st.session_state.current_section == "Insights Hitlist DB":
        # --- Onglet 5: Insights Hitlist DB (HITLIST_DATABASE_final_cleaned_clean.xlsx) ---
        st.header("🔍 Insights sur la Base de Données (Hitlist DB)")
        # Utiliser df_hitlist_filtered qui a déjà appliqué le filtre de date
        df_hitlist = df_hitlist_filtered.copy()
        if selected_accounts:
            df_hitlist = df_hitlist[df_hitlist['ACCOUNT'].isin(selected_accounts)]
        if selected_reports:
            df_hitlist = df_hitlist[df_hitlist['REPORT'].isin(selected_reports)]
        if selected_tasktypes:
            df_hitlist = df_hitlist[df_hitlist['TASKTYPE'].isin(selected_tasktypes)]

        if not df_hitlist.empty:
            if 'FULL_DATETIME' in df_hitlist.columns and pd.notna(df_hitlist['FULL_DATETIME'].min()) and pd.notna(df_hitlist['FULL_DATETIME'].max()):
                st.info(f"Données affichées pour la période: "
                        f"{df_hitlist['FULL_DATETIME'].min().strftime('%Y-%m-%d')} à "
                        f"{df_hitlist['FULL_DATETIME'].max().strftime('%Y-%m-%d')}")
            else:
                st.info("La plage de dates pour HITLIST_DATABASE n'a pas pu être déterminée.")

            st.subheader("Temps de Réponse (RESPTI) par Rapport (Top 10)")
            if 'REPORT' in df_hitlist.columns and 'RESPTI' in df_hitlist.columns and df_hitlist['RESPTI'].sum() > 0:
                top_reports_resp = df_hitlist.groupby('REPORT')['RESPTI'].sum().nlargest(10)
                fig_top_reports_resp = px.bar(top_reports_resp.reset_index(),
                                              x='REPORT', y='RESPTI',
                                              title="Temps de Réponse Total (RESPTI) par Rapport (Top 10)",
                                              labels={'RESPTI': 'Temps de Réponse Total (ms)', 'REPORT': 'Nom du Rapport'},
                                              color='RESPTI', color_continuous_scale=px.colors.sequential.Aggrnyl)
                st.plotly_chart(fig_top_reports_resp, use_container_width=True)
            else:
                st.info("Colonnes 'REPORT' ou 'RESPTI' manquantes ou le temps de réponse total est zéro/vide après filtrage.")

            st.subheader("Appels Base de Données (DBCALLS) par Rapport (Top 10)")
            if 'REPORT' in df_hitlist.columns and 'DBCALLS' in df_hitlist.columns and df_hitlist['DBCALLS'].sum() > 0:
                top_reports_dbcalls = df_hitlist.groupby('REPORT')['DBCALLS'].sum().nlargest(10)
                fig_top_reports_dbcalls = px.bar(top_reports_dbcalls.reset_index(),
                                                 x='REPORT', y='DBCALLS',
                                                 title="Nombre Total d'Appels Base de Données (DBCALLS) par Rapport (Top 10)",
                                                 labels={'DBCALLS': "Nombre d'Appels DB", 'REPORT': 'Nom du Rapport'},
                                                 color='DBCALLS', color_continuous_scale=px.colors.sequential.Tealrose)
                st.plotly_chart(fig_top_reports_dbcalls, use_container_width=True)
            else:
                st.info("Colonnes 'REPORT' ou 'DBCALLS' manquantes ou le total des appels DB est zéro/vide après filtrage.")

            st.subheader("Distribution du Temps de Réponse (RESPTI) Global")
            if 'RESPTI' in df_hitlist.columns and df_hitlist['RESPTI'].sum() > 0:
                if df_hitlist['RESPTI'].nunique() > 1:
                    fig_dist_respti = ff.create_distplot([df_hitlist['RESPTI'].dropna()], ['RESPTI'],
                                                         bin_size=df_hitlist['RESPTI'].std()/5,
                                                         show_rug=False, show_hist=False)
                    fig_dist_respti.update_layout(title_text="Distribution du Temps de Réponse (RESPTI) Global",
                                                  xaxis_title='Temps de Réponse (ms)',
                                                  yaxis_title='Densité')
                    fig_dist_respti.data[0].line.color = 'darkblue'
                    st.plotly_chart(fig_dist_respti, use_container_width=True)
                else:
                    st.info("La colonne 'RESPTI' contient des valeurs uniques ou est vide après filtrage, impossible de créer une courbe de densité.")
            else:
                st.info("Colonne 'RESPTI' manquante ou total est zéro/vide après filtrage.")
        else:
            st.warning("Le dataset 'hitlist_db' est vide ou ne contient pas les colonnes requises après filtrage.")

    elif st.session_state.current_section == "Performance des Processus de Travail":
        # --- Onglet 6: Performance des Processus de Travail (AL_GET_PERFORMANCE_final_cleaned_clean.xlsx) ---
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
        # --- Onglet 7: Résumé des Traces de Performance SQL (performance_trace_summary_final_cleaned_clean.xlsx) ---
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
                # Maintenu comme graphique à barres car il n'y a pas de dimension temporelle pour une courbe significative
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
            if 'GLTGB_DATE' in df_usr02.columns and pd.api.types.is_datetime64_any_dtype(df_usr02['GLTGB_DATE']) and not df_usr02['GLTGB_DATE'].isnull().all():
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



