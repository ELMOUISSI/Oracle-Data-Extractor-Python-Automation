import streamlit as st
import os
import pandas as pd
import time
import datetime
from sqlalchemy import create_engine
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed
import warnings

warnings.filterwarnings("ignore", category=UserWarning, module="pandas")

# --- Charger les variables d'environnement
load_dotenv()
username = os.getenv("ORACLE_USER")
password = os.getenv("ORACLE_PASSWORD")
host = os.getenv("ORACLE_HOST", "192.168.0.166")
port = os.getenv("ORACLE_PORT", "1521")
service_name = os.getenv("ORACLE_SERVICE", "TPXPROD")
MAX_THREADS = int(os.getenv("MAX_THREADS", 3))

SQL_FOLDER = "sql"
OUTPUT_FOLDER = "roquete"
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

# --- Config Oracle DSN
dsn = f"(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST={host})(PORT={port}))(CONNECT_DATA=(SERVICE_NAME={service_name})))"
engine = create_engine(f"oracle+oracledb://{username}:{password}@{dsn}")

# --- Fonction d’exécution d’une requête SQL
def execute_sql_file(file_path, progress_container):
    sql_name = os.path.splitext(os.path.basename(file_path))[0]
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = os.path.join(OUTPUT_FOLDER, f"{sql_name}_{timestamp}.xlsx")

    try:
        with open(file_path, "r", encoding="utf-8") as f:
            sql_query = f.read()
    except UnicodeDecodeError:
        with open(file_path, "r", encoding="latin-1") as f:
            sql_query = f.read()

    progress_container.info(f"🚀 Exécution de `{sql_name}`...")
    start_time = time.time()

    try:
        with engine.connect() as conn:
            df = pd.read_sql(sql_query, conn)
            df.to_excel(output_file, index=False)
        duration = (time.time() - start_time) / 60
        progress_container.success(f"✅ `{sql_name}` terminé ({len(df):,} lignes) en {duration:.2f} min")
        return sql_name, output_file, len(df), duration

    except Exception as e:
        progress_container.error(f"❌ Erreur dans `{sql_name}` : {e}")
        return sql_name, None, 0, 0


# --- UI Streamlit
st.set_page_config(page_title="Oracle Data Extractor", page_icon="💾", layout="wide")

st.markdown("""
<h1 style='text-align:center; color:#4CAF50;'>💾 Oracle Data Extractor</h1>
<p style='text-align:center; color:gray;'>Interface web pour exécuter et exporter des requêtes SQL Oracle</p>
""", unsafe_allow_html=True)

st.divider()

# --- Charger les fichiers SQL
sql_files = [f for f in os.listdir(SQL_FOLDER) if f.endswith(".sql") or f.endswith(".txt")]

if not sql_files:
    st.warning("⚠️ Aucun fichier SQL trouvé dans le dossier `sql/`.")
else:
    with st.sidebar:
        st.header("⚙️ Paramètres")
        selected_files = st.multiselect("📂 Sélectionnez vos requêtes :", sql_files)
        max_threads = st.slider("🧵 Threads en parallèle :", 1, 10, MAX_THREADS)
        st.markdown("---")
        start_btn = st.button("▶️ Lancer les extractions", use_container_width=True)

    if selected_files and start_btn:
        st.info("Démarrage des extractions...")
        progress_area = st.container()
        progress_bar = st.progress(0)
        results = []

        with ThreadPoolExecutor(max_workers=max_threads) as executor:
            futures = []
            for i, f in enumerate(selected_files):
                placeholder = progress_area.empty()
                future = executor.submit(execute_sql_file, os.path.join(SQL_FOLDER, f), placeholder)
                futures.append(future)

            for i, future in enumerate(as_completed(futures)):
                results.append(future.result())
                progress_bar.progress((i + 1) / len(selected_files))

        st.success("✅ Toutes les extractions sont terminées !")

        df_summary = pd.DataFrame(results, columns=["Requête", "Fichier", "Lignes", "Durée (min)"])
        st.dataframe(df_summary, use_container_width=True)

        # Télécharger le rapport
        csv_path = os.path.join(OUTPUT_FOLDER, f"summary_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")
        df_summary.to_csv(csv_path, index=False)

        with open(csv_path, "rb") as f:
            st.download_button(
                label="⬇️ Télécharger le rapport CSV",
                data=f,
                file_name=os.path.basename(csv_path),
                mime="text/csv"
            )
    else:
            st.info("💡 Ajoutez des fichiers `.sql` dans le dossier `sql/` pour commencer.")
