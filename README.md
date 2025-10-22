# Oracle Data Extractor — Python Automation

Résumé
-------
Oracle Data Extractor est un ensemble d'outils Python pour automatiser l'extraction de données depuis des bases Oracle et exporter les résultats (CSV, Excel, JSON). Le projet vise à fournir une solution simple et configurable pour les extractions planifiées ou ponctuelles.

Fonctionnalités
---------------
- Connexion à Oracle (cx_Oracle / oracledb)
- Exécution de requêtes paramétrées et scripts SQL
- Export en CSV / Excel (.xlsx) / JSON
- Configuration via fichier `.ini` et variables d'environnement
- Logging détaillé et gestion d'erreurs
- Mode CLI et utilisation comme bibliothèque Python

Prérequis
--------
- Python 3.8+
- Client Oracle (si tu utilises `cx_Oracle` classique) ou `oracledb` en mode Thin
- Bibliothèques Python listées dans `requirements.txt`

Installation rapide
-------------------
1. Cloner le dépôt
   ```
   git clone https://github.com/ELMOUISSI/Oracle-Data-Extractor-Python-Automation.git
   cd Oracle-Data-Extractor-Python-Automation
   ```

2. Créer un environnement virtuel et installer les dépendances
   ```
   python -m venv .venv
   source .venv/bin/activate   # ou .venv\Scripts\activate sur Windows
   pip install -r requirements.txt
   ```

3. Copier l'exemple de configuration et remplir les credentials
   - Renommer `.env.example` en `.env` ou utiliser `config.example.ini` comme base.

Utilisation (exemples)
----------------------
- Exécution depuis la CLI (exemple générique)
  ```
  python -m odata_extractor.cli --config config.ini --query-id daily_sales --output out/daily_sales.csv
  ```

- Utilisation comme module Python
  ```python
  from odata_extractor import Extractor

  cfg = "config.ini"
  ext = Extractor(cfg)
  ext.run(query_id="daily_sales", output_path="out/daily_sales.xlsx")
  ```

Configuration
-------------
Deux méthodes :
- Variables d'environnement (voir `.env.example`)
- Fichier INI (voir `config.example.ini`)

Voir `docs/CONFIGURATION.md` pour la doc complète sur la configuration des connexions Oracle, formats de sortie et exemples de requêtes.

Meilleures pratiques et sécurité
-------------------------------
- Ne place jamais les identifiants en clair dans le dépôt. Utilise `.env` local ou un store de secrets.
- Teste les requêtes sur un dataset restreint avant de lancer en production.
- Active la journalisation (LOG_LEVEL) pour faciliter le debug.

Contribution
------------
Voir `CONTRIBUTING.md` pour les règles de contribution, style de code et procédure de pull request.

Licence
-------
MIT — voir le fichier `LICENSE` pour les détails.

Contact
-------
Pour des questions, ouvre une issue sur le dépôt ou contacte @ELMOUISSI.
