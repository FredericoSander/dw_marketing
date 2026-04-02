import os
from dotenv import load_dotenv

load_dotenv()

# Kaggle
DATASET_NAME = os.getenv("DATASET_NAME", "geethasagarbonthu/marketing-and-e-commerce-analytics-dataset")
DATA_PATH = os.getenv("DATA_PATH", "data/")

# PostgreSQL
DB_HOST = os.getenv("DB_HOST_PROD", "localhost")
DB_PORT = os.getenv("DB_PORT_PROD", "5433")
DB_NAME = os.getenv("DB_NAME_PROD")
DB_USER = os.getenv("DB_USER_PROD")
DB_PASS = os.getenv("DB_PASS_PROD")

# Monta a URL de conexao no formato que o SQLAlchemy entende
DB_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
