import logging
import pandas as pd
from sqlalchemy import create_engine
from config import DB_URL
from config import DATASET_NAME, DATA_PATH

TABELAS= {
    "customers": "customers",
    "campaigns": "campaigns",
    "transactions": "transactions",
    "products": "products",
    "events": "events",
}

def conectar_db():
    """
    Conecta ao banco de dados PostgreSQL usando SQLAlchemy.
    Retorna a engine de conexão.
    """
    try:
        engine = create_engine(DB_URL)
        logging.info("Conexão com o banco de dados estabelecida com sucesso.")
        return engine
    except Exception as e:
        logging.error(f"Erro ao conectar ao banco de dados: {e}")
        raise

def carregar_dados(nome_arquivo: str ) -> pd.DataFrame:
    """
    Carrega os dados do arquivo CSV para um DataFrame do Pandas.
    """
    try:
        caminho = f'{DATA_PATH}/{nome_arquivo}.csv'
        logging.info(f"Carregando os dados do arquivo '{caminho}'...")
        return pd.read_csv(caminho)
              
    except Exception as e:
        logging.error(f"Erro ao carregar os dados do arquivo '{nome_arquivo}': {e}")
        raise

def salvar_postgres(df: pd.DataFrame, nome_tabela: str, engine):
    """
    Salva o DataFrame no banco de dados PostgreSQL.
    """
    try:
        logging.info(f"Salvando os dados na tabela '{nome_tabela}' do banco de dados...")
        df.to_sql(
                name=nome_tabela,
                con=engine,
                if_exists='replace',
                index=False
        )
        logging.info(f"Dados salvos com sucesso na tabela '{nome_tabela}'.")
    except Exception as e:
        logging.error(f"Erro ao salvar os dados na tabela '{nome_tabela}': {e}")
        raise

def executar():
    """
    Função principal para executar o processo de carregamento dos dados para o PostgreSQL.
    """
    try:
        engine = conectar_db()
        for nome_arquivo, nome_tabela in TABELAS.items():
            df = carregar_dados(nome_arquivo)
            salvar_postgres(df, nome_tabela, engine)
        logging.info("Processo de carregamento concluído com sucesso.")
    except Exception as e:
        logging.error(f"Erro ao executar o processo de carregamento: {e}")
        raise

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    executar()