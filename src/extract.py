import logging
import kaggle
from config import DATASET_NAME, DATA_PATH

def autenticar():
    """
    Autentica o usuário no Kaggle usando a API do Kaggle.
    Certifique-se de ter o arquivo kaggle.json com suas credenciais na pasta correta.
    """
    try:
        kaggle.api.authenticate()
        logging.info("Autenticação bem-sucedida no Kaggle.")
    except Exception as e:
        logging.error(f"Erro ao autenticar no Kaggle: {e}")
        raise

def baixar_dataset(dataset:str, path:str):
    """
    Baixa o dataset do Kaggle usando a API do Kaggle.
    O dataset será baixado e descompactado na pasta especificada em DATA_PATH.
    """
    try:
        logging.info(f"Baixando o dataset '{DATASET_NAME}' para a pasta '{DATA_PATH}'...")
        kaggle.api.dataset_download_files(dataset, path=path, unzip=True)
        logging.info("Download e descompactação concluídos com sucesso.")
    except Exception as e:
        logging.error(f"Erro ao baixar o dataset: {e}")
        raise
    
def executar():
    """
    Função principal para executar o processo de autenticação e download do dataset.
    """
    try:
        autenticar()
        baixar_dataset(DATASET_NAME, DATA_PATH)
        logging.info("Processo de extração concluído com sucesso.")
    except Exception as e:
        logging.error(f"Erro ao executar o processo: {e}")
        raise

def salvar_postgres():
    
    pass



if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    executar()

# kaggle.api.authenticate()
# kaggle.api.dataset_download_files('geethasagarbonthu/marketing-and-e-commerce-analytics-dataset', path='data/', unzip=True)


