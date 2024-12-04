from src.etl import NetflixETL

def main():
    # URL do arquivo CSV a ser processado
    url = 'https://drive.google.com/uc?id=1bJpHEb_axSoTiX0lSZb5yQr5EDTtzd5o&export=download'

    # Inicializa a classe com a URL do arquivo
    etl = NetflixETL(url)

    # Executa o processo ETL
    print("Fazendo download dos dados...")
    etl.download_data()

    print("Processando e limpando os dados...")
    etl.clean_data()

    print("Salvando os dados processados no arquivo 'netflix_tratado.csv'...")
    etl.save_data('netflix_tratado.csv')

    print("Processo ETL concluído com sucesso!")

if __name__ == "__main__":
    main()
