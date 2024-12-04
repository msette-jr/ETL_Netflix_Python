import pandas as pd
import requests


class NetflixETL:
    def __init__(self, url, output_file='netflix.csv'):
        """
        Inicializa a classe com a URL do arquivo fonte e o caminho do arquivo de saída.
        """
        self.url = url
        self.temp_file = 'temp_file.csv'
        self.output_file = output_file
        self.df = None

    def extract(self):
        """
        Extrai os dados da URL fornecida e salva em um arquivo temporário.
        """
        try:
            response = requests.get(self.url)
            with open(self.temp_file, 'wb') as file:
                file.write(response.content)
            self.df = pd.read_csv(self.temp_file)
            print("Extração concluída com sucesso.")
        except Exception as e:
            print(f"Erro ao extrair dados: {e}")

    def transform(self):
        """
        Realiza as transformações nos dados.
        """
        try:
            # Cria cópia do DataFrame
            df1 = self.df.copy()

            # Manipulação das datas
            df1['date_added'] = pd.to_datetime(df1['date_added'], format='mixed', errors='coerce')

            # Substituição de valores faltantes
            df1 = df1.fillna("nao_informado")

            # Seleciona e troca os valores das colunas 'rating' e 'duration' quando duration é 'nao_informado'
            mask = df1['duration'] == 'nao_informado'
            temp = df1.loc[mask, 'rating'].copy()
            df1.loc[mask, 'rating'] = df1.loc[mask, 'duration']
            df1.loc[mask, 'duration'] = temp

            # Função para tratar o atributo duration
            def substituir_valores(duration):
                if "min" in duration:
                    return int(duration.split(' ')[0])
                if "Season" in duration:
                    return int(duration.split(' ')[0])
                else:
                    return "nao_informado"

            # Aplicando a função
            df1['duracao'] = df1['duration'].apply(substituir_valores)

            # Ajustando os valores de 'rating'
            df1['rating'] = df1['rating'].replace(
                ['TV-Y', 'TV-Y7', 'G', 'TV-G', 'PG', 'TV-PG', 'TV-Y7-FV'], 'Kids'
            )
            df1['rating'] = df1['rating'].replace(['PG-13', 'TV-14'], 'Teens')
            df1['rating'] = df1['rating'].replace(['R', 'TV-MA', 'NC-17'], 'Adults')
            df1['rating'] = df1['rating'].replace(['NR', 'UR'], 'nao_informado')

            self.df = df1
            print("Transformação concluída com sucesso.")
        except Exception as e:
            print(f"Erro ao transformar dados: {e}")

    def load(self):
        """
        Carrega os dados transformados para um arquivo CSV.
        """
        try:
            self.df.to_csv(self.output_file, sep=';', decimal=',', index=False)
            print(f"Dados carregados com sucesso em {self.output_file}.")
        except Exception as e:
            print(f"Erro ao carregar dados: {e}")

    def run(self):
        """
        Executa o processo ETL completo.
        """
        self.extract()
        self.transform()
        self.load()


# Uso da classe
url = 'https://drive.google.com/uc?id=1bJpHEb_axSoTiX0lSZb5yQr5EDTtzd5o&export=download'
etl = NetflixETL(url)
etl.run()

