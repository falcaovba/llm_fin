import pandas as pd
import requests

class InterAPI:
    def __init__(self): 
        self.token = None
   
    def get_token(self):
        # Dados para autenticação OAuth2
        request_body = (
            "client_id=ccf0245c-0053-4529-bb61-e5995de4a2d7"
            "&client_secret=2db04453-e078-4fcd-8b83-3364b140946b"
            "&scope=extrato.read&grant_type=client_credentials"
        )
        # Solicitando o token
        response = requests.post(
            "https://cdpj.partners.bancointer.com.br/oauth/v2/token",
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            cert=("Inter API_Certificado.crt", "Inter API_Chave.key"),
            data=request_body,
        )
        
        response.raise_for_status()
        self.token = response.json().get("access_token")


    def get_extrato(self, data_inicio, data_fim):
        # Definindo parâmetros fixos e variáveis para paginação
        pagina = 0
        tamanho_pagina = 100
        todas_transacoes = []

        while True:
            opFiltros = {
                "dataInicio": data_inicio,
                "dataFim": data_fim,
                "pagina": pagina,
                "tamanhoPagina": tamanho_pagina,
            }

            cabecalhos = {
                "Authorization": f"Bearer {self.token}",
                "x-conta-corrente": "122282531",
                "Content-Type": "Application/json",
            }

            response = requests.get(
                "https://cdpj.partners.bancointer.com.br/banking/v2/extrato/completo",
                params=opFiltros,
                headers=cabecalhos,
                cert=("Inter API_Certificado.crt", "Inter API_Chave.key"),
            )
            response.raise_for_status()
            dados_extrato = response.json()

            transacoes = dados_extrato.get("transacoes", [])
            todas_transacoes.extend(transacoes)

            # Parar se não houver mais dados a serem retornados
            if not transacoes or len(transacoes) < tamanho_pagina:
                break

            pagina += 1

        # Convertendo o resultado completo em DataFrame
        self.df = pd.DataFrame(
            todas_transacoes, columns=["dataTransacao", "tipoOperacao", "descricao", "valor", "detalhes"]
        )

        
class TransformacaoesAPI:
    def __init__(self, df):
        self.df = df

    def extrair_descricao(self, detalhes):
        if isinstance(detalhes, dict):
            return detalhes.get('detalheDescricao') or detalhes.get('descricaoPix') or None
        return None

    def classificar_deb_cre(self, tipoOperacao):
        if tipoOperacao == 'D':
            return 'Débito'
        elif tipoOperacao == 'C':
            return 'Crédito'
        else:
            return '-'

    def mes_ano(self, data):
        meses = {
            1: "Jan", 2: "Fev", 3: "Mar", 4: "Abr", 5: "Mai", 6: "Jun",
            7: "Jul", 8: "Ago", 9: "Set", 10: "Out", 11: "Nov", 12: "Dez"
        }
        return f"{meses[data.month]}/{data.year}"

    def processar_dados(self):
        # Aplicar no DataFrame
        self.df['detalheDescricao'] = self.df['detalhes'].apply(self.extrair_descricao).str.replace(r'\s+', ' ', regex=True).str.strip()
        self.df['descricao'] = self.df['descricao'].str.replace(r'\s+', ' ', regex=True).str.strip()

        # Aplicar no DataFrame
        self.df['TipoOperacao'] = self.df['tipoOperacao'].apply(self.classificar_deb_cre)
        self.df = self.df.drop(columns=['detalhes', 'tipoOperacao'])

        self.df['Mês/Ano'] = pd.to_datetime(self.df['dataTransacao']).apply(self.mes_ano)
        
        self.df = self.df[['TipoOperacao', 'valor', 'dataTransacao', 'Mês/Ano', 'descricao', 'detalheDescricao']]\
            .rename(columns={'valor': 'Valor', 'dataTransacao': 'Data', 'detalheDescricao': 'Descrição', 'descricao': 'Fornecedor'})
 
                    
class InterDataProcessor:
    def __init__(self, data_inicio, data_fim):
        self.data_inicio = data_inicio
        self.data_fim = data_fim
        self.api = InterAPI()
        self.df_inter = None

    def processar_dados(self, data_inicio, data_fim):
        self.api.get_token()
        self.api.get_extrato(data_inicio, data_fim)
        transformacoes = TransformacaoesAPI(self.api.df)
        transformacoes.processar_dados()
        self.df_inter = transformacoes.df
        return self.df_inter
    
if __name__ == "__main__":
    pass

    # Exemplo de uso
    # data_inicio = "2021-01-01"
    # data_fim = "2021-01-31"
    # inter = InterDataProcessor(data_inicio, data_fim)
    # df = inter.processar_dados(data_inicio, data_fim)
    # print(df)
    # df.to_csv('inter.csv', index=False)