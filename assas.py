import requests
import pandas as pd

class AssasAPI:
    def __init__(self):
        self.api_token = '$aact_prod_000MzkwODA2MWY2OGM3MWRlMDU2NWM3MzJlNzZmNGZhZGY6Ojk2ZWRhYmMyLWViMmUtNDljOS1iNTdlLTc2Zjk3NDEzZWVhZTo6JGFhY2hfN2MwZTdlZjEtZWRhYy00ZjhhLThkMjItOWU4MDg0YmQ2OWMy'
        self.headers = {
            'access_token': self.api_token,
            'Content-Type': 'application/json'
        }
        self.url = "https://www.asaas.com/api/v3/financialTransactions"
        self.all_transactions = []
        self.offset = 0
        self.limit = 100
        self.has_more = True

    def get_transactions(self,data_inicio, data_fim):

        while self.has_more:
            params = {
                'startDate': data_inicio,
                'finishDate': data_fim,
                'offset': self.offset,
                'limit': self.limit
            }

            response = requests.get(self.url, headers=self.headers, params=params)

            if response.status_code == 200:
                data = response.json().get('data', [])
                self.all_transactions.extend(data)
                self.offset += self.limit

                if len(data) < self.limit:
                    self.has_more = False
            else:
                print("Erro ao recuperar dados:", response.status_code)
                break

        return pd.DataFrame(self.all_transactions, columns=['type','value','date','description'])
    
class TransformacoesAPI:
    
    def classificar_transacao(self, type):
        if type in ['BACEN_JUDICIAL_LOCK', 'BACEN_JUDICIAL_TRANSFER', 'BACEN_JUDICIAL_UNLOCK']:
            return 'Bloqueio Judicial'
        elif type == 'BILL_PAYMENT':
            return 'Pagamento'
        elif type in ['PAYMENT_FEE', 'PAYMENT_MESSAGING_NOTIFICATION_FEE', 'INSTANT_TEXT_MESSAGE_FEE', 'TRANSFER_FEE']:
            return 'Taxas bancária'
        elif type == 'PAYMENT_RECEIVED':
            return 'Receita unidade'    
        elif type == 'PIX_TRANSACTION_DEBIT_REFUND':
            return 'Estorno de Transferencia'    
        elif type == 'TRANSFER':
            return 'Transferência'     
        else:
            return '-'
    
    def classificar_deb_cre(self, type):
        if type in ['BACEN_JUDICIAL_LOCK', 'BACEN_JUDICIAL_TRANSFER', 'BACEN_JUDICIAL_UNLOCK']:
            return 'Bloqueio Judicial'
        elif type == 'BILL_PAYMENT':
            return 'Débito'
        elif type in ['PAYMENT_FEE', 'PAYMENT_MESSAGING_NOTIFICATION_FEE', 'INSTANT_TEXT_MESSAGE_FEE', 'TRANSFER_FEE']:
            return 'Débito'
        elif type == 'PAYMENT_RECEIVED':
            return 'Crédito'    
        elif type == 'PIX_TRANSACTION_DEBIT_REFUND':
            return 'Crédito'    
        elif type == 'TRANSFER':
            return 'Débito'     
        else:
            return '-' 
        
    def mes_ano(self, data):
        meses = {
            1: "Jan", 2: "Fev", 3: "Mar", 4: "Abr", 5: "Mai", 6: "Jun",
            7: "Jul", 8: "Ago", 9: "Set", 10: "Out", 11: "Nov", 12: "Dez"
        }
        return f"{meses[data.month]}/{data.year}"

    def transformar_dataframe(self, df):
        # Aplicando a função ao DataFrame
        df['Categoria'] = df['type'].apply(self.classificar_transacao)
        df['TipoOperacao'] = df['type'].apply(self.classificar_deb_cre)
        # Criando a coluna Mês/Ano
        df['Mês/Ano'] = pd.to_datetime(df['date']).apply(self.mes_ano)
        # Reordenando as colunas
        df_assas = df[['TipoOperacao', 'Categoria', 'value', 'date', 'Mês/Ano', 'description']].rename(columns={'value': 'Valor', 'date': 'Data', 'description': 'Descrição'})
        return df_assas
    
class AssasDataProcessor:
    def __init__(self,data_inicio, data_fim):
        self.api = AssasAPI()
        self.transf = TransformacoesAPI()
        self.df = self.api.get_transactions(data_inicio, data_fim)
        self.df_assas = self.transf.transformar_dataframe(self.df)
        
    def processar_dados(self):
        return self.df_assas

if __name__ == "__main__":
    pass

    # Exemplo de uso
    #data_inicio = '2021-01-01'
    #data_fim = '2021-12-31'
    #assas = AssasDataProcessor(data_inicio, data_fim)
    
    
    