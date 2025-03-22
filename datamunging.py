from inter import InterDataProcessor
from assas import AssasDataProcessor
import pandas as pd

# Assas Data Munging
class AssasDataMunging:
    def __init__(self, data_inicio, data_fim):
        self.processor_assas = AssasDataProcessor(data_inicio, data_fim)
        self.data_inicio = data_inicio
        self.data_fim = data_fim
        self.df_assas = None

    def processar_dados(self):
        # Processar os dados usando o processor_assas
        self.df_assas = self.processor_assas.processar_dados()
        self.df_assas['Instituição'] = 'Assas'

    @staticmethod
    def split_after_number(text):
        # Função para dividir a string após "nr."
        parts = text.split("nr.")
        if len(parts) > 1:
            return parts[1].strip()
        else:
            return "BANCO INTER"

    def conditional_split(self, row):
        # Função para aplicar condições específicas
        if row['Categoria'] == 'Bloqueio Judicial':
            return 'Justiça'
        elif row['Categoria'] == 'Estorno de Transferencia':
            return '456623262 CONDOMINIO AMBASSADOR'
        elif row['Categoria'] == 'Transferência' and row['Descrição'] in [
            'Transação via Pix com dados manuais para CONDOMINIO EDIFICIO AMBSSADOR',
            'Transação via Pix com dados manuais para condominio edificio ambassador',
            'Transferência para conta bancária']:
            return '456623262 CONDOMINIO AMBASSADOR'
        elif row['Categoria'] in ['Receita unidade', 'Taxas bancária']:
            return self.split_after_number(row['Descrição']) if isinstance(row['Descrição'], str) else 'BANCO INTER'
        else:
            return 'BANCO INTER'

    def aplicar_condicoes(self):
        # Aplicar a função conditional_split
        self.df_assas['Fornecedor'] = self.df_assas.apply(lambda row: self.conditional_split(row), axis=1)

    def selecionar_colunas(self):
        # Selecionar as colunas desejadas
        self.df_assas = self.df_assas[['Data', 'Mês/Ano', 'TipoOperacao', 'Categoria', 'Fornecedor', 'Valor', 'Descrição', 'Instituição']]

    def executar(self):
        # Executar todas as etapas
        self.processar_dados()
        self.aplicar_condicoes()
        self.selecionar_colunas()
        return self.df_assas
 
# -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- 
    
# Inter Data Munging
class InterDataMunging:
    def __init__(self, data_inicio, data_fim):
        self.processor_inter = InterDataProcessor(data_inicio, data_fim)
        self.data_inicio = data_inicio
        self.data_fim = data_fim
        self.df_inter = None

    def processar_dados(self):
        # Processar os dados usando o processor_inter
        self.df_inter = self.processor_inter.processar_dados(self.data_inicio, self.data_fim)
        self.df_inter['Instituição'] = 'Inter'

    def formatar_fornecedor(self):
        # Converter os valores da coluna 'Fornecedor' para letras maiúsculas
        self.df_inter['Fornecedor'] = self.df_inter['Fornecedor'].str.upper()

    def selecionar_colunas(self):
        # Selecionar as colunas desejadas
        self.df_inter['Categoria'] = None
        self.df_inter = self.df_inter[['Data', 'Mês/Ano', 'TipoOperacao', 'Categoria', 'Fornecedor', 'Valor', 'Descrição', 'Instituição']]

    def executar(self):
        # Executar todas as etapas
        self.processar_dados()
        self.formatar_fornecedor()
        self.selecionar_colunas()
        return self.df_inter
    
    
if __name__ == "__main__":
    pass