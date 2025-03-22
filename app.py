
import streamlit as st
import pandas as pd
import faiss
import numpy as np
from sentence_transformers import SentenceTransformer
from transformers import pipeline
import datetime
from datamunging import InterDataMunging, AssasDataMunging

@st.cache_resource
def load_model():
    return pipeline("text-generation", model="EleutherAI/gpt-neo-1.3B")

@st.cache_resource
def load_embedding_model():
    return SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")

llm = load_model()
embedder = load_embedding_model()

def criar_faiss_index(df):
    df['Valor'] = pd.to_numeric(df['Valor'], errors='coerce').fillna(0.0)
    textos = df.apply(lambda row: f"{row['Data']}: {row['TipoOperacao']} - {row.get('Descrição', '')} - {row.get('Fornecedor', '')} - R$ {row['Valor']}", axis=1).tolist()
    embeddings = embedder.encode(textos)
    index = faiss.IndexFlatL2(embeddings.shape[1])
    index.add(np.array(embeddings, dtype=np.float32))
    return index, textos

def consulta_rag(pergunta, index, textos):
    embedding_pergunta = embedder.encode([pergunta])
    D, I = index.search(np.array(embedding_pergunta, dtype=np.float32), k=10)
    contexto = " ".join([textos[i] for i in I[0]])

    prompt_analitico_executivo = (
        f"Com base nas seguintes informações financeiras extraídas do banco de dados: {contexto}. "
        "Elabore um resumo financeiro claro, objetivo, com análise de valores, somas e conclusões importantes. "
        "Se possível, apresente totais numéricos e observações relevantes."
    )

    resposta = llm(prompt_analitico_executivo, max_length=1000, temperature=0.3)[0]['generated_text']
    return resposta

st.title("Análise Financeira com LLM + RAG")

data_inicio = st.date_input("Data início", datetime.date.today().replace(day=1))
data_fim = st.date_input("Data fim", datetime.date.today())

if st.button("Atualizar dados"):
    with st.spinner("Carregando dados das APIs..."):
        inter_data_munging = InterDataMunging(data_inicio, data_fim)
        df_inter = inter_data_munging.executar()

        assas_data_munging = AssasDataMunging(data_inicio, data_fim)
        df_assas = assas_data_munging.executar()

        df_consolidado = pd.concat([df_inter, df_assas], ignore_index=True)

        st.session_state['df'] = df_consolidado
        st.session_state['index'], st.session_state['textos'] = criar_faiss_index(df_consolidado)
        st.success("Dados atualizados com sucesso!")

if 'df' in st.session_state:
    st.dataframe(st.session_state['df'])
    pergunta = st.text_input("Faça uma pergunta financeira:")

    if pergunta and st.button("Responder"):
        resposta = consulta_rag(pergunta, st.session_state['index'], st.session_state['textos'])
        st.write("Resposta:", resposta)