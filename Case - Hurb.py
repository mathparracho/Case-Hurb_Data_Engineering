#!/usr/bin/env python
# coding: utf-8

# <h1>Case - Hurb Data Engineering (Pandas)</h1>
# <p>Matheus Ramos Parracho</p>
# <br></br>
# <p>Apache Beam é uma framework que realmente nunca tinha utilizado antes. Eu tentei me adaptar para aprender e utilizar essa ferramenta, porém, dado ao meu tempo limitado essa semana por conta das várias provas de fim de período, realmente ficou bastante complicado para eu aprender a utilizar o Apache Beam.</p>
# <br></br>
# <p>Portanto, resolvi o Case com a ferramenta que tenho mais prática, que seria basicamente utilizando a lib "Pandas" para o gerenciamento dos arquivos csv.</p>
# <br></br>
# <p>Peço desculpas mais uma vez por não conseguir tempo para me adaptar ao Apache Beam, mas com certeza com um pouco de mais tempo e umas ajudinhas, eu conseguiria sim utilizar essa framework =)</p>

# In[ ]:


#################################################################


# <h3>Importando as bilbiotecas necessárias</h3>

# In[186]:


import pandas as pd
import json

#pd.set_option("max_rows", None)
pd.set_option('max_columns', None)


# <h3>Importando os arquivos</h3>

# In[187]:


df_estados = pd.read_csv("EstadosIBGE.csv",delimiter=';')
df_covid = pd.read_csv("HIST_PAINEL_COVIDBR_28set2020.csv",delimiter=';')


# In[188]:


df_estados.head()


# In[189]:


df_covid.head()


# <h3>Tratando os dados</h3>

# Necessario: 
# Agregado de informações por estado.
# - Formato: CSV
# - Informação: Regiao, Estado, UF, Governador, TotalCasos, TotalObitos

# In[23]:


#como não encontrei uma tabela de "metadata" específico para esse dataset, fiz alguns testes para deduzir o
#significado de alguns valores.


# In[ ]:


#a soma abaixo indica que as linhas em que a coluna "municipio" possui o valor "NaN"
#representam o caso geral para o estado indicado


# In[ ]:


#para testar, utilizei o Amazonas como exemplo. Fiz a soma de todos os valores de casos acumulados de cada
#municipio no último dia da pesquisa e comparei com o valor do dos casos acumulados no último dia de pesquisa
#do "NaN" para o estado do Amazonas. Logo, confirmamos que o significado do "NaN" é representar
#o caso geral do estado, englobando todos os municipios.


# In[42]:


sum(df_covid.loc[df_covid["estado"] == "AM"].loc[df_covid["data"] == "28/09/2020"]["casosAcumulado"].tolist()[1:])


# In[44]:


df_covid.loc[df_covid["estado"] == "AM"].loc[df_covid["data"] == "28/09/2020"]["casosAcumulado"].tolist()[0]


# In[ ]:


#na lista, a posicao "0", ou seja, a primeira linha, indica o numero dos casos na linha do "NaN"


# In[ ]:


#Portanto, podemos concluir que esse valor significa o total de casos para o estado em questão.


# In[ ]:





# In[ ]:


#separando as linhas com os valores totais e automatizando para todos os estados:


# In[190]:


coduf = df_estados["Código [-]"].tolist()


# In[191]:


df = pd.DataFrame()  
for index in coduf:
    df = df.append(df_covid.loc[df_covid["coduf"] == index].loc[df_covid["data"] == "28/09/2020"].iloc[0])
df.head()


# In[ ]:





# In[ ]:


#criando o dataframe final


# In[192]:


df_case = pd.DataFrame()
df_case["Regiao"] = df["regiao"]
df_case["UF"] = df["estado"]
df_case["TotalCasos"] = df["casosAcumulado"]
df_case["TotalObitos"] = df["obitosAcumulado"]

#para buscar os estados na ordem correta
estados = []
for index in coduf:
    estados += df_estados.loc[df_estados["Código [-]"] == index]["UF [-]"].tolist()
    
df_case["Estado"] = estados

#para buscar os governadores na ordem correta
governadores = []
for index in coduf:
    governadores += df_estados.loc[df_estados["Código [-]"] == index]["Governador [2019]"].tolist()
    
df_case["Governador"] = governadores    

df_case


# <h3>Escrevendo os arquivos</h3>

# In[193]:


#escrevendo o arquivo csv
df_case.to_csv('case_estados.csv',index=False)


# In[ ]:





# In[194]:


#montando o arquivo json
lista_json = []
dicionario = {}
nome_colunas = df_case.columns.tolist()

for index in range(len(df_case.index)):
    for i in range(len(nome_colunas)):
        dicionario[nome_colunas[i]] = df_case.iloc[index].tolist()[i]
    lista_json.append(dicionario)
    dicionario = {}


# In[214]:


json_case = json.dumps(lista_json, ensure_ascii=False).encode('latin1').decode('latin1')
print(json_case)


# In[212]:


#escrevendo o arquivo json
file = open('case_estados.json', 'w')
file.write(json_case)
file.close()


# In[ ]:




