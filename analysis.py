import pandas as pd
import numpy as np
import sqlite3
import matplotlib.pyplot as plt
import seaborn as sns 

pd.options.display.max_columns = None

conn = sqlite3.connect("data/Martech.db")

# Você deve desenhar e explicar o fluxo de dados eficiente cruzando as duas
# tabelas e explicando como funciona essa rotina
query_A = """
SELECT rd.client_id, rd.session_id, rd.transaction_id, rd.event_timestamp, 
       rd.event_name, rd.value,
       t.product_category, t.status
FROM raw_data rd
INNER JOIN transactions t
ON rd.transaction_id = t.transaction_id
WHERE t.status = 'PAID'
"""

res_A = pd.read_sql(query_A, conn)
res_A
transactions

# B. Crie um código que vai tratar a “raw_data” e vai transformar numa tabela
# somente com as compras realizadas “web_orders”, ela deve ser usada nas
# próximas questões
# Assumindo que 'web_orders' se refere as compras com source  google, facebook, tiktok:
query_B = """
SELECT *
FROM raw_data
WHERE source IN ('google', 'facebook', 'tiktok') AND event_name = 'compra'
"""

res_B = pd.read_sql(query_B, conn)
res_B

# Crie uma tabela agregada com os pedidos que tenha data do evento,
# client_id,session_id,transaction_id, status do pedido e origem (source).
raw_data.columns
transactions.columns
media_spend

query_C = """
CREATE TABLE AS tabela_c
SELECT rd.event_date, rd.client_id, rd.session_id, rd.transaction_id, t.status, rd.source, t.revenue
FROM raw_data rd
INNER JOIN transactions t
ON rd.transaction_id = t.transaction_id
WHERE source IN ('google', 'facebook', 'tiktok') AND event_name = 'compra';
"""
res_C = pd.read_sql(query_C, conn)
res_C

# Usando um ambiente python, utilize os dados gerados acima e monte um
# payload com os dados dos usuários que realizaram uma compra e a
# pagaram. As variáveis que devem ser enviadas são client_id,
# session_id,value,product_category.
query_D = """
SELECT rd.client_id, rd.session_id, rd.transaction_id, rd.value, t.product_category
FROM raw_data rd
INNER JOIN transactions t
ON rd.transaction_id = t.transaction_id
WHERE t.status = 'PAID'
"""

def fetch_results(connection, query, batch=100):
    cursor = connection.cursor()
    res = cursor.execute(query)
    out = []
    while True:
        temp = res.fetchmany(batch)
        if not temp:
            break
        out.extend(temp)
    cursor.close()
    out = pd.DataFrame(out)
    out.columns = [col[0] for col in cursor.description]
    return out

fetch_results(conn, query_D)

# 2. Crie uma visão com os dados que manipulou acima e adicione a tabela ‘media_spend’.
# Nela você encontra os investimentos por dia por mídia. Nessa visão será necessário colocar:
query_2a = """
select data, 
	   sum(value) as compras,
	   (select ifnull(sum(value),0) from data_vis d where d.status = 'PAID' and d.data = dv.data) as compras_pagas,
	   sum(revenue) as receita,
	   (select ifnull(sum(media_spend), 0) from data_vis d where d.media_id = 1 and d.data = dv.data)  as investimento
from data_vis dv 
group by data
order by data
"""
df2a = fetch_results(conn, query_2a)
df2a["data"] = pd.to_datetime(df2a["data"])
df2a["data"].max()
df2a["data"].min()

import matplotlib.dates as mdates

days_interval = mdates.DayLocator(interval=2)
day_month_formatter = mdates.DateFormatter("%d/%m")
cols = ["compras", "compras_pagas", "receita", "investimento"]

# Configurar subplots em um loop
fig, axes = plt.subplots(nrows=2, ncols=2,figsize=(15, 6))

# Iterar sobre cada coluna (exceto a coluna X) e criar um subplot para cada uma
for i, column in enumerate(cols):
    row = i // 2  # Calcula a linha
    col = i % 2   # Calcula a coluna
    sns.lineplot(x='data', y=column, data=df2a, ax=axes[row, col])
    axes[row, col].set_title(column)
    axes[row,col].xaxis.set_major_locator(mdates.MonthLocator())
    axes[row,col].xaxis.set_major_formatter(year_month_formatter) # formatter for major axis only
# Ajustar o layout
plt.tight_layout()
# Mostrar o gráfico
plt.show()



# Configurar a grade de subplots
grid = sns.FacetGrid(df2a.melt(id_vars = "data"), col="variable", col_wrap=2, height=4, sharey=False)

# Adicionar linhas aos subplots
grid.map(sns.lineplot, "data", "value")

# Adicionar rótulos e título
grid.set_axis_labels("X-axis", "Y-axis")
grid.fig.suptitle("Gráfico de Vários Subplots de Linha")

# Ajustar o layout
plt.tight_layout(rect=[0, 0, 1, 0.96])
import matplotlib.pyplot as plt
# Mostrar o gráfico
plt.show()

# Visão de compras foram realizadas comparando com as compras realmente
# pagas (taxa de pagamento)

query_2b = """
select sum(value) as compras,
	   (select ifnull(sum(value), 0) from data_vis d where d.status = 'PAID') as compras_pagas
from data_vis dv 
"""

df2b = fetch_results(conn, query_2b)
sns.barplot(df2b.melt(), x="variable", y = "value")
plt.show()


# Uma vista com o acumulado de resultados do período por source e
# adicionando CPA (investimento/compras pagas) e ROAS
# (receita/investimento)

query_2c = """
select source,
	   sum(value) as compras,
	   (select ifnull(sum(value), 0) from data_vis d where d.status = 'PAID' and d.source=dv.source) as compras_pagas,
	   sum(revenue) as receita,
	   (select ifnull(sum(media_spend), 0) from data_vis d where d.media_id = 1 and d.source = dv.source)  as investimento
from data_vis dv 
group by source
order by data
"""

df2c = fetch_results(conn, query_2c)
df2c["cpa"] = df2c["investimento"] / df2c["compras_pagas"]
df2c["roas"] = df2c["receita"] / df2c["investimento"]
df2c

# plotando bar plot de cada variável categorica com target:
cols = ["comrpas", "comrpas_pagas", "receita", "investimento", "cpa", "roas"]
plt.figure(figsize=(14, len(cols) * 3))
for idx, feature in enumerate(cols , 1):
    plt.subplot(len(cols), 3, idx)
    # plotting bar plot 
    sns.barplot(data=df2c, x="source", y=feature, errorbar='se')
    plt.title(f'{feature}')
    plt.xlabel('Source')
    plt.ylabel('')
# ajuste de layout:
plt.tight_layout()


#  Uma vista de vendas por categoria de produto
query_2d = """
select product_category,
	   sum(value) as compras,
	   sum(revenue) as receita
from data_vis dv 
where product_category != "None"
group by product_category
order by product_category
"""

df2d = fetch_results(conn, query_2d)
df2d

sns.barplot(data=df2d.sort_values(by="receita"), x="receita", y='product_category', orient='h')
plt.show()
plt.clf()
