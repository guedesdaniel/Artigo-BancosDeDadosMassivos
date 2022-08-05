from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import countDistinct
import time

sc = SparkContext.getOrCreate()
spark = SparkSession(sc)

df = spark.read.csv("gs://bucket-hadoop-daniel/tables/Pagamentos.csv/", sep = ';', header = True)

new_columns = ['id_viagem', 'numero_proposta', 'codigo_orgao_superior', 'nome_orgao_superior',
               'codigo_orgao_pagador', 'nome_orgao_pagador', 'codigo_uni_gestora_pagadora', 'nome_uni_gestora_pagadora',
              'tipo_pagamento', 'valor']

df = df.toDF(*new_columns)
df = df.withColumn('valor',df['valor'].cast('int')) # cast(int) transforma a coluna em int
df = df.withColumn('id_viagem',df['id_viagem'].cast('int'))

# Selecionar as colunas id_viagem, orgao superior, orgao pagador, tipo de pagamento e valor
df1 = df.select(df['id_viagem'], df['nome_orgao_superior'], df['nome_orgao_pagador'], df['tipo_pagamento'], df['valor'])


#Contagem de viagens - orgao superior

start = time.time()
df_tmp = df1
df_tmp = df_tmp.where(df_tmp.tipo_pagamento == "PASSAGEM").select('*')
df_tmp = df_tmp.select('id_viagem', 'nome_orgao_superior').distinct()
df_tmp = df_tmp.groupby('nome_orgao_superior').count()
df_tmp = df_tmp.select(col('nome_orgao_superior'),col('count').alias('numero_viagens'))


num_viagens_total = df_tmp.sort(col('numero_viagens').desc())

print('============\nTabela Número de Viagens por Órgão Superior\n==========')
df_tmp = df_tmp.sort(col('numero_viagens').desc()).show()

end= time.time()
print('Tempo em segundos: ', end-start)

#Salvar arquivo csv do resultado
num_viagens_total.write.format("csv").save("gs://bucket-hadoop-daniel/testes/resultado-orgao-superior-contagem")


###########Soma de gasto em viagem - orgao superior################

start = time.time()

df_tmp = df1
df_tmp = df_tmp.groupby('nome_orgao_superior').sum('valor')
df_tmp = df_tmp.select(col('nome_orgao_superior'), col('sum(valor)').alias('valor_viagens'))
valor_gasto = df_tmp.sort(col('valor_viagens').desc())

print('============\nTabela Valor gasto em Viagens por Órgão Superior\n==========')
df_tmp = df_tmp.sort(col('valor_viagens').desc()).show()
end = time.time()
print('Tempo em segundos: ', end-start)


#Salvar arquivo csv do resultado
valor_gasto.write.format("csv").save("gs://bucket-hadoop-daniel/testes/result-orgao-superior-gastos")


# As 10 Viagens mais caras

start = time.time()

df_viagem = df1.groupby('id_viagem').sum('valor')
print('============\nTabela - 10 Viagens mais caras\n==========')
df_viagem = df_viagem.sort(col('sum(valor)').desc()).show(10)


end = time.time()
print('\nTempo em segundos: ', end-start)


########## Viagens internacionais x Viagens nacionais##############

# Tratando os dados
df2 = spark.read.csv("gs://bucket-hadoop-daniel/tables/Trecho-Tables/", sep = ';', header = True)
new_columns = ['id_viagem', 'numero_proposta', 'sequencia_trecho', 'origem_data', 'origem_pais', 'origem_uf',
               'origem_cidade', 'destino_data', 'destino_pais', 'destino_uf', 'destino_cidade',
               'meio_transporte', 'numero_diarias', 'missao']


df2 = df2.toDF(*new_columns)
df2 = df2.withColumn('id_viagem',df2['id_viagem'].cast('int'))
df_tr = df2
df_tr = df_tr.select(df_tr['id_viagem'], df_tr['origem_pais'], df_tr['destino_pais'])

####### ===> Viagens nacionais <==== #####

start = time.time()

df_tr_viagem_nacional = df_tr.where("origem_pais == 'Brasil' AND destino_pais == 'Brasil'").select('*')
#df_tr_viagem_nacional = df_tr_viagem_nacional.groupby('id_viagem').count()
df_tr_viagem_nacional = df_tr_viagem_nacional.select(countDistinct('id_viagem').alias('viagens_nacionais'))

print('\n\n==============\nNúmero de viagens Nacionais\n=============')
df_tr_viagem_nacional.show()

end = time.time()
print('Tempo (em segundos): ', end-start)

#SALVANDO NO BUCKET
df_tr_viagem_nacional.write.format("csv").save("gs://bucket-hadoop-daniel/testes/result-viagens-nacionais")

######### ====> Viagens Internacionais

start = time.time()

df_tr_viagem_internacional = df_tr.where("origem_pais == 'Brasil' AND destino_pais != 'Brasil'").select('*')
df_tr_viagem_internacional = df_tr_viagem_internacional.select(countDistinct('id_viagem'))

print('\n\n==============\nNúmero de viagens Internacionais\n=============')
df_tr_viagem_internacional.show()

end = time.time()
print('Tempo (em segundos): ', end-start)

df_tr_viagem_internacional.write.format("csv").save("gs://bucket-hadoop-daniel/testes/result-viagens-internacionais")

#################################################################################
############## Viagem mais cara e seus destinos -- JOIN ENTRE TABELAS############
#################################################################################

start= time.time()

df1.createOrReplaceTempView('pagamentos')
df2.createOrReplaceTempView('trechos')

joinDF12= spark.sql("SELECT p.id_viagem, t.destino_pais, t.destino_uf, SUM(p.valor) AS valor_viagem \
FROM pagamentos p \
INNER JOIN trechos AS t ON (p.id_viagem == t.id_viagem) \
GROUP BY p.id_viagem, t.destino_pais, t.destino_uf ORDER BY valor_viagem DESC LIMIT 10")

print('\n\n===============\nTabela Viagem mais caras e seus destinos\n==========')
joinDF12.show(10)

end = time.time()
print('Tempo (em segundos): ', end-start)

#Salvando
joinDF12.write.format("csv").save("gs://bucket-hadoop-daniel/testes/result-viagens-mais-caras-com-destino")

