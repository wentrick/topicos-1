pacman::p_load(dplyr, tidyverse, rmdformats, stringr, 
       vroom, mongolite, RSQLite, DBI, dbplyr, geobr, data.table, sparklyr)
setwd("~/R Documents/R Data/dados_topicos_1")

### leitura dos dados completos do IBGE e Geobr ----
banco_IBGE <- read_csv("~/R Documents/R Data/tableExport.csv")# dados necessarios das regioes de saude
colnames(banco_IBGE) = c('index','cidade','municipio','code_IBGE_region','code_health_region','nome_health_region')  
banco_IBGE = banco_IBGE %>%
  select(-index)

banco_geobr = as.data.frame(read_health_region()) %>%
  select(-geom)

### leitura dos dados completos com colunas filtradas ----
(pasta_arquivos <- "~/R Documents/R Data/dados_topicos_1")

nomes_arquivos <- list.files(pasta_arquivos, "vacinas")

comando_pipe <- "findstr JANSSEN vacinas_acre_parte_1.csv"

comando_pipe <- "findstr  ." %>% 
  str_c(str_c(" ",nomes_arquivos, collapse = " "))

dt_dados_completos = fread(cmd = comando_pipe, encoding = 'UTF-8', 
                           col.names = c("estabelecimento_uf","vacina_descricao_dose","code_IBGE_region"), 
                           nThread = 12, 
                           select = c(20,29,18)
) 


### criando uma conexao ----

mydb <- dbConnect(RSQLite::SQLite(), "my-db.sqlite")
 

# adicionando dados vacina
dbWriteTable(mydb,"vacinas_dt",dt_dados_completos)


## pegando as 5 primeiras linhas
dbGetQuery(mydb, "SELECT * FROM vacinas_dt LIMIT 5")

## adicionando as outras tabelas

dbWriteTable(mydb,"codigos_IBGE",banco_IBGE)

dbWriteTable(mydb,"banco_geobr",banco_geobr)

## adicionando tabela Geobr

dbReadTable(mydb, "banco_geobr")

#adicionando dados do IBGE

dbReadTable(mydb, "codigos_IBGE")

### listando as tabelas
dbListTables(mydb)

###

dbGetQuery(mydb,"
 SELECT name_health_region, count(name_health_region) 
 FROM codigos_IBGE 
 LEFT JOIN vacinas_dt on codigos_IBGE.code_IBGE_region = vacinas_dt.code_IBGE_region
 LEFT JOIN banco_geobr on banco_geobr.code_health_region = codigos_IBGE.code_health_region
 WHERE vacina_descricao_dose = '2ª Dose'
 GROUP BY nome_health_region
 ORDER BY count(nome_health_region) DESC")



### Conexao MongoDB ----

cx_geobr <- mongo(collection = "geobr",
            db = "dados",
            url ="mongodb+srv://wentrick:davi2001@cluster0.f7fxeme.mongodb.net/test")


cx_geobr$insert(banco_geobr)


cx_ibge <- mongo(collection = "ibge",
            db = "dados",
            url ="mongodb+srv://wentrick:davi2001@cluster0.f7fxeme.mongodb.net/test")


cx_ibge$insert(banco_IBGE)


cx <- mongo(collection = "vacinas",
            db = "dados",
            url ="mongodb+srv://wentrick:davi2001@cluster0.f7fxeme.mongodb.net/test")


cx$insert(dt_dados_completos)



df <- cx_geobr$aggregate('[
    {
      "$lookup":
        {
          "from": "geobr",
          "localField": "code_health_region",
          "foreignField": "code_health_region",
          "as": "vacina_IBGE"
        }
   }
]')

# spark

config <- spark_config()
config$spark.executor.cores <- 12
config$spark.executor.memory <- "12G"
# config$spark.driver.memory = "4G"
# config$spark.driver.memoryOverhead = "4G"
# config$spark.memory.offHeap.enabled = "true"
# config$spark.memory.offHeap.size = "12G"
sc <- spark_connect(master = "local", config = config)

# leitura das bases
setwd("~/R Documents/R Directory/lista_2_topicos")

# write.csv(banco_IBGE, "dados_ibge.csv")
# write.csv(banco_geobr, "dados_geobr.csv")
# write.csv(dt_dados_completos, "dados_vacinas.csv", fileEncoding = "latin1")

dados_geobr <- vroom("dados_geobr.csv")
dados_ibge <- vroom("dados_ibge.csv")
dados_vacinas <- vroom("dados_vacinas.csv")

geo_tbl = spark_read_csv(
  sc = sc,
  name = "geo_tbl",
  path = "dados_geobr.csv",
  header = TRUE,
  delimiter = ",",
  charset = "latin1",
  infer_schema = T
)


ibge_tbl = spark_read_csv(
  sc = sc,
  name = "ibge_tbl",
  path = "dados_ibge.csv",
  header = TRUE,
  delimiter = ",",
  charset = "latin1"
) 

vacinas_tbl = spark_read_csv(
  sc = sc,
  name = "vacinas_tbl",
  path = "dados_vacinas.csv",
  header = TRUE,
  delimiter = ",",
  charset = "latin1",
  infer_schema = T
)

ibge_tbl = ibge_tbl %>%
  select(-1)

geo_tbl = ibge_tbl %>%
  select(-1)

geo_ibge_join = left_join(geo_tbl,ibge_tbl) 

vacinas_geo_ibge_join = left_join(geo_ibge_join,vacinas_tbl)

vacinas_geo_ibge_join = sdf_register(vacinas_geo_ibge_join,"vacinas_geo_ibge_join")

vacinas_geo_ibge_join %>% filter(vacina_descricao_dose == "2ª Dose") %>%
  group_by(`Nome da Região de Saúde`) %>% 
  summarise(freq = n()) %>%
  mutate(median = median(freq)) %>%
  mutate(nivel = case_when(freq > median(freq) ~ 'LOW',
                           freq < median(freq) ~ 'HIGH'))
