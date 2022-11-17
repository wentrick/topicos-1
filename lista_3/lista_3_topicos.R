#install.packages("remotes")
#remotes::install_github("rfsaldanha/microdatasus")

pacman::p_load(tidyverse,microdatasus,vroom,arrow,sparklyr,lubridate, dbplot)

setwd("~/R Documents/R Data/dados_topicos_1/dados_lista3_parquet") #bota teu diretorio da pasta aqui

ufs = c('RO', 'AC', 'AM', 'RR', 'PA' ,'AP', 'TO', 'MA', 'PI', 'CE', 'RN', 'PB', 'PE', 
        'AL', 'SE', 'BA', 'MG', 'ES', 'RJ', 'SP', 'PR', 'SC', 'RS', 'MS', 'MT', 'GO', 'DF')
# 
# for (i in ufs) {
#   dados_sinasc = fetch_datasus(
#     year_start                  = 1994,
#     year_end                   = 2020,
#     uf                              = i,
#     information_system = "SINASC"
#   )
#   dados_sinasc = dados_sinasc %>%
#     select(-contador,-CONTADOR,-1)
#   write_parquet(dados_sinasc,paste0("dados_sinasc_",i,'.parquet'))
# }


#1 b)

file.size("~/R Documents/R Data/dados_topicos_1/dados_lista3_parquet/dados_sinasc_GO.parquet")
file.size("~/R Documents/R Data/dados_topicos_1/dados_lista3_parquet/dados_sinasc_MS.parquet")
file.size("~/R Documents/R Data/dados_topicos_1/dados_lista3_parquet/dados_sinasc_ES.parquet")

file.size("~/R Documents/R Data/dados_topicos_1/dados_lista_3/dados_sinasc_ GO.csv")
file.size("~/R Documents/R Data/dados_topicos_1/dados_lista_3/dados_sinasc_ MS.csv")
file.size("~/R Documents/R Data/dados_topicos_1/dados_lista_3/dados_sinasc_ ES.csv")

#Conexao com o Spark

# Conectando a um cluster spark local

config <- spark_config()
config$spark.executor.cores <- 18
config$spark.executor.memory <- "12G"
sc <- spark_connect(master = "local",
                    config = config
                    )

sinasc_tbl_parquet = spark_read_parquet(sc=sc, 
                                name = "sinasc_tbl_parquet",
                                path = "~/R Documents/R Data/dados_topicos_1/dados_lista3_parquet/dados_sinasc_DF.parquet"
                                
) 

sinasc_tbl_csv = spark_read_csv(sc=sc, 
                                name = "sinasc_tbl_csv",
                                path = "~/R Documents/R Data/dados_topicos_1/dados_lista_3/*.csv"
                                
) 



# filelist = list.files("~/R Documents/R Data/dados_topicos_1/dados_lista3_parquet")
# 
# datalist <- list()
# for(i in 1:2){
#   name <- paste("dataset",ufs[i],sep = "_")
#   datalist[[i]] <- spark_read_parquet(path = filelist[i], sc = sc,
#                                   name = name)
# }
# 



sinasc_tbl_parquet_imputer = sinasc_tbl_parquet %>%
  select(DTNASC,IDADEMAE,PARTO,GRAVIDEZ,GESTACAO,CONSULTAS) %>%
  filter(!is.na(PARTO)) %>%
  filter(PARTO != 9) %>%
  mutate(IDADEMAE = as.numeric(IDADEMAE),
         CONSULTAS = as.numeric(CONSULTAS),
         GESTACAO = as.numeric(GESTACAO)) %>%
  ft_imputer(input_col = "IDADEMAE",output_cols =  "IDADEMAE",strategy = "median")%>%
  ft_imputer(input_col = "GESTACAO",output_cols =  "GESTACAO",missing_value = 9)%>%
  ft_imputer(input_col = "CONSULTAS",output_cols =  "CONSULTAS",missing_value = 9)
  
sinasc_tbl_parquet_imputer = sdf_register(sinasc_tbl_parquet_imputer, "sinasc_tbl_parquet_imputer")

sinasc_tbl_parquet_imputer %>% 
  select(PARTO) %>%
  filter(!is.na(PARTO)) %>%
  filter(PARTO != 9) %>%
  group_by(PARTO) %>%
  summarise(freq = n())

sinasc_tbl_parquet_imputer %>% 
  select(GESTACAO) %>%
  group_by(GESTACAO) %>%
  summarise(freq = n()) %>%
  arrange(-freq)



sinasc_tbl_Dates = sinasc_tbl_parquet_imputer %>%
  select(DTNASC)%>%
  collect()

sinasc_tbl_Dates = sinasc_tbl_Dates %>%
  mutate(DTNASC = as_datetime(dmy(DTNASC)),
         ano = year(DTNASC),
         weekday = wday(ymd(DTNASC)))

sinasc_tbl_dates = copy_to(sc,sinasc_tbl_Dates, "sinasc_tbl_dates")

sinasc_tbl_join = sdf_bind_cols(sinasc_tbl_dates,sinasc_tbl_parquet_imputer) 
sinasc_tbl_join = sdf_register(sinasc_tbl_join, "sinasc_tbl_join")

sinasc_tbl_regression = sinasc_tbl_join %>%
  filter(!is.na(weekday)) %>%
  select(weekday,IDADEMAE,PARTO,CONSULTAS) %>%
  ft_string_indexer(input_col = "PARTO", output_col = "parto_indexed") %>%
  ft_one_hot_encoder(
    input_cols = c("weekday"),
    output_cols = c("weekday_encoded")
  )  %>%
  ft_vector_assembler(
    input_cols = c("weekday_encoded"), 
    output_col = c("weekday_form")
  )  %>%
  ft_vector_assembler(
    input_cols = c("IDADEMAE"), 
    output_col = c("idademae_vec")
  )  %>%
  ft_standard_scaler(input_col = "idademae_vec", output_col = "idademae_norm", 
                       with_mean = TRUE) %>%
  ft_binarizer("CONSULTAS","CONSULTAS_bin",threshold = 5) %>%
  select(weekday_form,idademae_norm,parto_indexed,CONSULTAS_bin)

sinasc_tbl_regression <- sdf_register(sinasc_tbl_regression, "sinasc_tbl_regression")

#
partition <- sinasc_tbl_regression %>%
  sdf_random_split(training = 0.85, test = 0.15, seed = 1281)

# Create table references
data_training <- sdf_register(partition$train, "sinasc_train")
data_test <- sdf_register(partition$test, "sinasc_test")

# Cache
tbl_cache(sc, "sinasc_train")
tbl_cache(sc, "sinasc_test")

formula <- ('parto_indexed ~ weekday_form + idademae_norm + CONSULTAS_bin')

lr2_model <- data_training %>%
  ml_logistic_regression(formula)

validation_summary <- ml_evaluate(lr2_model, data_test)


roc <- validation_summary$roc() %>%
  collect()

validation_summary$area_under_roc()

ggplot(roc, aes(x = FPR, y = TPR)) +
  geom_line() + geom_abline(lty = "dashed")


logistic_pred <- ml_predict(lr2_model, data_test)
logistic_pred

ml_binary_classification_evaluator(logistic_pred)

table(pull(logistic_pred, label), pull(logistic_pred, prediction))






