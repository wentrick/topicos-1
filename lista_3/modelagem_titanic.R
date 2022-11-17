# Carregando os pacotes usados
pacman::p_load(sparklyr, tidyverse)

# Conectando a um cluster spark local
config <- spark_config()
config$spark.executor.cores <- 6
config$spark.executor.memory <- "12G"
sc <- spark_connect(master = "local", config = config)


# Listando os arquivos considerados
path <- "~/R Documents/R Data/dados_topicos_1/titanic"
list.files(path)



{start <- Sys.time()
  titanic_tbl = spark_read_csv(sc=sc, 
                               name = "titanic_tbl",
                               path = file.path(path,"train.csv"), 
                               header = TRUE, 
                               delimiter = ",", 
                               charset = "latin1",
                               infer_schema = T)
  spark_data_read_time <- Sys.time() - start
}

{tm <- sum(file.size(file.path(path,"train.csv"))/(10^3))
  print(paste("Levou", 
              round(spark_data_read_time, digits = 2),
              units(spark_data_read_time),
              "para carregar o conjunto de dados de",
              round(tm),
              "KB no SparklyR"))
}


sdf_dim(titanic_tbl) # semelhante a funçao dim() porém no ambiente spark


sdf_describe(titanic_tbl) # semelhante a funçao summary() porém no ambiente spark


glimpse(titanic_tbl)

# Verificando a quantidade de observações faltantes por variável
titanic_tbl %>%
  summarise_all(~sum(as.integer(is.na(.))))

# Calculando a proporção de sobreviventes por sexo biológico
titanic_tbl %>%
  group_by(Sex,Survived) %>% 
  tally() %>% 
  mutate(frac = round(n / sum(n),2)) %>% 
  arrange(Sex,Survived)

# Calculando a proporção de sobreviventes pelo local de embarque
titanic_tbl %>%
  group_by(Embarked,Survived) %>% 
  tally() %>% 
  mutate(frac = round(n / sum(n),2)) %>% 
  arrange(Embarked,Survived)

# Criando um histograma da variável Idade
library(dbplot)
options(scipen = 999)
dbplot_histogram(titanic_tbl, Age)

# Inputando a média para os dados faltantes da variável Age
model_tbl <- titanic_tbl %>%
  select(Survived, Pclass, Sex, Age, Fare, SibSp, Parch, Name, Embarked) %>%
  filter(!is.na(Embarked)) %>%
  mutate(Age = if_else(is.na(Age), mean(Age), Age)) 
glimpse(model_tbl)


# Calculando as medidas necessárias para a normalização.
scale_values <- model_tbl %>%
  summarize(
    mean_Age = mean(Age),
    mean_Fare = mean(Fare),
    sd_Age = sd(Age),
    sd_Fare = sd(Fare)
  ) %>%
  collect()

# Normalizando as variáveis Age e Fare.
## Usamos !! ou local() para calcular os valores no R
model_tbl <- model_tbl %>%
  mutate(scaled_Age = (Age - local(scale_values$mean_Age)) / 
           !!scale_values$sd_Age,
         scaled_Fare = (Fare - !!scale_values$mean_Fare) /
           !!scale_values$sd_Fare)

# Usando a função ft_standard_scaler ou ft_normalizer() 
teste <- model_tbl %>%
  ft_vector_assembler(input_col = "Age",
                      output_col = "Age_temp") %>%
  ft_standard_scaler(input_col = "Age_temp",
                output_col = "Age_scaled2",
                with_mean = T) %>%
  select(Age, Age_temp, scaled_Age, Age_scaled2) %>%
  collect()

# Criando variáveis que codificam os títulos dos passageiros
title <- c("Master", "Miss", "Mr", "Mrs")

title_vars <- title %>% 
  map(~ expr(ifelse(rlike(Name, !!.x), 1, 0))) %>%
  set_names(str_c("title_", title))

model_tbl <- model_tbl %>% 
  mutate(local(title_vars),
         title_Mr = if_else(title_Mrs == 1, 0, title_Mr),
         title_officer = if_else(
           title_Mr == 0 && title_Mrs == 0 &&
             title_Master == 0 && title_Miss == 0, 1, 0))

model_tbl %>%
  select(starts_with("title"), Name)

# Particionando o conjunto de dados em treino e teste
partition <- model_tbl %>%
  sdf_random_split(training = 0.85, test = 0.15, seed = 1281)

# Create table references
data_training <- sdf_register(partition$train, "trips_train")
data_test <- sdf_register(partition$test, "trips_test")

# Cache
tbl_cache(sc, "trips_train")
tbl_cache(sc, "trips_test")

formula <- ('Survived ~ Sex + scaled_Age + scaled_Fare + Pclass +
            SibSp + Parch + Embarked + title_Mr + title_Mrs + title_Miss +
            title_Master + title_officer')

lr_model <- glm(formula,
                family = binomial(link='logit'),
                data = data_training)
summary(lr_model)

# Prevendo os dados no conjunto de teste
pred.test <- predict(lr_model, data_test) > 0

# Calculando o percentual de acertos 
mean(pred.test == data_test %>% pull(Survived)) # Percentual de acertos

table(pred.test, data_test %>% pull(Survived)) # matrix de confusão

#usando o sparklyr
lr2_model <- data_training %>%
  ml_logistic_regression(formula)

# Prevendo usando o spark - Predict()
validation_summary <- ml_evaluate(lr2_model, data_test) 

roc <- validation_summary$roc() %>%
  collect()

validation_summary$area_under_roc()

ggplot(roc, aes(x = FPR, y = TPR)) +
  geom_line() + geom_abline(lty = "dashed")




