devtools::install_github("ipeaGIT/geobr", subdir = "r-package")
pacman::p_load(tidyverse, vroom, data.table,devtools,fs,cli,dtplyr,geobr)
setwd("~/R Documents/R Data/dados_topicos_1")

### carregando AC parte 1 ----
dados_acre1 = vroom("~/R Documents/R Data/dados_topicos_1/vacinas_acre_parte_1.csv", delim = ';')
banco_IBGE <- read_csv("~/R Documents/R Data/tableExport.csv")# dados necessarios das regioes de saude

### vendo o tamanho dos arquivos ----

file.size("~/R Documents/R Data/dados_topicos_1/vacinas_acre_parte_1.csv") #262.048845 MB

arquivos = list.files("~/R Documents/R Data/dados_topicos_1")

sum(unlist(map(arquivos,file.size))) #4517.386542 MB

tamanho_banco_completo = object.size(dados_acre1) #249.78428799999998 MB

### carregando apenas a vacina janssen ----


comando_pipe <- "findstr JANSSEN vacinas_acre_parte_1.csv"

variaveis_interesse <- c("paciente_idade", "paciente_dataNascimento", "paciente_enumSexoBiologico",
                         "paciente_racaCor_valor", "paciente_endereco_nmMunicipio", "paciente_endereco_nmPais",
                         "paciente_endereco_uf", "vacin_grupoAtendimento_nome", "vacina_categoria_nome","vacina_fabricante_nome",
                         "vacina_descricao_dose")

dados_acre_janssen <- vroom(pipe(comando_pipe), 
                         locale = locale("br", encoding = "latin1"),
                         #col_select = variaveis_interesse
                         )

tamanho_banco_filtrado = object.size(dados_acre_janssen) #8.249176 MB

tamanho_banco_completo - tamanho_banco_filtrado  #241.535112 MB

###  lendo multiplos arquivos com o vroom ----

(pasta_arquivos <- "~/R Documents/R Data/dados_topicos_1")

nomes_arquivos <- list.files(pasta_arquivos, "vacinas")

comando_pipe <- "findstr JANSSEN " %>% 
  str_c(str_c(" ",nomes_arquivos, collapse = " "))

dados_vacina_completo <- vroom(pipe(comando_pipe), 
                         locale = locale("br", encoding = "latin1"),
                         #col_select = variaveis_interesse
                         delim = ";")
### banco geobr e leitura dos dados completos com colunas filtradas ----

banco_geobr = as.data.table(read_health_region()) %>%
  select(-geom)

colnomes = colnames(dados_acre1)

comando_pipe <- "findstr  ." %>% 
  str_c(str_c(" ",nomes_arquivos, collapse = " "))

dt_dados_completos = fread(cmd = comando_pipe, encoding = 'UTF-8', 
                           col.names = c("estabelecimento_uf","vacina_descricao_dose","code_IBGE_region"), 
                           nThread = 12, 
                           select = c(20,29,18)
                           ) 


### Join das 3 bases (Geobr, dados da vacina e tabela com o codigo do IBGE)

dt_dados_completos = dt_dados_completos %>%
  mutate(code_IBGE_region = as.character(code_IBGE_region)) #modificando o nome da coluna para ser igual entre as bases para o join
  

banco_IBGE = banco_IBGE %>%
  mutate(code_health_region = as.character(`Cód Região de Saúde`), #modificando os nomes das colunas para os joins
         code_IBGE_region = as.character(`Cód IBGE`)) %>%
  select(-`Cód Região de Saúde`,-`Cód IBGE`)

colnames(banco_IBGE) = c('index','uf','municipio','cod_IBGE','cod_regiao_saude','nome_regiao_saude')  

#setnames(banco_IBGE, c('index','uf','municipio','cod_IBGE','cod_regiao_saude','nome_regiao_saude'))

dados_geobr_ibge <- merge(banco_IBGE, banco_geobr, by = "code_health_region") #join da tabela do IBGe com o Geobr por meio do codigo de regiao de saude

dados_finais = merge(dados_geobr_ibge,dt_dados_completos , by = "code_IBGE_region") #join da tabela formada pelo join naterior com a coluna das vacinas


regiao_vacina = dados_finais[vacina_descricao_dose %in% c("2ª Dose"), .N, by = .(vacina_descricao_dose, nome_regiao_saude)]


dados_finais = dados_finais %>%
  select(1,2,6,11,12) %>%
  filter(vacina_descricao_dose == "2ª Dose") %>%
  group_by(`Nome da Região de Saúde`) %>% 
  summarise(freq = n()) %>%
  mutate(median = median(freq)) %>%
  mutate(nivel = case_when(freq > median(freq) ~ 'LOW',
                           freq < median(freq) ~ 'HIGH'))       
teste = as.data.frame(dados_finais)


