# --- Script para Descarregar Discursos da CPI da Pandemia ---

# Objetivo: Baixar os discursos da CPI da Pandemia a partir da Base dos Dados
# e salvá-los num ficheiro CSV para a tarefa de LDA.

# --- 1. Instalação e Carregamento de Pacotes ---
# Certifique-se de que os pacotes estão instalados.
# install.packages(c("tidyverse", "DBI", "dbplyr", "basedosdados", "bigrquery"))
library(tidyverse)
library(DBI)
library(dbplyr)
library(basedosdados)
library(bigrquery)

# --- 2. Configuração e Conexão ---
# Autenticação e definição do billing project ID
bq_auth(path = "gcp-credentials.json")
set_billing_id("ce2r20251")

# Fazer a conexão com o BigQuery
con <- dbConnect(
  bigrquery::bigquery(),
  project = "basedosdados",
  billing = "ce2r20251"
)
cat("Conexão via DBI estabelecida.\n")


# --- 3. Definição da Consulta ---
# Esta consulta seleciona discursos da tabela `discursos` na base `br_senado_cpipandemia`.
# Usamos dbplyr para construir a consulta de forma preguiçosa (lazy).
consulta_lazy <- tbl(con, 
                     Id(catalog = "basedosdados", 
                        schema = "br_senado_cpipandemia", 
                        table = "discursos")
) |>
  filter(!is.null(texto_discurso)) |>
  select(sequencial_sessao, nome_discursante, data_sessao, texto_discurso)

# Renderizar a consulta para uma string SQL para o dry run e para o download
consulta_string <- sql_render(consulta_lazy)

# --- 4. Estimativa de Custo (Dry Run) ---
custo_estimado <- bq_perform_query_dry_run(consulta_string, billing = "ce2r20251")
cat("Custo estimado para a consulta (dry run):\n")
print(custo_estimado)


# --- 5. Execução da Consulta e Download dos Dados ---
cat("\nA iniciar o download dos dados da Base dos Dados. Isto pode levar alguns minutos...\n")
discursos_cpi <- read_sql(consulta_string)
cat("Download concluído com sucesso!\n")


# --- 6. Limpeza e Preparação Final ---
# Vamos filtrar por um comprimento mínimo de texto para garantir conteúdo substancial.
documentos_preparados <- discursos_cpi %>%
  mutate(
    comprimento_texto = nchar(texto_discurso)
  ) %>%
  filter(comprimento_texto > 1000) %>% # Manter apenas discursos com mais de 1000 caracteres
  select(sequencial_sessao, nome_discursante, data_sessao, texto_discurso) %>%
  rename(doc_id = sequencial_sessao, author = nome_discursante, date = data_sessao, text = texto_discurso)

cat(paste("Foram processados", nrow(documentos_preparados), "discursos.\n"))

# --- 7. Salvar os Dados num Ficheiro CSV ---
# Este é o ficheiro que você irá distribuir para os seus alunos.
write_csv(documentos_preparados, "cpi_pandemia_discursos.csv")

cat("Os dados foram salvos com sucesso no ficheiro 'cpi_pandemia_discursos.csv'.\n")
