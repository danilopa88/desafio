
# Aplicativo de streaming "5GFlix"

Este projeto visa fornecer uma solução de ETL para analisar dados de filmes e séries disponíveis nas plataformas Amazon e Netflix. O objetivo é transformar e integrar esses dados para suportar a análise de mercado e formular estratégias de negócio para a "5GFlix".



## Diagrama Lógico

O diagrama lógico abaixo ilustra a estrutura proposta para o processamento de dados:

1. **Coleta de Dados**:
    
   - **Amazon**:
     - **Video_v1_00**: Informações sobre vídeos.
     - **Video_DVD_v1_00**: Informações sobre DVDs.
     - **Digital_Video_Download_v1_00**: Informações sobre vídeos digitais.
   - **Netflix**:
     - **Combined Data**: Dados de filmes e séries em formato de texto.
     - **Movie Titles**: Dados de títulos de filmes.

2. **Pré-processamento dos Dados**: **AWS EMR e Apache Spark**
   - **Filtragem por Língua**: Apenas registros em inglês são mantidos.
   - **Limpeza de Dados**: Correção de valores nulos e inconsistências.

3. **Transformação dos Dados**: 
    **AWS EMR e Apache Spark**
   - **Netflix**:
     - **Extração e Processamento**: Extração de colunas relevantes, tratamento de IDs e rankings, e conversão de datas.
   - **Amazon**:
     - **Transformação de Colunas**: Conversão de IDs, avaliações, e datas, e filtragem por marketplace.

4. **Integração e Agregação**:
   - **Unificação dos Dados**: Combinação dos dados da Amazon e Netflix.
   - **Agregação e Junção**: Criação de DataFrames de avaliações, clientes e filmes únicos.

5. **Orquestração**:

   - **AWS Step Functions**: Originalmente o plano era realizar a orquestração pela Step Function, entretanto não tive tempo hábil. Porém o esboço se encontra no arquivo scripts/step_function.json.

6. **Armazenamento e Consulta**:
   - **Armazenamento no Amazon S3 e Delta Lake**: Os dados transformados são armazenados em formato Parquet para consultas eficientes.
   - **Consulta com AWS Athena**: Utilização do AWS Athena para consultas SQL sobre os dados armazenados no S3.

