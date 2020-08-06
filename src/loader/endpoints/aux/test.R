# ! /usr/bin/Rscript

# install.packages("EpiEstim", repo="https://cloud.r-project.org/");
# install.packages("RCurl", repo="https://cloud.r-project.org/");
library(tidyverse)
library(EpiEstim);
library(RCurl);

args = commandArgs(trailingOnly=TRUE)

params <- list(args[0], args[1], args[2])
print(params)

now <- function(params){
   
   df_cities_cases = read.csv(text = getURL("http://datasource.coronacidades.org/br/cities/cases/full"))

   df_state_cases = df_cities_cases %>% select("state","city_id","last_updated","daily_cases", "confirmed_cases") %>% #Seleciona as colunas utilizadas pelo modelo
      filter(!is.na(daily_cases)) %>% # Filtra os dias para que não haja NA
      group_by(state,last_updated) %>% # Agrupa os dados por estado e data
      summarize(new_cases = sum(daily_cases), total_cases = sum(confirmed_cases)) # Soma casos dos municipios

    # Altera nomes das colunas
    names(df_state_cases)[names(df_state_cases) == 'state'] <- 'state_id'
    names(df_state_cases)[names(df_state_cases) == 'last_updated'] <- 'dates'

    # Converção dos formatos dos dados
    df_state_cases$state_id = as.character(df_state_cases$state_id)
    df_state_cases$dates = as.Date(df_state_cases$dates)

    # Adicionando a coluna de população ao df
    df_state_cases = merge(x = df_state_cases, 
                        y = as.data.frame((df_farol %>% select(state_id, population))),
                        by = "state_id", all = TRUE)

    # Média móvel de nivis casos (7 dias)
    df_state_cases = df_state_cases %>% 
                        group_by(state_id) %>%
                        mutate(new_cases_mavg = runMean(new_cases, 7))

    # Incidencia: Infectados por 100k/hab. 
    df_state_cases$I = (100e3 * df_state_cases$new_cases_mavg)/df_state_cases$population

    # Filtra início da série
    df_state_cases = df_state_cases %>%
                        filter(!is.na(I)) %>%
                        filter(total_cases >= 15)

    # Roda o modelo
    # Gera a tabela dos estados
    rt_cori_serie = 0
    states = as.vector(unique(df_state_cases$state_id)) 

    for(st in states){
        rt = estimate_R(df_state_cases %>% filter(state_id == st) %>% select(dates, I), 
                        method="parametric_si",
                        config = make_config(list(
                            mean_si = 4.7,
                            std_si = 2.9,
                            mean_prior=3))
                    )
        
        rtx = bind_cols(state_id=st,rt$R)
        
        if(rt_cori_serie == 0){
            rt_cori_serie = rtx
                }
        else{
            rt_cori_serie = bind_rows(rt_cori_serie,rtx)
        }
    }

    # Retorna a série
    return(rt_cori_serie)
}

# Run script
now(params)


# => STDOUT:
# print("another \n")
# print("this is a print \n")
# cat("this is a cat \n")
# => TERMINAL:
# message("this is a message \n")
# warning("this is a warning")
# stop("this is a stop, or error!")

# sayHello <- function(){
#    print('hello')
# }
# sayHello()