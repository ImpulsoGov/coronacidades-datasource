# ! /usr/bin/Rscript

# install.packages("EpiEstim", repo="https://cloud.r-project.org/");
# install.packages("RCurl", repo="https://cloud.r-project.org/");
library(tidyverse);
library(EpiEstim);
library(RCurl);
library(vroom);
library(TTR);

args = commandArgs(trailingOnly=TRUE)
params <- list(args[0], args[1], args[2])
print(params)

now <- function(params){
   
   df_cities_cases = vroom("http://datasource.coronacidades.org/br/cities/cases/full", delim=',')
   df_farol = vroom("http://datasource.coronacidades.org/br/states/farolcovid/main", delim=',')

    df_state_cases = df_cities_cases %>% select("state_num_id","city_id","last_updated","active_cases", "confirmed_cases") %>%
        filter(!is.na(active_cases)) %>% # Filtra os dias para que não haja NA
        group_by(state_num_id,last_updated) %>% # Agrupa os dados por estado e data
        summarize(active_cases = sum(active_cases), total_cases = sum(confirmed_cases), .groups = 'drop') # Soma casos dos municipios

    # Altera nomes das colunas
    names(df_state_cases)[names(df_state_cases) == 'last_updated'] <- 'dates'

    # Converção dos formatos dos dados
    # df_state_cases$state_num_id = as.character(df_state_cases$state_num_id)
    df_state_cases$dates = as.Date(df_state_cases$dates)

    # Adicionando a coluna de população ao df
    df_state_cases = merge(x = df_state_cases, 
                        y = as.data.frame((df_farol %>% select(state_num_id, population))),
                        by = "state_num_id", all = TRUE)

    # Média móvel de casos ativos (7 dias)
    df_state_cases = df_state_cases %>% 
                    group_by(state_num_id) %>%
                    mutate(active_cases_mavg = runMean(active_cases, 7))

    # Incidencia: Infectados por 100k/hab. 
    df_state_cases$I = (100e3 * df_state_cases$active_cases_mavg)/df_state_cases$population

    # Filtra início da série
    df_state_cases = df_state_cases %>%
                        filter(!is.na(I)) %>%
                        filter(total_cases >= 15)

    # Gera a tabela dos estados
    rt_cori_serie = 0
    states = as.vector(unique(df_state_cases$state_num_id)) 

    for(st in states){
        rt = estimate_R(df_state_cases %>% filter(state_num_id == st) %>% select(dates, I), 
                        method="parametric_si",
                        config = make_config(list(
                            mean_si = 4.7,
                            std_si = 2.9,
                            mean_prior=3))
                    )

        rtx = bind_cols(state_num_id=st,rt$R)

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