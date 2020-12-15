install.packages("nycflights13")

library(nycflights13)

sc <- spark_connect(master="local")

fly_ref <- copy_to(sc, flights, 'flights_tbl')

ny_dc <- filter(fly_ref, origin == "JFK", dest == "DCA")

options(tibble.width = Inf)
show_query(ny_dc)

keep_filter <- copy_to(sc, filter(fly_ref, origin == "JFK", dest == "DCA"), "jfk_dca_flights")


######## Sort the flights into chronological order by month and day

fly_arr <- arrange(fly_ref, month, day)

show_query(fly_arr)

######## 