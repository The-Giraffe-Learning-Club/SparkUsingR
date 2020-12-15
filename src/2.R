---
title: "Data Transformation"
author: "Keshav Thakur"
---


# Section One ---------------------------------Plyr

library(Lahman)
library(plyr)

games <- ddply(Batting, "playerID", summarise, total = sum(G))
head(arrange(games, desc(total)), 5)


# Section two ---------------------------------Dplyr
#dplyr took 0.2s, a 35x speed-up

library(Lahman)
library(dplyr)

players <- group_by(Batting, playerID)
games <- summarise(players, total = sum(G))
head(arrange(games, desc(total)), 5)


### Chaining Transformation from Left --> Right
# 
# Batting %.%
#   group_by(playerID) %.%
#   summarise(total = sum(G)) %.%
#   arrange(desc(total)) %.%
#   head(5)


## Section Three ---------------------------------Sparklyr

library(sparklyr)
library(dplyr)
spark_install(version = "2.1.0")
sc <- spark_connect(master = "local")
install.packages(c("nycflights13", "Lahman"))
flights_tbl <- copy_to(sc, nycflights13::flights, "flights")
delay <- flights_tbl %>%
  group_by(tailnum) %>%
  summarise(count = n(), dist = mean(distance), delay = mean(arr_delay)) %>%
  filter(count > 20, dist < 2000, !is.na(delay)) %>%
  collect

library(ggplot2)
ggplot(delay, aes(dist, delay)) +
  geom_point(aes(size = count), alpha = 1/2) +
  geom_smooth() +
  scale_size_area(max_size = 2)

## Sector Four -------------------------Sparklyr 2nd Example

library(sparklyr)
library(dplyr)
spark_install(version = "2.1.0")
sc <- spark_connect(master = "local")
install.packages(c("nycflights13", "Lahman"))
batting_tbl <- copy_to(sc, Lahman::Batting, "batting")
batting_tbl %>%
  select(playerID, yearID, teamID, G, AB:H) %>%
  arrange(playerID, yearID, teamID) %>%
  group_by(playerID) %>%
  filter(min_rank(desc(H)) <= 2 & H > 0)



