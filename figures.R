library(arrow)
library(lubridate)
library(scales)
library(tidytext)
library(tidyverse)

true <- TRUE

bots <- c('autotldr', 'AutoModerator', 'PoliticsModeratorBot', 'TrumpTrain-bot')

read_f <- function(file) {
  read_parquet(file, c('id', 'author', 'subreddit')) %>% filter(!author %in% bots)
}

doms <- function(dir) {
  list.files(dir, 'pq$', full.names=true) %>%
    map_dfr(read_f) %>%
    mutate(cat=factor(case_when(
      subreddit %in% prog_subs ~ 'p', 
      subreddit %in% cons_subs ~ 'c',
      TRUE ~ 'x')))
}

prog_subs <- c('politics', 'hillaryclinton', 'democrats', 'SandersForPresident', 'WayOfTheBern')
cons_subs <- c('Conservative', 'Republican', 'The_Donald')

tf_idf <- function(df) {
  d1 <- count(df, dom, cat, sort=1)
  d2 <- group_by(d1, dom) %>% summarize(total=sum(n))
  left_join(d1, d2) %>% bind_tf_idf(dom, cat, n) %>% filter(cat!='x')
}

data <- function(file, key) {
  df <- tibble(read_parquet(file)) 
  attr(df$created_utc, 'tzone') <- 'UTC'
  group_by(df, m=floor_date(created_utc, 'month')) %>% count(m, sort=true) %>% mutate(key, key)
}

plot <- function(file, file1) {
  df <- read_csv(file) %>%
    group_by(k, kind, subreddit) %>%
    #filter(subreddit %in% prog_subs) %>%
    filter(subreddit %in% cons_subs) %>%
    summarise(n=sum(n)) %>% 
    separate(k, c('_', 'y', 'm')) %>% 
    mutate(y=parse_number(y), m=parse_number(m)) %>% 
    mutate(month=make_datetime(y, m))
  gp <- ggplot(df, aes(month, n, color=kind)) + geom_line() + geom_point() +
    theme(axis.text.x=element_text(angle=90)) +
    scale_x_datetime(breaks='2 month', date_labels='%b %Y') + scale_y_continuous(label=comma)  +
    scale_y_continuous(label=comma)
  print(gp+facet_wrap(~subreddit, ncol=1, scales='free_y'))
  ggsave(file1)
}
