library(arrow)
library(lubridate)
library(scales)
#library(stringr)
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

write_y <- function(df, file) {
  write_csv(group_by(df, k1, y) %>% summarise(year_n=mean(n)), file)
}

plotx1 <- function(file, pre, subs) {
  read_csv(file) %>%
    filter(str_detect(k, pre)) %>%
    group_by(k, subreddit) %>%
    filter(subreddit %in% subs) %>%
    summarise(n=sum(n)) %>% 
    separate(k, c('k1', 'y', 'm')) %>% 
    mutate(y=parse_number(y), m=parse_number(m)) %>% 
    mutate(month=make_datetime(y, m))
}

plotx <- function(file, subs, out_csv, out_png) {
  df <- left_join(plotx1(file, '^rs', subs), plotx1(file, '^rc', subs), by=c('month', 'subreddit')) %>%
    mutate(n=n.x+n.y)
  write_csv(mutate(df, change=(n-lag(n))/lag(n)*100), out_csv)
  #write_y(df, str_c('y_', out_csv))

  gp <- ggplot(df, aes(month, n)) + geom_line() + geom_point() +
    theme(axis.text.x=element_text(angle=90)) +
    scale_x_datetime(breaks='2 month', date_labels='%b %Y') +
    scale_y_continuous(labels=label_number(scale_cut=cut_short_scale())) + 
    labs(x=NULL, y=NULL)
  print(gp+facet_wrap(~subreddit, ncol=1, scales='free_y'))
  ggsave(out_png)
}

xplot1 <- function(file, pre) {
  read_csv(file) %>%
    filter(str_detect(k, pre)) %>%
    group_by(k) %>%
    summarise(n=sum(n)) %>% 
    separate(k, c('k1', 'y', 'm')) %>% 
    mutate(y=parse_number(y), m=parse_number(m)) %>% 
    mutate(month=make_datetime(y, m))
}

xplot <- function(file) {
  df <- xplot1(file, '^rs')
  d1 <- xplot1(file, '^rc')
  df <- bind_rows(list(mutate(df, kind='submissions'), mutate(d1, kind='comments')))
  write_csv(mutate(df, change=(n-lag(n))/lag(n)*100), 'things.csv')
  write_y(df, 'y_things.csv')

  gp <- ggplot(df, aes(month, n)) + geom_line() + geom_point() +
    theme(axis.text.x=element_text(angle=90)) +
    scale_x_datetime(breaks='2 month', date_labels='%b %Y') +
    scale_y_continuous(labels=label_number(scale_cut=cut_short_scale())) +
    labs(x=NULL, y=NULL)
  print(gp+facet_wrap(~kind, ncol=1, scales='free_y'))
  ggsave('things.png')
}

ploty <- function(file, file_rel, subs, out_csv, out_png) {
  df <- read_csv(file) 
  df$kind = 'match'
  d1 <- read_csv(file_rel) 
  d1$kind = 'relative'
  df <- bind_rows(list(df, d1)) %>%
    group_by(k, kind, subreddit) %>%
    filter(subreddit %in% subs) %>%
    summarise(n=sum(n)) %>% 
    separate(k, c('_', 'y', 'm')) %>% 
    mutate(y=parse_number(y), m=parse_number(m)) %>% 
    mutate(month=make_datetime(y, m))
  write_csv(mutate(df, change=(n-lag(n))/lag(n)*100), out_csv)

  gp <- ggplot(df, aes(month, n, color=kind)) + geom_line() + geom_point() +
    theme(axis.text.x=element_text(angle=90)) +
    scale_x_datetime(breaks='2 month', date_labels='%b %Y') +
    scale_y_continuous(labels=label_number(scale_cut=cut_short_scale())) +
    labs(x=NULL, y=NULL)
  print(gp+facet_wrap(~subreddit, ncol=1, scales='free_y'))
  ggsave(out_png)
}

yplot1 <- function(file, file_rel) {
  df <- read_csv(file) 
  df$kind = 'match'
  d1 <- read_csv(file_rel) 
  d1$kind = 'relative'
  bind_rows(list(df, d1)) %>%
    group_by(k, kind) %>%
    summarise(n=sum(n)) %>% 
    separate(k, c('_', 'y', 'm')) %>% 
    mutate(y=parse_number(y), m=parse_number(m)) %>% 
    mutate(month=make_datetime(y, m))
}

yplot <- function() {
  df <- yplot1('rs_cnts.csv', 'rs_rel_cnts.csv') 
  d1 <- yplot1('rc_cnts.csv', 'rc_rel_cnts.csv') 
  df <- bind_rows(list(mutate(df, kind1='submissions'), mutate(d1, kind1='comments')))
  write_csv(mutate(df, change=(n-lag(n))/lag(n)*100), 'things1.csv')

  gp <- ggplot(df, aes(month, n, color=kind)) + geom_line() + geom_point() +
    theme(axis.text.x=element_text(angle=90)) +
    scale_x_datetime(breaks='2 month', date_labels='%b %Y') +
    scale_y_continuous(labels=label_number(scale_cut=cut_short_scale())) +
    labs(x=NULL, y=NULL)
  print(gp+facet_wrap(~kind1, ncol=1, scales='free_y'))
  ggsave('things1.png')
}
