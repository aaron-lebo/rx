library(scales)
library(tidytext)
library(tidyverse)

true <- TRUE

read_csvs <- function(dir, read=read_csv) {
  list.files(dir, 'csv$', full.names=true) %>% map_dfr(read)
}

read_match_csv <- function(file) {
  read_csv(file, col_types=cols_only(
    id=col_factor()))#, 
    #matches=col_character(),
    #combos=col_factor()))
}

bots <- c('autotldr', 'AutoModerator', 'PoliticsModeratorBot', 'TrumpTrain-bot')

read_f <- function(file) {
  read_csv(file, col_types=cols_only(
    dom=col_factor(), 
    author=col_factor(),
    subreddit=col_factor())) %>%
    rename(usr=author, sbr=subreddit) %>%
    filter(!usr %in% bots)
}

doms <- function(dir='doms') {
  list.files(dir, 'csv$', full.names=true) %>%
    map_dfr(read_f) %>%
    mutate(cat=factor(case_when(
      sbr %in% prog_subs ~ 'p', 
      sbr %in% cons_subs ~ 'c',
      TRUE ~ 'x')))
}

prog_subs <- c('politics', 'hillaryclinton', 'democrats', 'SandersForPresident', 'WayOfTheBern')
cons_subs <- c('Conservative', 'Republican', 'The_Donald')

tf_idf <- function(df) {
  d1 <- count(df, dom, cat, sort=1)
  d2 <- group_by(d1, dom) %>% summarize(total=sum(n))
  left_join(d1, d2) %>% bind_tf_idf(dom, cat, n) %>% filter(cat!='x')
}

csv <- function(df, ct, f) {
  filter(df, cat==ct) %>% arrange(desc(tf_idf), desc(tf)) %>% write_csv(f)
}

plot <- function(df) {
  for(sb in unique(df$sbr)) {
    d1 <- filter(df, sbr==sb) %>% top_n(50, tf_idf) %>% mutate(dom=reorder(dom, tf_idf))
    png(sprintf('%s.png', sb))
    print(ggplot(d1, aes(dom, tf_idf, fill=sbr)) +
      geom_col(show.legend=0) +
      coord_flip())
    dev.off()
  }
}

plot <- function(subr='submissions') {
  df <- read_csv('~/subm.csv')
  d1 <- read_csv('~/subm_matches.csv')
  d2 <- read_csv('~/subm_relatives.csv')
  if (subr != 'submissions') {
    df <- filter(df, subreddit==subr)
    d1 <- filter(d1, subreddit==subr)
    d2 <- filter(d2, subreddit==subr, !id %in% d1$id)
  }
  df <- group_by(df, file) %>% summarize(n=sum(n))
  d2 <- distinct(d2, id, .keep_all=1)
  d3 <- left_join(count(d1, file), count(d2, file), by='file') %>%
    mutate(n.y=replace(n.y, is.na(n.y), 0)) %>% gather(key, n, n.x, n.y)
  g1 <- ggplot(df, aes(x=file, y=n, group=1)) + geom_line() + geom_point() + 
    theme(axis.text.x=element_text(angle=90)) + scale_y_continuous(label=comma) + labs(title=subr, x=NULL)
  g2 <- ggplot(d3, aes(x=file, y=n, color=key, group=key)) + geom_line() + geom_point() +
    theme(axis.text.x=element_text(angle=90)) + scale_y_continuous(label=comma) + guides(color='none')
  grid::grid.newpage()
  png(sprintf('%s.png', subr))
  grid::grid.draw(rbind(ggplotGrob(g1), ggplotGrob(g2)))
  dev.off()
}

plots <- function() {
  plot()
  for(sb in prog_subs) {plot(sb)}
  for(sb in cons_subs) {plot(sb)}
}

