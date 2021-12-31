library(grid)
library(lubridate)
library(scales)
library(tidyverse)

plot_ <- function(df, what1, text) {
  ggplot(filter(df, what==what1), aes(date, X2)) +
    geom_line() +
    theme(axis.title.x=element_blank(), axis.text.x=text) +
    scale_x_date(breaks='3 month', date_labels='%b %Y') +
    scale_y_continuous(labels=comma)
}

plot <- function(df) {
  c <- plot_(df, 'RC', element_blank())
  s <- plot_(df, 'RS', element_text(angle=60, hjust=1))
  grid.newpage()
  grid.draw(rbind(ggplotGrob(c), ggplotGrob(s), size='last'))
}

plot2 <- function(kind) {
  read_csv(kind) %>%
  gather(key, val, matches, relatives) %>%
  ggplot(aes(x=date, y=val, colour=key)) %>%
  plot(kind)
  ggsave(sprintf('%s_split.png', kind))
}

doit <- function(file, date1, date2) {
  read_csv(file, col_names=0) %>% 
    mutate(what=substring(X1, 0, 2)) %>% 
    mutate(date=ym(substring(X1, 4))) %>% 
    filter(between(date, as.Date(date1), as.Date(date2)))
}
