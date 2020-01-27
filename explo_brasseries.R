library(tidyverse)
library(data.table)
library(foreign) #pour lire des tables DBF, package de base à charger
library(fst)

setwd("~/DATA/brasseries")

  #### Importer les données et les grouper ####
dbf <- list.files(pattern = "\\.dbf") %>%
  setNames(., sub("\\.dbf$", "", basename(.)))
tous <- lapply(dbf, read.dbf)

  #Grouper les dbf
etab <- bind_rows(tous, .id = "fichier")

#Trop de variables : il y a trois types de fichiers différents, à séparer et unifier
etab1 <- etab %>%
  filter(is.na(REGION) == FALSE)

  #Premier filtre
etab_correc <- etab %>%
  filter(is.na(REGION) == TRUE) %>%
  mutate(REGION = REG)

  #Deuxième filtre : corriger les champs manquants avec les structures différentes
etab2_ <- etab_correc %>%
  mutate(REGION = case_when(is.na(REGION) == TRUE ~ reg,
                            is.na(REGION) == FALSE ~ REGION),
         DEP = case_when(is.na(DEP) == TRUE ~ dep,
                         is.na(DEP) == FALSE ~ DEP),
         COM = case_when(is.na(COM) == TRUE ~ com,
                         is.na(COM) == FALSE ~ COM),
         APE = case_when(is.na(APE) == TRUE ~ ape,
                         is.na(APE) == FALSE ~ APE),
         TAILLE = case_when(is.na(TAILLE) == TRUE ~ taille,
                            is.na(TAILLE) == FALSE ~ TAILLE),
         FREQ = case_when(is.na(FREQ) == TRUE ~ freq,
                          is.na(FREQ) == FALSE ~ as.integer(FREQ)),
         ARTISAN = case_when(is.na(ARTISAN) == TRUE ~ artisan,
                             is.na(ARTISAN) == FALSE ~ ARTISAN))

#Vérifier qu'il n'y a plus de champ REGION vide
etabtest <- etab2_ %>%
  filter(is.na(REGION) == TRUE)

#Sélectionner les bonnes colonnes avec les bons noms
etab1 <- etab1 %>%
  select(fichier, REGION, DEP, COM, APE, TAILLE, ARTISAN, FREQ)
etab2 <- etab2_ %>%
  select(fichier, REGION, DEP, COM, APE, TAILLE, ARTISAN, FREQ)

nrow(etab1) + nrow(etab2)
#15210785 observations au total = correspond au fichier groupé de base

#Grouper les deux fichiers nettoyés
etab_12 <- bind_rows(etab1, etab2)

#Corriger le problème de type de colonnes pour bind_rows()
etab_12 <- etab_12 %>%
  mutate(DEP = as.character(DEP),
         TAILLE = as.character(TAILLE),
         ARTISAN = as.character(ARTISAN))

  #Importer le csv isolé
dbf2018 <- fread("ets_2018.csv")
# dbf2018 %>%
#   select(REGION = REG, everything())
names(dbf2018)[names(dbf2018) == "REG"] <- "REGION"

#Créer une colonne id correspondant à celui de l'autre fichier groupé
dbf2018$fichier <- "ets2018"

#Placer la colonne ID au début pour + de clarté
dbf2018 <- dbf2018[, c(17, 1:16)]

#Nettoyer le csv isolé
dbf2018 <- dbf2018 %>%
  select(fichier, REGION, DEP, COM, APE, TAILLE, ARTISAN, FREQ) %>%
  mutate(REGION = as.character(REGION), 
         TAILLE = as.character(TAILLE),
         TAILLE = str_pad(TAILLE, width = 2, pad = 0))

#Grouper l'ensemble dbf/csv
etab_all <- bind_rows(etab_12, dbf2018) %>% 
  arrange(fichier)

  #### Formater pour l'analyse ####

#Formater les numéros de département et région
etab_all <- etab_all %>%
  mutate(DEP = str_pad(DEP, width = 2, pad = 0),
         REGION = str_pad(REGION, width = 2, pad = 0),
         COM = str_pad(COM, width = 5, pad = 0),
         ANNEE = substring(fichier, 4))


#Attention, les régions ont changé en 2015. Donc il faut une correspondance
corres_region <- fread("anciennes-nouvelles-regions.csv", encoding = "UTF-8")
corres_region <- corres_region %>%
  select(ancien_code = `Anciens Code`, new_code = `Nouveau Code`, nom = `Nouveau Nom`) %>%
  mutate(ancien_code = str_pad(ancien_code, width = 2, pad = 0))
etab_all <- etab_all %>%
  left_join(corres_region, by = c("REGION" = "ancien_code"))

etab_all_reg <- etab_all %>%
  mutate(new_code = case_when(is.na(new_code) == FALSE ~ str_pad(new_code, width = 2, pad = 0),
                              is.na(new_code) == TRUE ~ REGION))

etab_allok <- etab_all_reg %>%
  mutate(REGION_OK = new_code,
         NOM_REGION = nom) %>%
  select(-new_code)

#Exporter
fwrite(etab_allok, file = "etablissements20132018.csv", sep = ";", col.names = TRUE)


#Filtrer les brasseries
brasseries <- etab_allok %>% 
  filter(APE == "1105Z")

brasseries_bzh <- brasseries %>%
  filter(REGION_OK == "53")

#Exporter les brasseries
fwrite(brasseries, file = "brasseries_20132018.csv", sep = ";", col.names = TRUE)
fwrite(brasseries_bzh, file = "brasseries_bzh.csv", sep = ";", col.names = TRUE)

#Exporter certaines variables
save(brasseries, file = "brasseries_solo.RDATA")
  #ou


  #### Evolution brasseries ####

#Par année
brasseries_annee  <-  brasseries %>%
  group_by(ANNEE) %>%
  summarise(NOMBRE = sum(FREQ))

#Graph France
ggplot(brasseries_annee, aes(ANNEE, NOMBRE, fill = "#E30613", label = NOMBRE)) +
  geom_col() +
  geom_text(nudge_y = 20) +
  labs(title = "Le nombre de brasseries en France") +
  theme_light() +
  theme(panel.grid.major.x = element_blank(),
        panel.grid.minor.x = element_blank(),
        panel.grid.minor.y = element_blank(),
        panel.border =  element_blank())
ggsave(file = "brasseries_france_annee.png")

#Graph Bretagne
brasseries_annee_bzh  <-  brasseries %>%
  filter(REGION_OK == "53") %>%
  group_by(ANNEE) %>%
  summarise(NOMBRE = sum(FREQ))

ggplot(brasseries_annee_bzh, aes(ANNEE, NOMBRE, fill = "#E30613", label = NOMBRE)) +
  geom_col() +
  geom_text(vjust = "top") +
  labs(title = "Le nombre de brasseries en Bretagne") +
  theme_light() +
  theme(panel.grid.major.x = element_blank(),
        panel.grid.minor.x = element_blank(),
        panel.grid.minor.y = element_blank(),
        panel.border =  element_blank())
ggsave(file = "brasseries_bretagne_annee.png")

#Graph ensemble

brasseries_region <- brasseries %>%
  group_by(ANNEE, REGION_OK) %>%
  summarize(NOMBRE_REG = sum(FREQ))

brasseries_region2 <- brasseries_region %>%
  pivot_wider(id_cols = REGION_OK, values_from = NOMBRE_REG, names_from = ANNEE)

fwrite(brasseries_region2, file = "brasseries_region_annee.csv", sep = ";", row.names = F)
  
ggplot(brasseries_region, aes(ANNEE, NOMBRE_REG, fill = "#E30613", label = "NOMBRE")) +
  geom_line()

#Par région
brasseries_region_2018 <- brasseries %>%
  filter(ANNEE == "2018") %>%
  group_by(REGION) %>%
  summarize(NOMBRE = sum(FREQ)) %>%
  arrange(REGION)
  
ggplot(brasseries_region, aes(REGION, NOMBRE, label = NOMBRE)) +
  geom_col() +
  geom_text(size = 3, hjust = "left", nudge_y = 2) +
  coord_flip() +
  labs(title = "Le nombre de brasseries par région en 2018") +
  theme_light() +
  theme(panel.grid.major.y = element_blank(), 
        panel.grid.minor.y = element_blank(),
        panel.grid.minor.x = element_blank(),
        panel.border =  element_blank())
ggsave(file = "brasseries_par_région_2018.png")

#Par dep
brasseries_dep_2018 <- brasseries %>%
  filter(ANNEE == "2018") %>%
  group_by(DEP) %>%
  summarise(NOMBRE = sum(FREQ)) %>%
  arrange(desc(NOMBRE), .by_group = TRUE)

brasseries_dep_2018_bzh <- brasseries %>%
  filter(ANNEE == "2018" & REGION == "53") %>%
  group_by(DEP) %>%
  summarise(NOMBRE = sum(FREQ)) %>%
  arrange(desc(NOMBRE), .by_group = TRUE)

#Par commune
brasseries_communes_2018 <- brasseries %>%
  filter(ANNEE == "2018") %>%
  group_by(COM) %>%
  summarise(NOMBRE = sum(FREQ)) %>%
  arrange(desc(NOMBRE), .by_group = TRUE)

#Comparer avec nombre d'habitants
#Nombre de villes par département (plutôt que de nombre de brasseries brut)

  #### Selon la taille de l'effectif ####
nomenclature_salaries <- tribble(
  ~TAILLE, ~EFFECTIF,
  "00", "0 salarié",
  "01", "1 à 2 salariés",
  "02", "3 à 5 salariés",
  "03", "6 à 9 salariés",
  "11", "10 à 19 salariés",
  "12", "20 à 49 salariés",
  "21", "50 à 99 salariés",
  "22", "100 à 199 salariés",
  "31", "200 à 249 salariés",
  "32", "250 à 499 salariés",
  "41", "500 à 999 salariés",
  "42", "1 000 à 1 999 salariés",
  "51", "2 000 à 4 999 salariés",
  "52", "5 000 à 9 999 salariés",
  "53", "10 000 salariés et plus")

taille <- left_join(brasseries, nomenclature_salaries, by = c("TAILLE")) %>%
  filter(ANNEE == "2018") %>%
  group_by(EFFECTIF) %>%
  summarize(NOMBRE = sum(FREQ)) %>%
  mutate(PART = round(NOMBRE / sum(NOMBRE) * 100, digits = 1)) %>%
  arrange(desc(PART))

taille_bzh <- left_join(brasseries, nomenclature_salaries, by = c("TAILLE")) %>%
  filter(ANNEE == "2018" & REGION_OK == "53") %>%
  group_by(EFFECTIF) %>%
  summarize(NOMBRE = sum(FREQ)) %>%
  mutate(PART = round(NOMBRE / sum(NOMBRE) * 100, digits = 1)) %>%
  arrange(match(EFFECTIF, c("0 salarié", 	"1 à 2 salariés", "3 à 5 salariés", "6 à 9 salariés", "10 à 19 salariés", "20 à 49 salariés")))

taille_bzh_20132018  <- left_join(brasseries, nomenclature_salaries, by = c("TAILLE")) %>%
  filter(REGION_OK == "53") %>%
  group_by(ANNEE, EFFECTIF) %>%
  summarize(NOMBRE = sum(FREQ)) %>%
  mutate(PART = round(NOMBRE / sum(NOMBRE) * 100, digits = 1)) %>%
  arrange(ANNEE, match(EFFECTIF, c("0 salarié", 	
                                   "1 à 2 salariés", 
                                   "3 à 5 salariés", 
                                   "6 à 9 salariés", 
                                   "10 à 19 salariés", 
                                   "20 à 49 salariés")), 
          .by_group = TRUE)

ggplot(taille_bzh_20132018, aes(EFFECTIF, NOMBRE)) +
  geom_col() +
  labs(title = "Les brasseries en Bretagne selon l'effectif",
       y = "Nombre",
       x = "Taille de l'effectif") +
  theme_minimal() +
  theme(panel.grid.major.x = element_blank()) +
  facet_wrap(~ANNEE) +
  coord_flip()

ggsave(file = "BrasseriesBZHTaille_20132018.png")


