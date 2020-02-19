library(tidyverse)
library(data.table)
library(foreign) #pour lire des tables DBF, package de base à charger
library(fst)


  #### Importer les données et les grouper ####

dbf <- list.files(path = "./rawdata", pattern = "\\.dbf", full.names = TRUE) %>%
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
  mutate(REGION = case_when(is.na(REGION) == FALSE ~ REGION,
                            is.na(REGION) == TRUE ~ reg),
         DEP = case_when(is.na(DEP) == FALSE ~ DEP,
                         is.na(DEP) == TRUE ~ dep),
         COM = case_when(is.na(COM) == FALSE ~ COM,
                         is.na(COM) == TRUE ~ com),
         APE = case_when(is.na(APE) == FALSE ~ APE,
                         is.na(APE) == TRUE ~ ape),
         TAILLE = case_when(is.na(TAILLE) == FALSE ~ TAILLE,
                            is.na(TAILLE) == TRUE ~ taille),
         FREQ = case_when(is.na(FREQ) == FALSE ~ as.integer(FREQ),
                          is.na(FREQ) == TRUE ~ freq),
         ARTISAN = case_when(is.na(ARTISAN) == FALSE ~ ARTISAN,
                             is.na(ARTISAN) == TRUE ~ artisan))

#Vérifier qu'il n'y a plus de champ REGION vide
etabtest <- etab2_ %>%
  filter(is.na(REGION) == TRUE)
rm(etabtest)

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
etab_all <- etab_12 %>%
  mutate(DEP = as.character(DEP),
         TAILLE = as.character(TAILLE),
         ARTISAN = as.character(ARTISAN))



  #### Formater pour l'analyse ####

#Formater les numéros de département et région
etab_all <- etab_all %>%
  mutate(DEP = str_pad(DEP, width = 2, pad = 0),
         REGION = str_pad(REGION, width = 2, pad = 0),
         COM = str_pad(COM, width = 5, pad = 0),
         ANNEE = substring(fichier, 11),
         TAILLE = str_pad(TAILLE, width = 2, pad = 0))


#Attention, les régions ont changé en 2015. Donc il faut une correspondance

corres_region <- fread("anciennes-nouvelles-regions.csv", encoding = "UTF-8") %>%
  select(ancien_code = `Anciens Code`, new_code = `Nouveau Code`, nom = `Nouveau Nom`) %>%
  mutate(ancien_code = str_pad(ancien_code, width = 2, pad = 0),
         new_code = str_pad(new_code, width = 2, pad = 0)) %>%
  distinct() %>%
  arrange(ancien_code)

etab_allok <- etab_all %>%
  #associer les anciens codes région aux nouveaux d'après la col "ancien_code"
  left_join(corres_region, by = c("REGION" = "ancien_code")) %>%
  #répercuter les codes inchangés dans la colonne "new_code"
  mutate(new_code = case_when(is.na(new_code) == FALSE ~ new_code,
                              is.na(new_code) == TRUE ~ REGION),
         REGION_OK = new_code,
         NOM_REGION = nom) %>% #modifier les noms
  select(-nom, - new_code) #supprimer les colonnes inutiles après le chgt de nom

#Correspondance des noms de nouvelles région

noms_regions <- tribble(
  ~code, ~nom,
  "84", "Auvergne-Rhône-Alpes",
  "27", "Bourgogne-Franche-Comté",
  "53", "Bretagne",
  "24", "Centre-Val de Loire",
  "94", "Corse",
  "44", "Grand Est",
  "01", "Guadeloupe",
  "03", "Guyane",
  "32", "Hauts-de-France",
  "11", "Ile-de-France",
  "04", "La Réunion",
  "02", "Martinique",
  "06", "Mayotte",
  "28", "Normandie",
  "75", "Nouvelle-Aquitaine",
  "76", "Occitanie",
  "52", "Pays de la Loire",
  "93", "Provence-Alpes-Côte d'Azur"
)

etab_allok_nom <- etab_allok %>%
  left_join(noms_regions, by = (c("REGION_OK" = "code")))

etab_allok <- etab_allok_nom %>%
  select(everything(), -NOM_REG) %>%
  mutate(NOM_REGION = nom) %>%
  select(-nom)



  #### Export fichier général ####

#Exporter csv
fwrite(etab_allok, file = "data/etablissements20142018.csv", sep = ";", col.names = TRUE)
#Exporter fst
write.fst(etab_allok, "data/etab20142018.fst")
#etab_allok <- read.fst("data/etab20142018.fst")


#Filtrer les brasseries
brasseries <- etab_allok %>% 
  filter(APE == "1105Z")

brasseries_bzh <- brasseries %>%
  filter(REGION_OK == "53")


#Exporter les brasseries
fwrite(brasseries, file = "data/brasseries_20142018.csv", sep = ";", col.names = TRUE)
fwrite(brasseries_bzh, file = "data/brasseries_bzh.csv", sep = ";", col.names = TRUE)

#Exporter certaines variables
save(brasseries, file = "data/brasseries_solo.RDATA")
#ou
write.fst(brasseries, "data/brasseries20142018.fst")
#brasseries <- read.fst("data/brasseries20142018.fst")



  #### Evolution France ####

#Evolution annuelle France entière
brasseries_annee  <-  brasseries %>%
  group_by(ANNEE) %>%
  summarise(NOMBRE = sum(FREQ))

#Graph évolution annuelle France entière
ggplot(brasseries_annee, aes(ANNEE, NOMBRE, fill = "#E30613", label = NOMBRE)) +
  geom_col() +
  geom_text(nudge_y = 20) +
  labs(title = "Le nombre de brasseries en France") +
  theme_light() +
  theme(panel.grid.major.x = element_blank(),
        panel.grid.minor.x = element_blank(),
        panel.grid.minor.y = element_blank(),
        panel.border =  element_blank())
ggsave(file = "figures/brasseries_france_annee.png")


ggplot(brasseries_annee, aes(ANNEE, NOMBRE)) +
  #coord_flip() +
  geom_point(color = "orange", size = 6, alpha = 0.8) +
  geom_segment(aes(x = ANNEE, xend = ANNEE, y = 0, yend = NOMBRE), 
               size = 1, 
               color = "grey",
               alpha = 0.8) +
  #facet_grid(. ~ ANNEE) +
  labs(title = "Le nombre de brasseries en France",
       x = element_blank(),
       y = element_blank()) +
  theme_minimal() +
  theme(
    panel.grid.major.x = element_blank(),
    panel.grid.minor.y = element_blank(),
    panel.border = element_blank(),
    axis.ticks.x = element_blank())



  #### Evolution régions ####


#Evolution annuelle par région
brasseries_region <- brasseries %>%
  group_by(ANNEE, REGION_OK, NOM_REGION) %>%
  summarize(NOMBRE_REG = sum(FREQ))

#Graph évolution annuelle par région
ggplot(brasseries_region, aes(ANNEE, NOMBRE_REG, fill = "#E30613", label = NOMBRE_REG)) +
  geom_col() +
  facet_wrap(~NOM_REGION) +
  labs(title = "Le nombre de brasseries par région entre 2013 et 2018",
       x = element_blank(), 
       y = element_blank()) +
  theme_light() +
  theme(panel.grid.major.x = element_blank(),
        panel.grid.minor.x = element_blank(),
        panel.grid.minor.y = element_blank(),
        panel.border =  element_blank(),
        legend.position="none") +
  scale_x_discrete(breaks = c("2013", "2017"))
ggsave(file = "figures/brasseries_evol_region.png")



#Pour export format facilité Excel
brasseries_region2 <- brasseries_region %>%
  pivot_wider(id_cols = REGION_OK, values_from = NOMBRE_REG, names_from = ANNEE)
fwrite(brasseries_region2, file = "data/brasseries_region_annee.csv", sep = ";", row.names = F)


  #### Focus Bretagne évolution ####

#Bretagne évolution annuelle
brasseries_annee_bzh  <-  brasseries %>%
  filter(REGION_OK == "53") %>%
  group_by(ANNEE) %>%
  summarise(NOMBRE = sum(FREQ))


brasseries_annee_44  <-  brasseries %>%
  filter(DEP == "44") %>%
  group_by(ANNEE) %>%
  summarise(NOMBRE = sum(FREQ))


#Brasseries Bretagne à 5 évolution annuelle
brasseries_annee_bzh5  <-  brasseries %>%
  filter(DEP %in% c('22', '29', '35', '44', '56')) %>%
  group_by(ANNEE) %>%
  summarise(NOMBRE = sum(FREQ))


#Graph Bretagne évolution annuelle
ggplot(brasseries_annee_bzh, aes(ANNEE, NOMBRE, fill = "#E30613", label = NOMBRE)) +
  geom_col() +
  geom_text(vjust = "top") +
  labs(title = "Le nombre de brasseries en Bretagne") +
  theme_light() +
  theme(panel.grid.major.x = element_blank(),
        panel.grid.minor.x = element_blank(),
        panel.grid.minor.y = element_blank(),
        panel.border =  element_blank())

ggsave(file = "figures/brasseries_bretagne_annee.png")


  #### Répartition par région ####


#Par région en 2018
brasseries_region_2018 <- brasseries %>%
  filter(ANNEE == "2017") %>%
  group_by(REGION_OK, NOM_REGION) %>%
  summarize(NOMBRE = sum(FREQ)) %>%
  arrange(REGION_OK)
  
ggplot(brasseries_region_2018, aes(NOM_REGION, NOMBRE, label = NOMBRE)) +
  geom_col() +
  geom_text(size = 3, hjust = "left", nudge_y = 2) +
  coord_flip() +
  labs(title = "Le nombre de brasseries par région en 2018") +
  theme_light() +
  theme(panel.grid.major.y = element_blank(), 
        panel.grid.minor.y = element_blank(),
        panel.grid.minor.x = element_blank(),
        panel.border =  element_blank())
ggsave(file = "figures/brasseries_par_région_2018.png")



#Connaître le nombre de villes comptant une brasserie
brasseries_region_2018_villes <- brasseries %>%
  filter(ANNEE == "2017") %>%
  group_by(REGION_OK, NOM_REGION) %>%
  summarize(NB_VILLES = n()) %>%
  arrange(desc(NB_VILLES))


#Par dep en 2018
brasseries_dep_2018 <- brasseries %>%
  filter(ANNEE == "2017") %>%
  group_by(DEP) %>%
  summarise(NOMBRE = sum(FREQ)) %>%
  arrange(desc(NOMBRE), .by_group = TRUE)


#Par dep en 2018 en Bretagne
brasseries_dep_2018_bzh <- brasseries %>%
  filter(ANNEE == "2017" & REGION == "53") %>%
  group_by(DEP) %>%
  summarise(NOMBRE = sum(FREQ)) %>%
  arrange(desc(NOMBRE), .by_group = TRUE)

#Comparer avec la population par département
popdep <- fread("rawdata/pop2020_dep.csv", skip = 7, encoding = "UTF-8") %>%
  select(codedep = `Code département`, pop2020 = `Population municipale`)
  unique()

brasseries_dep_2018_pop <- left_join(brasseries_dep_2018, popdep, by = c("DEP" = "codedep")) %>%
  mutate(pour_100000hab = round(NOMBRE / pop2020 * 100000, digits = 1)) %>%
  arrange(desc(pour_100000hab))
fwrite(brasseries_dep_2018_pop, file = "data/brasseries_dep_pop_2018.csv", sep = ";")


#Nombre de villes par département comptant une brasserie en 2018
brasseries_villes_par_dep_2018 <- brasseries %>%
  filter(ANNEE == "2017") %>%
  group_by(DEP) %>%
  summarise(NB_VILLES = n()) %>%
  arrange(desc(NB_VILLES))
fwrite(brasseries_villes_par_dep_2018, file = "data/brasseries_villespardep_2018.csv")


#Par commune
brasseries_communes_2018 <- brasseries %>%
  filter(ANNEE == "2017") %>%
  group_by(COM) %>%
  summarise(NOMBRE = sum(FREQ)) %>%
  arrange(desc(NOMBRE), .by_group = TRUE)



#### En Bretagne ####

brasseries_bzh_2018 <- brasseries %>%
  filter(ANNEE == "2017", REGION_OK == "53") %>%
  group_by(COM) %>%
  summarize(NB = sum(FREQ)) %>%
  arrange(desc(NB))



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

#Correspondance données brasseries et effectif correspondant selon le code TAILLE
taille <- left_join(brasseries, nomenclature_salaries, by = c("TAILLE"))

#Evolution annuelle selon la taille de l'effectif
evol_taille <- taille %>%
  filter(REGION == "53") %>%
  group_by(ANNEE, EFFECTIF) %>%
  summarize(NB = sum(FREQ))

#Graphique lollipop d'évolution annuelle selon la taille de l'effectif
ggplot(evol_taille, aes(EFFECTIF, NB)) +
  #coord_flip() +
  geom_point(color = "orange", size = 3) +
  geom_segment(aes(x = EFFECTIF, xend = EFFECTIF, y = 0, yend = NB), size = 1, color = "grey") +
  facet_grid(. ~ ANNEE) +
  theme_minimal() +
  theme(
    panel.grid.major.x = element_blank(),
    panel.border = element_blank(),
    axis.ticks.x = element_blank(),
    axis.text.x = element_text(angle = 90, hjust = 1, size = 7))

ggsave(file = "figures/brasseries_evol_taille.png")

#Répartition des établissements en 2018 selon l'effectif
part_taille_2018 <- taille %>%
  filter(ANNEE == "2017") %>%
  group_by(EFFECTIF) %>%
  summarize(NOMBRE = sum(FREQ)) %>%
  mutate(PART = round(NOMBRE / sum(NOMBRE) * 100, digits = 1)) %>%
  arrange(desc(PART))


#Evolution annuelle des établissements de Bretagne selon l'effectif
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


#Graph colonnes de l'évolution annuelle en Bretagne selon l'effectif
ggplot(taille_bzh_20132018, aes(EFFECTIF, NOMBRE)) +
  geom_col() +
  labs(title = "Les brasseries en Bretagne selon l'effectif",
       y = "Nombre",
       x = "Taille de l'effectif") +
  theme_minimal() +
  theme(panel.grid.major.x = element_blank()) +
  facet_wrap(~ANNEE) +
  coord_flip()

ggsave(file = "figures/BrasseriesBZHTaille_20132018.png")


#Répartition des établissements de Bretagne en 2018 selon l'effectif
taille_bzh <- left_join(brasseries, nomenclature_salaries, by = c("TAILLE")) %>%
  filter(ANNEE == "2017" & REGION_OK == "53") %>%
  group_by(EFFECTIF) %>%
  summarize(NOMBRE = sum(FREQ)) %>%
  mutate(PART = round(NOMBRE / sum(NOMBRE) * 100, digits = 1)) %>%
  arrange(match(EFFECTIF, c("0 salarié", 	"1 à 2 salariés", "3 à 5 salariés", "6 à 9 salariés", "10 à 19 salariés", "20 à 49 salariés")))




  #### Selon la population ####

#Comparer avec nombre d'habitants
#Nombre de villes par département (plutôt que de nombre de brasseries brut)

popregion <- tribble(
  ~code_reg, ~nom_reg, ~pop_reg_2020,
  "84",	"Auvergne-Rhône-Alpes",	7948287,
  "27",	"Bourgogne-Franche-Comté",	2811423,
  "53",	"Bretagne",	3318904,
  "24",	"Centre-Val de Loire",	2576252,
  "94",	"Corse",	334938,
  "44",	"Grand Est",	5549586,
  "01",	"Guadeloupe",	390253,
  "03",	"Guyane",	268700,
  "32",	"Hauts-de-France",	6003815,
  "11",	"Île-de-France",	12174880,
  "04",	"La Réunion",	853659,
  "02",	"Martinique",	372594,
  "28",	"Normandie",	3330478,
  "75",	"Nouvelle-Aquitaine",	5956978,
  "76",	"Occitanie",	5845102,
  "52",	"Pays de la Loire",	3757600,
  "93",	"Provence-Alpes-Côte d'Azur",	5030890
)

brasseries_pop <- brasseries_region_2018 %>%
  left_join(popregion, by = c("REGION_OK" = "code_reg")) %>%
  mutate(brasseries_100000hab = round(NOMBRE / pop_reg_2020 * 100000, digits = 2)) %>%
  arrange(desc(brasseries_100000hab))
fwrite(brasseries_pop, file = "data/brasseries_reg_pop_2018.csv")

