# Load libaries 
library(tidyverse) 
library(dplyr)
library(readxl)
library(ggplot2)
library(openxlsx)
library(sf)

# Set seed
set.seed(123)

# Get Windows username
win_username <- Sys.getenv("USERNAME")

# Load in data
file_name = "Cluster_Maps_Data_20240221"
glob_df <- read_excel(paste0("C:/Users/",win_username,"/Office for National Statistics/Strategy, Coordination and Dissemination - Documents/Sharepoint_data_loading/Clustering/Outputs/Clustering_model_outputs/",file_name,".xlsx"), 
                      sheet = "Global") 

# Load in shapefile
shp <- st_read("D:/IDS/Subnational Statistics/Data/clustering_local/LAD_MAY_2023_UK_BFC_V2.shp")

# Filter on Local Authority Code & Cluster
#  Set cluster to factor
df_c <- glob_df %>%
  select(AREACD, Cluster) %>%
  rename("LAD23CD" = "AREACD") %>%
  mutate(Cluster = case_when(Cluster == "0" ~ "Glob_A",
                             Cluster == "1" ~ "Glob_B",
                             Cluster == "2" ~ "Glob_C",
                             Cluster == "3" ~ "Glob_D"),
         Cluster = as.factor(Cluster))

# Join clusters to shapefile
shp2 <- shp %>%
  left_join(df_c, by = c("LAD23CD")) %>%
  filter(!is.na(Cluster)) 

# Produce map
glob_map <- ggplot(shp2) +
  geom_sf(aes(fill = Cluster), size = 0.01) +
  theme(axis.text.x = element_blank(),        # Remove X axis
        axis.text.y = element_blank(),        # Remove Y axis
        axis.ticks = element_blank(),         # Remove ticks
        rect = element_blank()) +
  scale_fill_manual(values = c("Glob_A" = "#3399CC", "Glob_B" = "#FF9933", 
                               "Glob_C" = "#66CC33", "Glob_D" = "#FF3300"))

# Show map
glob_map


# Open Excel Workbook
wb <- loadWorkbook(file = "C:/Users/kellyj2/Office for National Statistics/Strategy, Coordination and Dissemination - Documents/Sharepoint_data_loading/Clustering/Outputs/Clustering_model_outputs/Cluster_Maps_20240221.xlsx")

# Add worksheet to excel
addWorksheet(wb, "Global_model")

# Add Plot
print(glob_map)
insertPlot(wb, "Global_model", xy = c("B", 2), width = 5, height = 7, fileType = "png", units = "in")


################

# Load in data
file_name = "Cluster_Maps_Data_20240221"
con_df <- read_excel(paste0("C:/Users/",win_username,"/Office for National Statistics/Strategy, Coordination and Dissemination - Documents/Sharepoint_data_loading/Clustering/Outputs/Clustering_model_outputs/",file_name,".xlsx"), 
                      sheet = "Connectivity") 

# Filter on Local Authority Code & Cluster
#  Set cluster to factor
df_c <- con_df %>%
  select(AREACD, Cluster) %>%
  rename("LAD23CD" = "AREACD") %>%
   mutate(Cluster = case_when(Cluster == "0" ~ "Con_A",
                              Cluster == "1" ~ "Con_B",
                              Cluster == "2" ~ "Con_C",
                              Cluster == "3" ~ "Con_D"),
          Cluster = as.factor(Cluster))

# Join clusters to shapefile
shp2 <- shp %>%
  left_join(df_c, by = c("LAD23CD")) %>%
  filter(!is.na(Cluster))

# Produce map
con_map <- ggplot(shp2) +
  geom_sf(aes(fill = Cluster), size = 0.01) +
  theme(axis.text.x = element_blank(),
        axis.text.y = element_blank(),
        axis.ticks = element_blank(),
        rect = element_blank()) +
  scale_fill_manual(values = c("Con_A" = "#3399CC", "Con_B" = "#FF9933", 
                               "Con_C" = "#66CC33", "Con_D" = "#FF3300"))

con_map

# Add worksheet to excel
addWorksheet(wb, "Connect_model")

# Add Plot
print(con_map)
insertPlot(wb, "Connect_model", xy = c("B", 2), width = 5, height = 7, fileType = "png", units = "in")


########################

# Load in data
file_name = "Cluster_Maps_Data_20240221"
econ_df <- read_excel(paste0("C:/Users/",win_username,"/Office for National Statistics/Strategy, Coordination and Dissemination - Documents/Sharepoint_data_loading/Clustering/Outputs/Clustering_model_outputs/",file_name,".xlsx"), 
                     sheet = "Economic") 

# Filter on Local Authority Code & Cluster
#  Set cluster to factor
df_c <- econ_df %>%
  select(AREACD, Cluster) %>%
  rename("LAD23CD" = "AREACD") %>%
   mutate(Cluster = case_when(Cluster == "0" ~ "Econ_A",
                              Cluster == "1" ~ "Econ_B",
                              Cluster == "2" ~ "Econ_C",
                              Cluster == "3" ~ "Econ_D"),
          Cluster = as.factor(Cluster))

# Join clusters to shapefile
shp2 <- shp %>%
  left_join(df_c, by = c("LAD23CD")) %>%
  filter(!is.na(Cluster))

# Produce map
econ_map <- ggplot(shp2) +
  geom_sf(aes(fill = Cluster), size = 0.01) +
  theme(axis.text.x = element_blank(),
        axis.text.y = element_blank(),
        axis.ticks = element_blank(),
        rect = element_blank()) +
   scale_fill_manual(values = c("Econ_A" = "#3399CC", "Econ_B" = "#FF9933", 
                                "Econ_C" = "#66CC33", "Econ_D" = "#FF3300"))

# Add worksheet to excel
addWorksheet(wb, "Economic_model")

# Add Plot
print(econ_map)
insertPlot(wb, "Economic_model", xy = c("B", 2), width = 5, height = 7, fileType = "png", units = "in")



########################

# Load in data
file_name = "Cluster_Maps_Data_20240221"
demo_df <- read_excel(paste0("C:/Users/",win_username,"/Office for National Statistics/Strategy, Coordination and Dissemination - Documents/Sharepoint_data_loading/Clustering/Outputs/Clustering_model_outputs/",file_name,".xlsx"), 
                      sheet = "Demographic") 

# Filter on Local Authority Code & Cluster
#  Set cluster to factor
df_c <- demo_df %>%
  select(AREACD, Cluster) %>%
  rename("LAD23CD" = "AREACD") %>%
   mutate(Cluster = case_when(Cluster == "0" ~ "Demo_A",
                              Cluster == "1" ~ "Demo_B",
                              Cluster == "2" ~ "Demo_C",
                              Cluster == "3" ~ "Demo_D"),
          Cluster = as.factor(Cluster))

# Join clusters to shapefile
shp2 <- shp %>%
  left_join(df_c, by = c("LAD23CD")) %>%
  filter(!is.na(Cluster))

# Produce map
demo_map <- ggplot(shp2) +
  geom_sf(aes(fill = Cluster), size = 0.01) +
  theme(axis.text.x = element_blank(),
        axis.text.y = element_blank(),
        axis.ticks = element_blank(),
        rect = element_blank()) +
   scale_fill_manual(values = c("Demo_A" = "#3399CC", "Demo_B" = "#FF9933", 
                                "Demo_C" = "#66CC33", "Demo_D" = "#FF3300"))


# Add worksheet to excel
addWorksheet(wb, "Demographic_model")

# Add Plot
print(demo_map)
insertPlot(wb, "Demographic_model", xy = c("B", 2), width = 5, height = 7, fileType = "png", units = "in")


###########################

# Load in data
file_name = "Cluster_Maps_Data_20240221"
health_df <- read_excel(paste0("C:/Users/",win_username,"/Office for National Statistics/Strategy, Coordination and Dissemination - Documents/Sharepoint_data_loading/Clustering/Outputs/Clustering_model_outputs/",file_name,".xlsx"), 
                      sheet = "Health & Wellbeing") 


# Load in shapefile
#shp <- st_read("D:/IDS/Subnational Statistics/Data/clustering_local/LAD_MAY_2023_UK_BFC_V2.shp")

# Filter on Local Authority Code & Cluster
#  Set cluster to factor
df_c <- health_df %>%
  select(AREACD, Cluster) %>%
  rename("LAD23CD" = "AREACD") %>%
   mutate(Cluster = case_when(Cluster == "0" ~ "Health_A",
                              Cluster == "1" ~ "Health_B",
                              Cluster == "2" ~ "Health_C",
                              Cluster == "3" ~ "Health_D"),
          Cluster = as.factor(Cluster))

# Join clusters to shapefile
shp2 <- shp %>%
  left_join(df_c, by = c("LAD23CD")) %>%
  filter(!is.na(Cluster))

# Produce map
health_map <- ggplot(shp2) +
  geom_sf(aes(fill = Cluster), size = 0.01) +
  theme(axis.text.x = element_blank(),
        axis.text.y = element_blank(),
        axis.ticks = element_blank(),
        rect = element_blank()) +
   scale_fill_manual(values = c("Health_A" = "#3399CC", "Health_B" = "#FF9933", 
                                "Health_C" = "#66CC33", "Health_D" = "#FF3300"))

health_map

# Add worksheet to excel
addWorksheet(wb, "Health_model")

# Add Plot
print(health_map)
insertPlot(wb, "Health_model", xy = c("B", 2), width = 5, height = 7, fileType = "png", units = "in")


# Save Excel Workbook
saveWorkbook(wb, file = "C:/Users/kellyj2/Office for National Statistics/Strategy, Coordination and Dissemination - Documents/Sharepoint_data_loading/Clustering/Outputs/Clustering_model_outputs/Cluster_Maps_20240221.xlsx", overwrite = TRUE)















install.packages("ggradar", dependencies = TRUE, type = "win.binary")






















########################################################

install.packages("officer", dependancies = TRUE, type = "win.binary")
install.packages("rvg", dependancies = TRUE, type = "win.binary")
library(officer)
library(rvg)


anyplot = dml(ggobj = map,
               bg = "white",
               pointsize = 12,
               editable = TRUE)

doc <- read_pptx()
doc <- add_slide(doc, "Title and Content", "Office Theme")
doc <- ph_with(doc, anyplot, location = ph_location_fullsize())
fileout <- "test_map.pptx"
print(doc, target = fileout)


ggsave(
  plot = map,
  filename = "global_model_map.png",
  bg = "transparent"
)






