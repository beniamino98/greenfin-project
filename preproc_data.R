library(dplyr)
library(lubridate)
library(readr)
library(zoo)

# dataset available at:
# https://www.kaggle.com/datasets/nicholasjhana/energy-consumption-generation-prices-and-weather/data?select=energy_dataset.csv

# Working direc
wd <- getwd()

# Preprocess weather data for all the cities
df_weather <- read_csv(paste0(wd, "/summerschool2024/", "weather_features.csv")) %>%
  mutate(date_day = as.Date(dt_iso)) %>%
  rename(date = "dt_iso", city = "city_name") %>%
  mutate(
    Year = lubridate::year(date),
    Month = lubridate::month(date),
    Day = lubridate::day(date),
    Hour = lubridate::hour(date),
  ) %>%
  select(-weather_icon) %>%
  select(date, date_day, city, Year, Month, Day, Hour, everything()) %>%
  mutate(
    temp = temp -273.15,
    temp_min = temp_min -273.15,
    temp_max = temp_max -273.15,
    pressure = case_when(
      pressure > 1051 ~ NA_integer_,
      pressure < 931 ~ NA_integer_,
      TRUE ~ pressure
    ),
    wind_speed = case_when(
      wind_speed > 50 ~ NA_integer_,
      TRUE ~ wind_speed
    )
  )
# Remove duplicated rows
df_weather <- df_weather[-c(which(duplicated(select(df_weather, date, city)))),]
# Interpolate NAs
df_weather$pressure <- zoo::na.approx(df_weather$pressure)
df_weather$wind_speed <- zoo::na.approx(df_weather$wind_speed)

df_energy <- read_csv(paste0(wd, "/summerschool2024/", "energy_dataset.csv")) %>%
  select(date = "time",
         brown_coal = `generation fossil brown coal/lignite`,
         fossil = `generation fossil coal-derived gas`,
         fossil_gas = `generation fossil gas`,
         hard_coal = `generation fossil hard coal`,
         fossil_oil = `generation fossil oil`,
         fossil_oil_shale = `generation fossil oil shale`,
         fossil_peat = `generation fossil peat`,
         nuclear = `generation nuclear`,
         energy_other = `generation other`,
         # Renewables
         solar = `generation solar`,
         biomass = `generation biomass`,
         geothermal = `generation geothermal`,
         river = `generation hydro run-of-river and poundage`,
         river_pump = `generation hydro pumped storage consumption`,
         river_reservoir = `generation hydro water reservoir`,
         marine = `generation marine`,
         energy_other_renewable = `generation other renewable`,
         energy_waste = `generation waste`,
         wind = `generation wind offshore`,
         wind_onshore = `generation wind onshore`,
         # Forecast
         pred_solar = `forecast solar day ahead`,
         pred_wind_offshore = `forecast wind offshore eday ahead`,
         pred_wind_onshore = `forecast wind onshore day ahead`,
         pred_demand = `total load forecast`,
         pred_price = `price day ahead`,
         # Actual
         demand = `total load actual`,
         price = `price actual`,
  ) %>%
  # Aggregate fossil sources
  mutate(fossil = brown_coal + fossil + fossil_gas + hard_coal + fossil_oil + fossil_oil_shale + fossil_peat) %>%
  select(-brown_coal, -fossil_gas, -hard_coal, -fossil_oil, -fossil_oil_shale, -fossil_peat) %>%
  # Aggregate wind sources
  mutate(wind = wind + wind_onshore) %>%
  select(-wind_onshore, -pred_wind_offshore, -pred_wind_onshore) %>%
  # Aggregate river sources
  mutate(river = river + river_pump + river_reservoir) %>%
  select(-river_pump, -river_reservoir) %>%
  # Remove null columns
  select(-marine, -geothermal) %>%
  # Compute total production
  mutate(
    prod = fossil + wind + solar + biomass + river + nuclear,
    prod_renewable = wind + solar + biomass + river
  ) %>%
  # Auxiliary variables
  mutate(
    date_day = as.Date(date),
    Year = lubridate::year(date),
    Month = lubridate::month(date),
    Day = lubridate::day(date),
    Hour = lubridate::hour(date),
    Season = solarr::detect_season(date),
  ) %>%
  # Reorder variables
  select(date, date_day, Year, Month, Day, Hour, Season, everything())

# Impute NA values
df_energy$fossil <- zoo::na.approx(df_energy$fossil)
df_energy$nuclear <- zoo::na.approx(df_energy$nuclear)
df_energy$energy_other <- zoo::na.approx(df_energy$energy_other)
df_energy$solar <- zoo::na.approx(df_energy$solar)
df_energy$biomass <- zoo::na.approx(df_energy$biomass)
df_energy$river <- zoo::na.approx(df_energy$river)
df_energy$energy_other_renewable <- zoo::na.approx(df_energy$energy_other_renewable)
df_energy$energy_waste <- zoo::na.approx(df_energy$energy_waste)
df_energy$wind <- zoo::na.approx(df_energy$wind)
df_energy$demand <- zoo::na.approx(df_energy$demand)
df_energy <- mutate(df_energy,
                    prod = fossil + wind + solar + biomass + river + nuclear,
                    prod_renewable = wind + solar + biomass + river)

df_weather_hour <- df_weather %>%
  group_by(date, date_day, Year, Month, Day, Hour) %>%
  summarise(temp = mean(temp, na.rm = TRUE),
            temp_min = min(temp_min, na.rm = TRUE),
            temp_max = max(temp_max, na.rm = TRUE),
            pressure = mean(pressure, na.rm = TRUE),
            humidity = mean(humidity, na.rm = TRUE),
            wind_speed = mean(wind_speed, na.rm = TRUE),
            wind_deg = mean(wind_deg, na.rm = TRUE),
            rain_1h = mean(rain_1h, na.rm = TRUE),
            rain_3h = mean(rain_3h, na.rm = TRUE),
            snow_3h = mean(snow_3h, na.rm = TRUE),
            clouds_all = mean(clouds_all, na.rm = TRUE))

data <- left_join(df_energy, df_weather_hour, by = c("date", "date_day", "Year", "Month", "Day", "Hour"))

# Save data
write_csv(data, file = paste0(wd, "/summerschool2024/","data.csv"))
