-- Q1
SELECT COUNT(DISTINCT VehicleClass) AS NumberOfClasses
FROM co2_emissions_canada;

-- Q2
SELECT VehicleClass, Transmission, AVG(EngineSize_L) AS AverageEngineSize, 
AVG(FuelConsumptionCity_L_100km) AS AverageFuelConsumptionInCity, 
AVG(FuelConsumptionHwy_L_100km) AS AverageFuelConsumptionInHighway,
AVG(CO2Emissions_g_km) AS AverageCO2Emissions
FROM co2_emissions_canada
GROUP BY VehicleClass, Transmission;

-- Q3
SELECT ZIP, EVNetwork, COUNT(*) AS NumberOfStations
FROM ev_stations_v1
WHERE YEAR(STR_TO_DATE(DateLastConfirmed, '%m/%d/%Y')) BETWEEN 2010 AND 2022
GROUP BY ZIP, EVNetwork
ORDER BY ZIP, EVNetwork;

-- Q4
select ZIP, count(StationName) from ev_stations_v1
where Latitude between 33.20 and 34.70 and Longitude between -118.40 and -117.20
group by ZIP;

-- Q5 
SELECT State, Model, COUNT(DISTINCT VIN)
FROM electric_vehicle_population
WHERE (Make = 'TESLA')
GROUP BY State, Model
ORDER BY COUNT(DISTINCT VIN) DESC;

-- Q6
select ElectricVehicleType, CleanAlternativeFuelVehicleEligibility, avg(ElectricRange) from electric_vehicle_population
group by ElectricVehicleType, CleanAlternativeFuelVehicleEligibility;

-- Q7
SELECT ev.Make, ev.Model, ev.State, COUNT(*) AS number_of_vehicles, COALESCE(AVG(CAST(cec.CO2Emissions_g_km AS DECIMAL(10, 2))), 0) AS Estimate_CO2Emissions_g_km
FROM electric_vehicle_population ev
LEFT JOIN co2_emissions_canada cec ON ev.Make = cec.Make AND ev.Model = cec.Model
GROUP BY ev.Make, ev.Model, ev.State;

-- Q8 i)
WITH ElectricVehicleCounts AS (
    SELECT State, COUNT(*) AS Number_of_Electric_Vehicles
    FROM electric_vehicle_population
    GROUP BY State
),
EVStationCounts AS (
    SELECT State, COUNT(*) AS Number_of_EV_Stations
    FROM ev_stations_v1
    GROUP BY State
)
SELECT ev.State, COALESCE(ev.Number_of_EV_Stations, 0) AS Number_of_EV_Stations, COALESCE(e.Number_of_Electric_Vehicles, 0) AS Number_of_Electric_Vehicles,
CASE
	WHEN COALESCE(e.Number_of_Electric_Vehicles, 0) = 0 THEN 0
	ELSE COALESCE(e.Number_of_Electric_Vehicles, 0) / COALESCE(ev.Number_of_EV_Stations, 1)
END AS EV_to_EV_Station_Ratio
FROM EVStationCounts ev
LEFT JOIN ElectricVehicleCounts e ON ev.State = e.State
ORDER BY EV_to_EV_Station_Ratio DESC;

-- Q8 ii)
WITH ElectricVehicleCounts AS (
    SELECT PostalCode, COUNT(*) AS Number_of_Electric_Vehicles
    FROM electric_vehicle_population
    GROUP BY PostalCode
),
EVStationCounts AS (
    SELECT ZIP, COUNT(*) AS Number_of_EV_Stations
    FROM ev_stations_v1
    GROUP BY ZIP
)
SELECT ev.ZIP, COALESCE(ev.Number_of_EV_Stations, 0) AS Number_of_EV_Stations, COALESCE(e.Number_of_Electric_Vehicles, 0) AS Number_of_Electric_Vehicles,
CASE
	WHEN COALESCE(e.Number_of_Electric_Vehicles, 0) = 0 THEN 0
	ELSE COALESCE(e.Number_of_Electric_Vehicles, 0) / COALESCE(ev.Number_of_EV_Stations, 1)
END AS EV_to_EV_Station_Ratio
FROM EVStationCounts ev
LEFT JOIN ElectricVehicleCounts e ON ev.ZIP = e.PostalCode
ORDER BY EV_to_EV_Station_Ratio DESC;

-- Q9
SELECT
    naicsDescription,
    SUM(totalEmissions) AS SumTotalEmissions
FROM
    nei_2017_full_data
WHERE
    naicsDescription LIKE '%auto%' OR
    naicsDescription LIKE '%motor%'
GROUP BY
    naicsDescription;

-- Q10
SELECT state, companyName,
SUM(CASE
	WHEN emissionsUom = 'LB' THEN totalEmissions
	ELSE totalEmissions * 2000
END) AS Sum_Total_Emissions_Pounds
FROM nei_2017_full_data
WHERE naicsCode REGEXP '^(31|32|33)'
AND (
	companyName REGEXP 'dana'
	OR companyName REGEXP 'emerson'
	OR companyName REGEXP 'nucor'
	OR companyName REGEXP 'micron'
	OR companyName REGEXP 'allegheny'
	OR companyName REGEXP 'albemarle'
	OR companyName REGEXP 'schneider'
	OR companyName REGEXP 'veatch'
)
GROUP BY state, companyName;

-- Q13
WITH ElectricVehicleCounts AS (
    SELECT State, COUNT(*) AS Number_of_Electric_Vehicles
    FROM electric_vehicle_population
    GROUP BY State
),
EVStationCounts AS (
    SELECT State, COUNT(*) AS Number_of_EV_Stations
    FROM ev_stations_v1
    GROUP BY State
)
SELECT ev.State, COALESCE(ev.Number_of_EV_Stations, 0) AS Number_of_EV_Stations, COALESCE(e.Number_of_Electric_Vehicles, 0) AS Number_of_Electric_Vehicles,
CASE
	WHEN COALESCE(e.Number_of_Electric_Vehicles, 0) = 0 THEN 0
	ELSE COALESCE(e.Number_of_Electric_Vehicles, 0) / COALESCE(ev.Number_of_EV_Stations, 1)
END AS EV_to_EV_Station_Ratio
FROM EVStationCounts ev
LEFT JOIN ElectricVehicleCounts e ON ev.State = e.State
ORDER BY EV_to_EV_Station_Ratio DESC;

SELECT state, LEFT(naicsCode, 2) AS sector_code, COUNT(*) AS frequency
FROM nei_2017_full_data
WHERE state = 'WA'
GROUP BY state, sector_code
ORDER BY state, sector_code;
