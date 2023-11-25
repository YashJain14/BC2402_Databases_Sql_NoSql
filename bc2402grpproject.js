show dbs
use co2_emissions_canada // change to database name


//Q1
db.co2_emissions_canada.distinct('VehicleClass').length;

//Q2
db.co2_emissions_canada.aggregate([
  {
    $group: {
      _id: { VehicleClass: "$VehicleClass", Transmission: "$Transmission" },
      AverageEngineSize: { $avg: "$EngineSize_L" },
      AverageFuelConsumptionCity: { $avg: "$FuelConsumptionCity_L_100km" },
      AverageFuelConsumptionHwy: { $avg: "$FuelConsumptionHwy_L_100km" },
      AverageCO2Emissions: { $avg: "$CO2Emissions_g_km" }
    }
  },
  {
    $project: {
      _id: 0, // Exclude the _id field from the results
      VehicleClass: "$_id.VehicleClass",
      Transmission: "$_id.Transmission",
      AverageEngineSize: 1,
      AverageFuelConsumptionCity: 1,
      AverageFuelConsumptionHwy: 1,
      AverageCO2Emissions: 1
    }
  }
]);


// Q3
db.ev_stations_v1.aggregate([
    {$project:{_id:1,ZIP:1,EVNetwork:1,DateLastConfirmed:{$convert: {input: "$DateLastConfirmed", to:"date",onError:"Error converting", onNull:"Null Value"}}}},
    {$match:{DateLastConfirmed:{$gte:(ISODate("2010-01-01T08:00:00.000+08:00"))}}},
    {$match:{DateLastConfirmed:{$lte:(ISODate("2022-12-31T08:00:00.000+08:00"))}}},
    {$group: { _id:{ZIP: "$ZIP",EVNetwork:"$EVNetwork"},totalstations:{$sum:1}}}])
//Output 18862 docs

//Q4
db.ev_stations_v1.aggregate([
    {$match:{Latitude:{$gte:"33.20",$lte:"34.70"}}},
    {$match:{Longitude:{$lte:"-118.40",$gte:"-117.20"}}},
    {$group: { _id:{ZIP: "$ZIP"},totalstations:{$sum:1}}}])   
//Output 357 docs

// Q5
db.electric_vehicle_population.aggregate([
    {$match:{Make:"TESLA"}},
    {$group: { _id:{State: "$State",Model:"$Model"},totalTelsa:{$sum:1}}},
    {$sort: {totalTesla:-1}}])
// Output 65 docs

//Q6
db.electric_vehicle_population.aggregate([
    {$project:{_id:1,ElectricVehicleType:1, CleanAlternativeFuelVehicleEligibility:1,ElectricRange:{$convert: {input: "$ElectricRange", to:16}}}},
    {$group: { _id:{ElectricVehicleType: "$ElectricVehicleType",
    CleanAlternativeFuelVehicleEligibility:"$CleanAlternativeFuelVehicleEligibility"},
    averageelectricrange :{$avg:"$ElectricRange"}}}])
//Output 5 docs


//Q7
db.co2_emissions_canada.aggregate([
    {$project: {Make: 1, Model: 1, CO2Emissions_g_km: 1}},
    {
        $addFields: {
            CO2Emissions_g_km: {
                $toInt: {
                    $trim: {
                        input: "$CO2Emissions_g_km",
                        chars: "\r"
                    }
                }
            }
        }
    },
    {
        $group: {
            _id: {
                Make: "$Make",
                Model: "$Model"
            },
            Estimate_CO2Emissions_g_km: {$avg: "$CO2Emissions_g_km"}
        }
    },
    {$project: {Make: "$_id.Make", Model: "$_id.Model", Estimate_CO2Emissions_g_km: 1, _id: 0}},
    {$out: "CO2_Estimates"}
    ])
db.electric_vehicle_population.aggregate([
    {$project: {Make: 1, Model: 1, State: 1}},
    {
        $group: {
            _id: {
                Make: "$Make",
                Model: "$Model",
                State: "$State"
            },
            number_of_vehicles: {$sum: 1}
        }
    },
    {$project: {Make: "$_id.Make", Model: "$_id.Model", State: "$_id.State", number_of_vehicles: 1, _id: 0}},
    {$lookup: 
        {
            from: "CO2_Estimates",
            let: {a: "$Make", b: "$Model"},
            pipeline: [
                {$match: {$and: [
                    {$expr: {$eq: ["$Make" , "$$a"]}},
                    {$expr: {$eq: ["$Model" , "$$b"]}}
                    ]}}
                ],
            as: "matches"
        }
    },
    {
        $project: {
            Make: 1,
            Model: 1,
            State: 1,
            number_of_vehicles: 1,
            Estimate_CO2Emissions_g_km: {
                $cond: {
                    if: {$eq: ["$matches", []] },
                    then: 0,
                    else: {$arrayElemAt: ["$matches.Estimate_CO2Emissions_g_km", 0] }
                }
            },
            _id: 0
            
        } }
    ]);
// output 296 docs
        
//Q8
// if collections [ev_stations_v1] and [electric_vehicle_population] are supposed to be used):
//Start of Q8i
db.electric_vehicle_population.aggregate([
    {$project: {State: 1, _id: 0}},
    {
        $group: {
            _id: "$State",
            Number_of_Electric_Vehicles: {$sum: 1}
        }
    },
    {$out: "Number_of_Electric_Vehicles_per_State"}
    ])
db.ev_stations_v1.aggregate([
    {$project: {State: 1, _id: 0}},
    {
        $group: {
            _id: "$State",
            Number_of_EV_Stations: {$sum: 1}
        }
    },
    {
        $lookup: {
            from: "Number_of_Electric_Vehicles_per_State",
            localField: "_id",
            foreignField: "_id",
            as: "matches"
        }
    },
    {
        $project: {
            _id: 1,
            Number_of_EV_Stations: 1,
            Number_of_Electric_Vehicles: {
                $cond: {
                    if: {$eq: ["$matches", []] },
                    then: 0,
                    else: {$arrayElemAt: ["$matches.Number_of_Electric_Vehicles", 0] }
                }
            }
        }
    },
    {
        $addFields: {
            EV_to_EV_Station_Ratio: {$divide: ["$Number_of_Electric_Vehicles", "$Number_of_EV_Stations"]}
        }
    },
    {
        $sort: {
            EV_to_EV_Station_Ratio: -1
        }
    }
    ])
//For almost all the states, the number of EV stations do not matter as there are more EV stations to EVs except for the State: WA, which has
//an EV_to_EV_Station_Ratio of 62.0483. This suggest that the State: WA needs more EV stations to meet demand.
//End of Q8i
//Start of Q8ii
db.electric_vehicle_population.aggregate([
    {$project: {PostalCode: 1, _id: 0}},
    {
        $group: {
            _id: "$PostalCode",
            Number_of_Electric_Vehicles: {$sum: 1}
        }
    },
    {$out: "Number_of_Electric_Vehicles_per_PostalCode"}
    ])
db.ev_stations_v1.aggregate([
    {$project: {ZIP: 1, _id: 0}},
    {
        $group: {
            _id: "$ZIP",
            Number_of_EV_Stations: {$sum: 1}
        }
    },
    {
        $lookup: {
            from: "Number_of_Electric_Vehicles_per_PostalCode",
            localField: "_id",
            foreignField: "_id",
            as: "matches"
        }
    },
    {
        $project: {
            _id: 1,
            Number_of_EV_Stations: 1,
            Number_of_Electric_Vehicles: {
                $cond: {
                    if: {$eq: ["$matches", []] },
                    then: 0,
                    else: {$arrayElemAt: ["$matches.Number_of_Electric_Vehicles", 0] }
                }
            }
        }
    },
    {
        $addFields: {
            EV_to_EV_Station_Ratio: {$divide: ["$Number_of_Electric_Vehicles", "$Number_of_EV_Stations"]}
        }
    },
  {
    $match: {
      _id: { $ne: 0 }
    }
  },
    {
        $sort: {
            EV_to_EV_Station_Ratio: -1
        }
    }
    ])
//The large majority of the data has an EV_to_EV_Station_Ratio of 0, which suggests that EV stations //outnumber EVs in general. However, some 
//EV_to_EV_Station_Ratio are unnaturally high, the highest being 1772. This might suggest that EV //station locations are not optimized to 
//meet demand.
//End of Q8ii

//(if collections [evByPostalCode], [evByState, [evStationByState]and [evStationByZIP] are supposed to be used):
//Start of Q8i
db.evStationByState.aggregate([
  {
    $lookup: {
      from: "evByState",
      localField: "groupByState",
      foreignField: "groupByState",
      as: "matches"
    }
  },
  {
    $unwind: {path: "$matches", preserveNullAndEmptyArrays: true}
  },
  {
    $project: {
      _id: "$groupByState",
      stationCount: 1,
      vehicleCount: {$ifNull: ["$matches.vehicleCount", 0]},
      vehicleToStationRatio: {
        $cond: {
          if: {$gt: ["$stationCount", 0]},
          then: {$divide: ["$matches.vehicleCount", "$stationCount"]},
          else: 0
        }
      }
    }
  },
  {
    $sort: {vehicleToStationRatio: -1}
  }
])
//End of Q8i

//Start of Q8ii
db.evStationByZIP.aggregate([
  {
    $lookup: {
      from: "evByPostalCode",
      localField: "groupByZIP",
      foreignField: "groupByPostalCode",
      as: "matches"
    }
  },
  {
    $unwind: {path: "$matches", preserveNullAndEmptyArrays: true}
  },
  {
    $project: {
      _id: "$groupByZIP",
      stationCount: 1,
      vehicleCount: {$ifNull: ["$matches.vehCount", 0]},
      vehicleToStationRatio: {
        $cond: {
          if: {$gt: ["$stationCount", 0]},
          then: {$divide: ["$matches.vehCount", "$stationCount"]},
          else: 0
        }
      }
    }
  },
  {
    $match: {
      _id: { $ne: 0 }
    }
  },
  {
    $sort: {vehicleToStationRatio: -1}
  }
])
//End of Q8ii



// Q9
db.nei_2017_full_data.aggregate([
  {
    $match: {
      naicsDescription: { $regex: "auto|motor", $options: "i" } // The "i" option makes it case insensitive
    }
  },
  {
    $group: {
      _id: "$naicsDescription",
      totalEmissions: { $sum: "$totalEmissions" }
    }
  }
]);


//Q10 
db.nei_2017_full_data.aggregate([
  {
    $match: {
      $and: [
        {naicsCode: {$regex: /^(31|32|33)/ }},
        {
          $or: [
            {companyName: {$regex: "dana", $options: "i"}},
            {companyName: {$regex: "emerson", $options: "i"}},
            {companyName: {$regex: "nucor", $options: "i"}},
            {companyName: {$regex: "micron", $options: "i"}},
            {companyName: {$regex: "allegheny", $options: "i"}},
            {companyName: {$regex: "albemarle", $options: "i"}},
            {companyName: {$regex: "schneider", $options: "i"}},
            {companyName: {$regex: "veatch", $options: "i"}},
          ]
        }
      ]
    }
  },
  {
    $addFields: {
      companyName: {
        $switch: {
          branches: [
            {case: {$regexMatch: {input: "$companyName", regex: "dana", options: "i"}}, then: "dana"},
            {case: {$regexMatch: {input: "$companyName", regex: "emerson", options: "i"}}, then: "emerson"},
            {case: {$regexMatch: {input: "$companyName", regex: "nucor", options: "i"}}, then: "nucor"},
            {case: {$regexMatch: {input: "$companyName", regex: "micron", options: "i"}}, then: "micron"},
            {case: {$regexMatch: {input: "$companyName", regex: "allegheny", options: "i"}}, then: "allegheny"},
            {case: {$regexMatch: {input: "$companyName", regex: "albemarle", options: "i"}}, then: "albemarle"},
            {case: {$regexMatch: {input: "$companyName", regex: "schneider", options: "i"}}, then: "schneider"},
            {case: {$regexMatch: {input: "$companyName", regex: "veatch", options: "i"}}, then: "veatch"},
          ],
          default: "$companyName"
        }
      }
    }
  },
  {
    $group: {
      _id: {state: "$state", companyName: "$companyName"},
      Sum_Total_Emissions_Pounds: { 
          $sum: {
          $cond: [
            {$eq: ["$emissionsUom", "LB"]},
            {$toDouble: "$totalEmissions"},
            {$multiply: [{$toDouble: "$totalEmissions"}, 2000]}
          ]
        }
        }
    }
  },
  {
    $project: {
      state: "$_id.state",
      companyName: "$_id.companyName",
      Sum_Total_Emissions_Pounds: 1,
      _id: 0
    }
  },
  {
    $group: {
      _id: "$state",
      companies: {
        $push: {
          companyName: "$companyName",
          Sum_Total_Emissions_Pounds: "$Sum_Total_Emissions_Pounds"
        }
      }
    }
  },
]);




// Q13
db.nei_2017_full_data.aggregate([
    {
        $addFields: {
            naicsCode: {
                $substr: ["$naicsCode", 0, 2]
            }
        }
    },
    {
        $project: {
            _id: 0,
            state: 1,
            siteName: 1,
            naicsCode: 1
        }
    },
    {
        $group: {
            _id: "$siteName",
            uniqueDocument: {$first: "$$ROOT"}
        }
    },
    {
        $replaceRoot: {newRoot: "$uniqueDocument"}
    },
    {$project: {siteName: 0}},
    {
        $group: {
            _id: {state: "$state", naicsCode: "$naicsCode"},
            frequency: {$sum: 1}
        }
    },
    {
        $group: {
            _id: "$_id.state",
            naicsCodes: {
                $push: {
                    k: "$_id.naicsCode",
                    v: "$frequency"
                }
            }
        }
    },
    {
        $project: {
            _id: 0,
            state: "$_id",
            naicsCodes: {$arrayToObject: "$naicsCodes"}
        }
    },
    {$out: "NAICS_Sector_Codes_By_State"}
])

db.evStationByState.aggregate([
    {
        $lookup: {
            from: "evByState",
            localField: "groupByState",
            foreignField: "groupByState",
            as: "matches"
        }
    },
    {
        $project: {
            _id: 1,
            stationCount: 1,
            groupByState: 1,
            vehicleCount: {
                $cond: {
                    if: {$eq: ["$matches", []] },
                    then: 0,
                    else: {$arrayElemAt: ["$matches.vehicleCount", 0] }
                }
            }
        }
    },
    {
        $project: {
            _id: "$_id.groupByState",
            stationCount: "$stationCount",
            vehicleCount: "$vehicleCount"
        }
    },
    {
        $addFields: {
            EV_to_EV_Station_Ratio: {$divide: ["$vehicleCount", "$stationCount"]}
        }
    },
    {
        $sort: {
            EV_to_EV_Station_Ratio: -1
        }
    },
    {
        $lookup: {
            from: "NAICS_Sector_Codes_By_State",
            localField: "_id",
            foreignField: "state",
            as: "matches"
        }
    },
    {
        $addFields: {
            naicsCodes: "$matches.naicsCodes"
        }
    },
    {
        $project: {
            _id: 1,
            stationCount: 1,
            vehicleCount: 1,
            EV_to_EV_Station_Ratio: 1,
            naicsCodes: 1
        }
    }
    ])



//Q14
//Extracted from the group report (please refer to group report for figures)

//Would a general, robust adoption of EVs be adequate to turn around the climate crisis?
//Based on our extensive research on the areas of Evs and climate crisis, it is not adequate to turn //around the climate crisis but should be able to reduce the impact of climate crisis with the //reason being carbon dioxide is one of the main causes for the climate crisis (NASA, 2023). //Having EVs introduced and rolled out globally would help to reduce the emissions of carbon //dioxide (CO2). This is because the global transportation sector contributes largely to CO2 //emissions, releasing more than seven billion metric tons a year (Statista, 2022). Additionally, //the group, cars and vans, was the major polluter of the global transportation sector, contributing //48% of  CO2 emissions for the global transportation sector (Figure 1) (Statista, 2022). //Currently, there are 10.64 millions of EV sales worldwide  and is expected to continue to rise //(Figure 2) (Statista, 2023). This is also the same for charging stations (Figure 3) (Statista, 2023). 

//Our team analyses that an increase in EVs sales would definitely decrease the percentage of //CO2 emissions for the group, cars and vans, and the global transportation sector. This would //further lead to a decrease in CO2 emissions globally and reduce the degree of climate crisis. //However, it might take several years to see a vast drop in the percentage of CO2 emission as //most people have the perception that  petrol cars are much better than EVs. (Ceenergy News, //2022). Examples would be convenience and performance purposes. Hence, debunking the //misconception is the top most priority and also not all electricity was generated from renewable //energy. 

//Would electricity generation remain to be largely fossil-based, nonetheless?


//What else can we do in the short term and long term?
//For short term plans with the purpose of developing a good habit, one could first spread //awareness to each other about the current situation about global boiling (transit from global //warming) so as to understand the seriousness of it and serve as a motivation to take immediate //action.Furthermore, one can also minimise carbon footprint in our daily lives by using //energy-efficient appliances (i.e. using those with very good or excellent energy efficiency //rating from the energy label (Figure 5)).



