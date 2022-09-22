#=
In this particular script we're going to go through the exercise of taking 
sample data provided by Saad and building a complete dummy database 
from which we can cluster loads.
=#

#= Sample Data 
mydata = """{
    "commodities": [
        "Meat",
         "Wood"
    ],
    "polyline": "BGs8vmxBjq1j5ETrOnB_dnG3DrEvCrJvCT_JTzK_ErErEzFnB3IAjIAjDvC3IrErEnGnBvHAjNUnGU_JUrY8BjIUrOU7GoB7GoG", // for google map
    "temp": 0, // if reefer
    "billing_rate": 233, // asking price
    "reference_number": "asdf", // reference number of the load
  "seal_number": “asd”,
    "palatized": null, // null if not applicable True if shipment is palatized
    "load_details": [
        {
            "date": "2022-03-22 10:30 AM", // pickup time
            "date_end": "2022-03-23 10:30 AM", // dropoff time
            "number": "sd",  // pick up number
            "postal_code": "90401",
            "address": "1212 Santa Monica, 3rd Street Promenade, Santa Monica, CA, USA",
            "is_pickup": true, // if for pickup false for drop off
            "state_id": 5, // state id in correspondence to our Database State table
            "city_id": 519, //  city id in correspondence to our Database State table
            "latitude": 34.0179429,
            "longitude": -118.4990073,
            "stop_comments": "asd"
        },
        {
            "date": "2022-03-23 10:30 AM",
            "date_end": “2022-03-23 10:30 AM",
            "number": "sdf", // drop off number
            "postal_code": "01001",
            "address": "DFF Corporation, Gen Creighton W Abrams Drive, Agawam, MA, USA",
            "commodities": [
                "sdf"
            ],
            "is_pickup": false,
            "state_id": 20,
            "city_id": 1677,
            "latitude": 42.054429,
            "longitude": -72.66135919999999,
            "stop_comments": "asd"
        }
    ],
    "meta_container_name": "53' Reefer Trailer",
    "equipment_type": "Trailer",
    "weight": 323
}"""


Some notes, the state id seems to correspond to the alphabetical listing of states with Hawai'i 
removed as we don't ship there over the road from out of state.

We will clean this ever so slightly and we can use the JSON package to parse using the 
triple "

=#

mydata = """{
    "commodities": [
        "Meat",
         "Wood"
    ],
    "polyline": "BGs8vmxBjq1j5ETrOnB_dnG3DrEvCrJvCT_JTzK_ErErEzFnB3IAjIAjDvC3IrErEnGnBvHAjNUnGU_JUrY8BjIUrOU7GoB7GoG",
    "temp": 0, 
    "billing_rate": 233, 
    "reference_number": "asdf", 
    "seal_number": "asd",
    "palatized": null, 
    "load_details": [
        {
            "date": "2022-03-22 10:30 AM", 
            "date_end": "2022-03-23 10:30 AM", 
            "number": "sd",  
            "postal_code": "90401",
            "address": "1212 Santa Monica, 3rd Street Promenade, Santa Monica, CA, USA",
            "is_pickup": true, 
            "state_id": 5, 
            "city_id": 519, 
            "latitude": 34.0179429,
            "longitude": -118.4990073,
            "stop_comments": "asd"
        },
        {
            "date": "2022-03-23 10:30 AM",
            "date_end": "2022-03-23 10:30 AM",
            "number": "sdf", 
            "postal_code": "01001",
            "address": "DFF Corporation, Gen Creighton W Abrams Drive, Agawam, MA, USA",
            "commodities": [
                "sdf"
            ],
            "is_pickup": false,
            "state_id": 20,
            "city_id": 1677,
            "latitude": 42.054429,
            "longitude": -72.66135919999999,
            "stop_comments": "asd"
        }
    ],
    "meta_container_name": "53' Reefer Trailer",
    "equipment_type": "Trailer",
    "weight": 323
}"""

using JSON
dataDict = JSON.parse(mydata);

using CSV, DataFrames, StatsBase 

zipdf = CSV,read("/home/oem/CDL1000/data/external/uszips3.csv", DataFrame)
zipdf = select(zipdf,[:zip,:latitude,:longitude,:population,:density])
zipdf = dropmissing(zipdf)


#=
This result, based on the sample data is a dictionary with 11 entires.
One of the entries is "load_details"
This is a length 2 vector where each item is a dictionary.
dataDict["load_details"][1] is a dictionary with 11 entries should have commodities as well
This is the pick up detail
dataDict["load_details"][1] is a dictionary with 12 entries 
This is the drop off detail


We can deal with only 
zip code (which encodes lat/long/city_id,state_id), is_pickup, date, date_end
for the pickup/dropoff details. Which gives 8 items.
For the clustering, if we have origin lat/long and destination lat/long in the same order,
we can remove pickup/dropoff.

For the moment we don't have multiple pick/drop items


So with zip origin, zip destination, date (origin), date_end(destination) we can add in

equipment_type, commodities, palatized, billing_rate
We don't need temp (temperature = 0 if reefer).
=#

#=
For date formatting
df = DateFormat("yyyy-mm-ddTHH:MM:SS") gets the date_end into the format we need
=#

function formatJSONtoDataFrame(jsonObject)
    dataDict = JSON.parse(jsonObject)
    df = DataFrame(dataDict)
    df1 = select(df, :load_details)
    df2 = select(df, Not(:load_details))
    origindf = DataFrame(df1[1,:load_details])
    originNames = names(origindf)
    newOriginNames= [string("Origin_", n) for n in originNames]
    rename!(origindf, newOriginNames)
    insertcols!(origindf, :reference_number => df1[1,:reference_number])
    destinationdf = DataFrame(df1[2,:load_details])
    destinationNames = names(destinationdf)
    newDestinationNames = [string("Destination_",n) for n in destinationNames]
    rename!(destinationdf, newDestinationNames)
    insertcols!(destinationdf, :reference_number => df1[1,:reference_number])
    bigdf = innerjoin(df1,origindf,destinationdf,on=:reference_number)
    return big_df

#=    
function reduceDataFrametoClusterFeatures(fullDataFrame)
    df = fullDataFrame
    clusterdf = select(df,[:billing_rate,:commodities,:equipment_type,:palatized,:weight,
    :Origin_postal_code,:Origin_date, :Destination_postal_code,:Destination_date_end])
    return clusterdf    
=#
    
#=
In order to get these clustering algorithms working, we will need to generate
a sufficient number of samples and a minimal number of dimensions.
Additionally we will fill the table with realistic data.
In this case, we can get origin and destination postal code.  We can pull lat/long 
from this info.  We will not need city_id, state_id, specific address.  
Additionally, is pickup will be clear as true on origin, and false on destination.
We will like billing_rate, but that will need to be a calculation based on distance, 
date, and equipment_type.

So we should sample
origin postal code, destination postal code, origin date, commodities, weight

We should "calculate" 
billing rate, lat/long, equipment_type (based on commodity)
=#

function convertDateStringtoDateTime(datestring)
    #= 
    date string will be in format yyyy-mm-dd hh:mm XM
    we will convert this to 24 hour 
    =#
    dateform = DateFormat("yyyy-mm-ddTHH:MM")
    ymd, hrmin, xm = split(datestring)
    hr,mn = split(hrmin,":")
    if xm == "PM"
        newhr = parse(Int,hr) + 12
    else
        newhr = hr
    end #if

    newdatestring = string(ymd,"T",newhr,":",mn)
    return DateTime(newdatestring, dateform)    
end 

function makeOneRandomDate()
    dateform = DateFormat("yyyy-mm-ddTHH:MM")
    mydate = Date("2022") + Day(rand(collect(1:365))) 
    mydate = string(mydate,"T",rand(collect(7:20)),":",rand([0,30]))
    startDate = DateTime(mydate,dateform)
    endDate = startDate + Hour(rand(3:48))
    return startDate, endDate
end    
    
function makeRandomDates(nSamples)
    startDates = []
    endDates = []
    for k = 1:nSamples
        st,en = makeOneRandomDate()
        push!(startDates,st)
        push!(endDates,en)
    end
    return startDates, endDates
end        




function simpleSampler(nSamples, postalcodedf)
    #=
    postal code df will have only 
    zipcodes, lat,long, population, and density
    so that we can make weighted samples from 
    population and density.
    We'll use density as the origin sampler 
    and population as the destination sampler
    =#

    pcdf = postalcodedf
    #clusterdf = DataFrame()
    originLocs = wsample(collect(1:size(pcdf)[1]), Array(pcdf[!,:density]), nSamples)
    destinationLocs = wsample(collect(1:size(pcdf)[1]), Array(pcdf[!,:population]), nSamples)
    origins = pcdf[originLocs,[:zip,:latitude,:longitude]]
    rename!(origins, [:Origin_zip,:Origin_latitude,:Origin_longitude])
    destinations = pcdf[destinationLocs,[:zip,:latitude,:longitude]]
    rename!(destinations, [:Destination_zip,:Destination_latitude,:Destination_longitude])
    inicios, terminados = makeRandomDates(nSamples)
    sample_df = [origins destinations]
    insertcols!(sample_df, :date => inicios, :date_end => terminados)
    return sample_df
end








