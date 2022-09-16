using LinearAlgebra, EvoTrees, XLSX, CSV, DataFrames
using StatsBase

dataPath1 = "/home/oem/CDL1000/julia/fuelPredictor/v_negative1/data_sets/pswrgvwall.xlsx"
dataPath2 = "/home/oem/CDL1000/julia/fuelPredictor/v_negative1/data_sets/psw18vwall.xlsx"

xf1 = XLSX.readxlsx(dataPath1)
#sheets = XLSX.sheetnames(xf1)

alphabet = ["A";"B";"C";"D";"E";"F";"G";"H";"I";"J";"K";"L";"M";"N";"O";
            "P";"Q";"R";"S";"T";"U";"V";"W";"X";"Y";"Z"]

function makeBetaGam(alphabet)
    betagam = []
    for a in alphabet
        push!(betagam,string(a))
    end
    for b in alphabet
        for g in alphabet
            push!(betagam, string(b,g))
        end
    end
    return betagam
end                


global betagam = makeBetaGam(alphabet);

#=
The EIA data has a contents page first and then has sheetsso we'll read data for 
sheets[2:end]
=#
function getSheetsInfo(xlfile)
    sheets = XLSX.sheetnames(xlfile)
    sheetSizes = []
    for sheet in sheets[2:end]
        sheetSize = size(xf1[sheet][:])
        push!(sheetSizes, sheetSize)
    end
    return sheets, sheetSizes
end        

function makeDataFrameFromSheet(xlfile, sheetName, range1, range2)
    range = string(range1,":",range2)
    df = DataFrame(xlfile[sheetName][range],:auto)
    #we use :auto because the xl sheet is read as a Matrix
    return df[1:end-1,1:end-1] #the last row and column are missing, so we remove them    
end 

function getdfNames(df, xlfile, sheetName, range1, range2)
    #=
    in the EIA data there are three columns of information before
    the numerical bit.  So we will likely have A3:Z3 or something similar
    =#
    range = string(range1,":",range2)
    names = xlfile[sheetName][range]
    rename!(df, Symbol.(vec(names)))
    return df
end    

function combineAllSheets(xlfile)
    #betagam is a global vector of xlfile possible column names up to 702 columns
    sheetNames, sheetSizes = getSheetsInfo(xlfile)
    dfs = []
    for (k,s) in enumerate(sheetNames)
        sname = s
        ssize = sheetSizes[k]
        data_range1 = "A4"
        
        data_range2 = string(betagam[ssize[2]],ssize[1])
        tempdf = makeDataFrameFromSheet(xlfile, s, data_range1, data_range2)
        name_range1 = "A3"
        name_range2 = string(betagam(ssize[2]),3) #the third row is the row of names
        try
            tempdf = getdfNames(tempdf, xlfile, sname, name_range1, name_range2)
            push!(dfs, tempdf)
        catch
            println("Had a naming error with ", s)
        end
    end
    big_df = dfs[1]
    L = length(dfs)
    if L > 1
        for item = 2:L
            big_df = innerjoin(big_df, dfs[item], on = :Date)
        end
    end
    return big_df
end    







#=
df1 = makeDataFrameFromSheet(xf1, "Data 1", A4, V1677)
df2 = makeDataFrameFromSheet(xf1, "Data 2", A4, T1454)
df12 = innerjoin(df1, df2, on = :Date)
=#
function splitTrainTest(df, training_proportion)
    training_df = df[1:Int(floor(size(df)[1]*training_proportion)), :]
    testing_df = df[Int(floor(size(df)[1]*training_proportion))+1 : end, :]
    return training_df, testing_df
end    

function getXY(df, targetName, targetHorizon)
    X = df[1:end - targetHorizon , :] #Want to keep target in for autoregressive information
    Y = df[targetHorizon+1 : end, targetName]
    return Array(X), Array(Y)
end

#=
get a random target
targetName = names(df12)[rand(collect(1:size(df12)[2]))]
=#

function getCovariance(df, targetName, numFeatures)
    #=
    We expect the dataframe passed in to be cleaned and prepared
    Perhaps this is already in log returns, z-scored, mean scaled, etc
    check eltype.(eachcol(df)) and remove Date
    =#
    df2 = select(df, Not("Date"))
    covmat = cov(Matrix(coalesce.(df2,0)))
    return covmat
end    

function getRandomTrainingFeatures(df, covmat, targetName, numFeautres)
    df2 = select(df, Not("Date"))
    loc = findall(x->x==targetName, names(df2))[1]
    wts = covmat[:,loc] #get the correlation probabilities 
    wts /= sum(wts)
    numFeatures = minimum([numFeatures; length(wts)])
    featureLocs = wsample(collect(1:length(wts)), wts, numFeatures, replace = false)
    if !in(loc,featureLocs) 
        push!(featureLocs, loc)
    end    
    return featureLocs
end

#=
Here we need to do something significant with the large amount of missing data.
Perhaps we choose the columns first then drop the missing data.
This will have the consequence of losing the correlation probabilites.
We could attempt to use DynamicsTimeWarping and use the distances to be inverse probabilities.

In the dataframe we need
dropmissing(df)
in a single Vector
v = collect(skipmissing(v))
https://discourse.julialang.org/t/how-remove-missing-value-from-vector/58916
=#





function trainOneExample(df, targetName, horizon, parametersDictionary)
    X,Y = getXY(df, targetName, horizon)
    params = EvoTreeRegressor(; parametersDictionary...)
    model = fit_evotree(params, X, Y)
    return model
end     

function makeOnePrediction(model, x_predictor)
    pred = predict(model, x_predictor)
    return pred
end    

function makeEnsemblePredictions(models::Vector, x_predictor)
    df = DataFrame()
    for (k,m) in enumerate(models)
        tempPrediction = EvoTrees.predict(m, x_predictor)
        insertcols!(df, "model $k" => vec(tempPrediction))
    end
    dfarr = Array(df)
    modmean = vec(mean(dfarr,dims=2))
    modmed = vec(median(dfarr, dims=2))
    modmax = vec(maximum(dfarr,dims=2))
    modmin = vec(minimum(dfarr, dims = 2))
    moddev = vec(std(dfarr, dims = 2))
    modupci = vec(modmed .+ 2*moddev) #upper confidence of 2 devs above median
    modlowci = vec(modmed .- 2*moddev) #lower confidence of 2 devs below median
    insertcols!(df, :Mean => modmean, :Median => modmed, :Max => modmax,
                    :Min => modmin, :Dev => moddev, :UpperCI => modupci,
                    :LowerCI => modlowci)
    return df
end 
    
function buildRandomEvoTrees(numberOfTrees)
    losses = [:linear,:logistic,:L1,:quantile]
    metrics = [:mse,:rmse,:mae,:logloss,:none]
    paramsList = []
    for k = 1:numberOfTrees
        params = Dict(:loss => rand(losses), :metric => rand(metrics),
             :max_depth => rand(collect(3:9)), :eta => rand(), 
             :gamma => rand(), :alpha => rand(), :lambda => rand(), 
             :nrounds => rand(collect(20:100)), :nbins => rand(collect(50:200)), 
             :min_weight => 2*rand())
        push!(paramsList,params)
    end
    return paramsList
end    
    
