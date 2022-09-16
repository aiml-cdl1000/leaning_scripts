using LinearAlgebra
#= 
This is just a little script to compute a 1 term prediction of a time series
using log returns and a nearly arbitrary length lag sequence
=#

function predictOneTerm(ts, lags)
    #the time series ought to have all positive values
    ts = Array{Float32}(ts) #just in case we had Matrix{Any} 
    lts = log.(ts[2:end]) - log.(ts[1:end-1]) #same as log(a_n+1 / a_n) but computationally faster
    autoregMatrix = []
    logTarget = lts[lags-1 : end]
    finalLags = lts[end+1-lags:end]

    for k = 1:lags
        autoregMatrix = [autoregMatrix; ts[k:end-lags+k]]
    end
    ARM = Array{Float32}(reshape(autoregMatrix,:,lags))
    coefs = inv(ARM'*ARM)*ARM'*logTarget
    finalPred = exp(coefs'*finalLags)*ts[end]
    return finalPred
end    
