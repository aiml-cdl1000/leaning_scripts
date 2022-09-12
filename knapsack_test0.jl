#knapsack 0/1 Test 
#= 
we will try to solve a simple knpasack problem by dynamic 
    programming

following 
https://www.guru99.com/knapsack-problem-dynamic-programming.html

=#

using Random

wts = rand(collect(5:10),8)
vals = rand(collect(3:30),8)
wtsLocations = sortperm(wts)
wts = sort(wts)
vals = vals[wtsLocations]

wtCapactiy = rand(collect(31:41))


#Here's an example where we know the maximum answer is 15
#using items 3 and 4 with weights 6,4 and values 8,7 respectively

#=
wts = [5;6;6;4]
wtsLocations = sortperm(wts)
vals = [5;4;8;7]
wts = sort(wts)
vals = vals[wtsLocations]

wtCapactiy = 12
=#

#= we want to maximize the sum of values
of our items with differing weights, with a 
given weight capacity.
=#

Sack = zeros(length(wts)+1, wtCapactiy)

Sack[2,wts[1]+1:end] .= vals[1];

for col = wts[1]:size(Sack)[2]
for row = 3:size(Sack)[1]
    newcol = maximum([1;col - (wts[row-1]-1)]) 
    #= 
    since we have sorted the weights the next weight may be more than the current column
    so we don't want to go out of bounds.  If that is the case, we go back to the first column
    =#
    Sack[row,col] = maximum([Sack[row-1, col]; vals[row-1] + Sack[row-1, newcol]])
end 
end 

Sack = map(Int, Sack)

println(Sack[end,end])
function getItemsInfo(Sack::Array)
itemSelected = zeros(length(wts));
wtsSelected = []
valsSelected = []
current_col = size(Sack)[2]
current_row = size(Sack)[1]
while (current_col > 1) && (current_row > 1)
    print("column")
    println(current_col)
    print("Row ")
    println(current_row)
    if Sack[current_row, current_col] > Sack[current_row-1, current_col]
        println(current_row)
        itemSelected[current_row-1] = 1
        #current_col = Sack[current_row, current_col] - wts[current_row-1]
        #the above row is in the tutorial, but is obviously not correct
        #it is corrected below
        current_col = maximum([1;current_col - wts[current_row-1]])
        push!(wtsSelected,wts[current_row-1])
        push!(valsSelected,vals[current_row-1])
    end
    current_row -= 1
end

println(wtsSelected)
println(valsSelected)
println(itemSelected)

return wtsSelected, valsSelected, itemSelected
end