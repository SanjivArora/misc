using DataFrames, XLSX,Plots,Measurements,HypothesisTests,Statistics,IterTools,Dates
##calculate the percentage reduction in toner fro each of the excel values









sheet =XLSX.readxlsx("C://Users//Ajefferi//Downloads//CartridgeWeightsPricesUpdated.xlsx")

sh = sheet["Sheet1"]

sh["B"][3]
#= calculating the means and standard errors

for pairing in IterTools.partition("BCDEFGHIJKLMNOPQRSTUVWXY",2,2)
    try
        price = sh[string(pairing[1])][3]
        #@show "doing $pairing"
        toner = sh[string(pairing[1])][2]
        println("$toner")
        prevData = sh[string(pairing[1])][9:61]
        emptyW = sh[string(pairing[1])][5]
        curData = sh[string(pairing[2])][9:61]
        fullW = sh[string(pairing[1])][4]
        prev = calculateResidualToner(emptyW,prevData)
        cur = calculateResidualToner(emptyW,curData)

        tw = fullW - emptyW
        diff = cur - prev

        percDiff = diff / tw * 100  ##as a percentage of the bottle used


    catch

        continue

    end
    ##write these values to the excel sheet
end
=#

function calculateResidualToner(emptyWeight,sample)

    #@show typeof.(sample)
    sample = [item for item in sample if typeof(item) != Missing]
    out = largecalc(sample,length(sample))
    return out - emptyWeight
    #length(sample) < 25 ? smallcal(sample,length(sample)) : largecalc(sample,length(sample))
end


function smallcalc(data,len)
    ##use t dsitribution to spcify our condfidence interval as well our your z statistic
    sampleMean = mean(data)
    t = one

end

function largecalc(data,len)
    ##using a 95% confidence interval
    sampleMean = mean(data)

    sampleAvg = mean(data) / len ##statistic

    test = OneSampleTTest(sampleMean,std(data),len)

    error = test.t * std(data) /len
    return measurement(sampleMean,error)
    ##run a hypothesis test for this
    #error = 2 * (σ / sqrt(len))
    #popEst =  measurement(sampleMean,error)


end


function calculateCosts(sh,offset = 0::Int,range= 62::Int)

    ##calculate total residual toner percentage acrocss alltoner kinds
    prev = []
    cur = []
    tonerBottles = []

    prevTot = []
    curTot = []
    prices = []

    for pairing in "BCDEFGHIJKLMNOPQRSTUVWXY"

            date = sh[string(pairing)][6 + offset]
            #@show "doing $pairing"
            toner = sh[string(pairing)][2 +offest]

            price =sh[string(pairing)][3+ offset]

            data = sh[string(pairing)][10 + offset:range]

            emptyW = sh[string(pairing)][5 + offset]
            fullW = sh[string(pairing)][4 + offset]
            #println(data)
            #push!(outs,typeof(sh[string(pairing)][9:60]) )
            push!(tonerBottles,toner)
            push!(prices,price)
            #@show date
            date == Date("2020-01-20") ? push!(prev,totalResToner(data,emptyW)) : push!(cur,totalResToner(data,emptyW))

            #@show emptyW, fullW
            totalToner = fullW - emptyW

            Arr = [item for item in data if typeof(item) != Missing]
            Arr = [item for item in Arr if typeof(item) != String]

            len = length(Arr)


            date == Date("2020-01-20") ? push!(prevTot,totalToner * len ) : push!(curTot,totalToner * len)

            #@show sum(prev), sum(cur)

            #percDiff = diff / tw * 100  ##as a percentage of the bottle used

        end
    ##write these values to the excel sheet

    return prev ./ prevTot, cur ./ curTot, [tonerBottles[i] for i in 1:length(tonerBottles) if i % 2 != 0],[prices[i] for i in 1:length(prices) if i % 2 != 0]

end



function totalResToner(Arr,w)

    Arr = [item for item in Arr if typeof(item) != Missing]
    Arr = [item for item in Arr if typeof(item) != String]

    return sum(Arr .- w)
end


function outputResults(sh,offset=0,range=62)

    DecBotPerc,JanBotPerc,tonerBottles,prices = calculateCosts(sh,offset,range)

    diffs = DecBotPerc .- JanBotPerc

    NovBottleCosts = (prices .* DecBotPerc)

    JanBottleCosts = (prices .* JanBotPerc)

    costPercDiffs = NovBottleCosts .- JanBottleCosts

    percNovBottleCosts = NovBottleCosts ./ prices

    percJanBottleCosts = JanBottleCosts ./ prices

    perBottleChanges = percNovBottleCosts .- percJanBottleCosts ##decrease in proportion of bottle costs


    percentCostNov = sum(NovBottleCosts) / sum(prices)

    PercentCostJan = sum(JanBottleCosts) / sum(prices)

    perCostDiff =   (percentCostNov - PercentCostJan) .* 100



    costReductions = Dict(tonerBottles .=> costPercDiffs)

    percCostReductions = Dict(tonerBottles .=> perBottleChanges)

    bottleReductions = Dict(tonerBottles .=> diffs)



    percentNov = sum(cur) / sum(curTot)

    PercentJan = sum(prev) / sum(prevTot)

    diff =   percentNov - PercentJan
    ##ideally

    return tonerBottles,percCostReductions,bottleReductions,percentNov,PercentJan,diff,percentCostNov,PercentCostJan,perCostDiff

end






function writeToFile(file,stat)
    Base.write("C://Users//Ajefferi//Documents//$file",stat)

end



println("\nWaste Toner Percentage Reductino by Bottle (January to November)")
for (key,val) in bottleReductions

    value = 100 * round((-1 * val),digits=3)

    println(" $key : %$(value)")
    ##


end

println("\nCost Reductions per bottle: ")
for (key,val) in costReductions

    value =  round((-1 * val),digits=3)

    println(" $key : \$$(value)")



end





tonerBottles,percCostReductions,bottleReductions,percentNov,PercentJan,diff,percentCostNov,PercentCostJan,perCostDiff = outputResults(sh)


df = DataFrame(Bottle=tonerBottles,PercentResidualTonerDecrease=diffs,CostReductionsPerBottle=costPercDiffs)

df.PercentResidualTonerDecrease = round.(df.PercentResidualTonerDecrease,digits =3) .* -100

df.CostReductionsPerBottle = round.(df.CostReductionsPerBottle,digits=2) .*-1


@show df

##output all the other relevant Statistics

CSV.write("C://Users//Ajefferi//Documents//CroxleyResults",df)
println("Percentage Waste toner usage\n")

println("January: ", PercentJan)

println("Novemeber: ", percentNov)

println("Difference: ",diff )


println("Associated Costs\n")

println("January: ", PercentCostJan)

println("Novemeber: ", percentCostNov)

println("Difference: ", percCostDiffs)
