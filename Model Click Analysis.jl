using DataFrames,ODBC,Statistics,StatsPlots,Peaks,LsqFit,Polynomials,Optim

using StatsBase
conn = ODBC.Connection("ricohnz-db2")

query = """select Pas.*,mm.Model_type,iscolour,Customer from

(select Mth,Serial,Model,Account,BlackVol,ColourVol,BlackVol + ColourVol as TotalVol,Labour,Dealer,Parts,FreetonerCost,ServiceCost from RICOH_SAS.Pbi.Profitablity_AfterSales ) pas


join Ricoh_SAS.pbi.CUstomer_master cm on pas.account = cm.Account


join RICOH_SAS.pbi.Machine_master mm on mm.Serial = pas.Serial


where cm.Branch != 'ZZ' and cm.division_DWR != 'Internal'
"""

df = DBInterface.execute(conn,query) |> DataFrame

filterdf(df::DataFrame,field::Symbol,value) = df[df[field] .== value,:]


function play(modelSubset,stepsize = 10)

    for modelName in modelSubset
        try
            p = plot()
            modeldf = df[df.Model .== modelName,:]
            M = calcMaxCumVols(modeldf)
            println("Max Volume Count Found")
            n = findN(collect(1:M))


            brackets = collect(1:n:M)
            bracketList = [[[0.0],[0.0],[0.0]] for i in brackets]
            labels = ["Toner","Parts","Labour"]



            countsss = 0
            println("Iterate  through Serials")
            for serial in unique(modeldf.Serial)

                serialdf = df[df.Serial .== serial,:]
                sort!(serialdf,:Mth)
                #check sort order
                if serialdf.Mth[1] > serialdf.Mth[end]
                    println("Reverse the sort order ")
                    break
                end


                serialdf.cumVol = cumsum(serialdf.TotalVol)
                serialdf.cumToner = cumsum(serialdf.FreetonerCost)
                serialdf.cumParts = cumsum(serialdf.Parts)
                serialdf.cumLabour = cumsum(serialdf.Labour)
                #serialdf.cumBlk = cumsum(serialdf.BlackVol)
                #serialdf.cumCol = cumsum(serialdf.ColourVol)
                [assignToBracket(i,brackets,bracketList,serialdf) for i in 1:length(serialdf.cumVol)]
                #println("assigned all values of serial")
                countsss += 1
                mod(countsss,500) == 0  ? println("assigned $countsss Serrials of $(length(unique(modeldf.Serial)))") : dononthing= true

            end

            #display(p)
            println("all Serials Assinged")

             toner = [item[1] for item in bracketList]
             ##@show bracketList


            labour = [item[2] for item in bracketList]
            parts = [item[3] for item in bracketList]


            println("Averaging...")
            AvgLabour = [mean(item) for item in labour]
            AvgToner = [mean(item) for item in toner]
            AvgParts = [mean(item) for item in parts]
            stdLabour = [std(item) for item in labour]
            stdParts = [std(item) for item in parts]
            stdToner = [std(item) for item in toner]

            println("Plotting Graph ...")

            p=plot()
            ##savefig(p,"H://Model Plots//ClickAnalysis//$modelName")


            title!("Costs against Clicks for $modelName ")
            scatter!(p,brackets,AvgToner,yerr=stdToner,label = "avgToner")
            savefig(p,"H:/Model Plots/ModelBreakdowns/$modelName Toner")

            p = plot()
            title!("Costs against Clicks for $modelName ")
            scatter!(p,brackets,AvgLabour,yerr=stdLabour,label = "avgLabour")
            savefig(p,"H:/Model Plots/ModelBreakdowns/$modelName Labour")

            p = plot()
            title!("Costs against Clicks for $modelName ")
            scatter!(p,brackets,AvgParts,yerr=stdParts,label = "avgParts")
            savefig(p,"H:/Model Plots/ModelBreakdowns/$modelName Parts")




        catch e
            println(e)
            if e == InterruptException()
                break
            else
                continue
        end
    end
end

end

    ##return lengths





function assignToBracket(i,brackets,bracketList,df)
    value = df.cumVol[i]
    pairings = collect(IterTools.partition(brackets,2,1))

    for j in 1:length(pairings)
        pair = pairings[j]

        if value > pair[1] && value <= pair[2]
            ##first try with CUmlativeParts toner labour
            tonerVal = df.cumToner[i]
            partsVal = df.cumParts[i]
            labourVal = df.cumLabour[i]
            push!(bracketList[j][1],tonerVal)
            push!(bracketList[j][2],partsVal)
            push!(bracketList[j][3],labourVal)

            return



        end

    end

    ##push the value onto
end

using IterTools

function findN(list)
    ##obtain an even step size from range

    stepChosen = 0
    stepSizes = [1000,2000,5000,10000,15000,20000,250000,3000000,40000,50000,100000,150000,200000,500000,1000000]
    for stepSize in stepSizes
        nsteps = maximum(list) / stepSize
        if nsteps <= 600
            println("using step size of $nsteps")
            stepChosen = stepSize
            break
        end
    end

    return stepChosen

end





function calcMaxCumVols(df)
    serials = unique(df.Serial)
    maxCum = 0
    i = 0
    for serial in serials
        i += 1
        #println("data for serial: $serial")
        serialdf = filterdf(df,:Serial,serial)
        serialdf = select(serialdf,[:TotalVol,:Serial,:Mth])

        cumVol = cumsum(serialdf.TotalVol)
        proposed = maximum(cumVol)
        if proposed > maxCum
            maxCum  = proposed
        end
        if mod(i,100) == 0

            println("$i of $(length(serials))")

        end
    end
    return maxCum

end


n= 1
modelSubset = sort!(unique(df.Model))
play(modelSubset)

function cumulativeMod(df)
    modelsdf = []
    j = 0
    for model in unique(df.Model)
        j += 1
        i = 0
        try
            modeldf = filterdf(df,:Model,model)
            serialdfs = []
            for serial in unique(modeldf.Serial)
                try
                serialdf = df[df.Serial .== serial,:]
                sort!(serialdf,:Mth)
                #check sort order
                i += 1
                if mod(i,100) == 0
                    println("$i of $(length(unique(modeldf.Serial))) Serials")
                end
                serialdf.cumVol = cumsum(serialdf.TotalVol)
                serialdf.cumToner = cumsum(serialdf.FreetonerCost)
                serialdf.cumParts = cumsum(serialdf.Parts)
                serialdf.cumLabour = cumsum(serialdf.Labour)
                select!(serialdf,:Customer,:iscolour,:Model_type,:Mth,:Model,:Serial,:cumVol,:cumParts,:cumLabour,:cumToner)
                push!(serialdfs,serialdf)
            catch e
                println(e)
                continue
            end
            end
        catch e
            println(e)
        end
        if mod(j,10) == 0
            println(" Calculated $ijof $(length(unique(df.Model))) Models")
        end
        allserials = vcat(reduce,serialdfs)
        push!(modelsdf,allserials)
    end
    allmodelsdf = vcat(reduce,modelsdf)
    ## now correlate
    return allmodelsdf
end


##quick Machine Agae Profit Analysis
function analyseByCategory(df,field::Symbol)
    corToner =[]
    corParts =[]
    corLabour = []
    println("Correlating for each $field ")
    for item in unique(df[field])

        filtered = filterdf(df,field,item)
        push!(corToner,cor(df.cumVol,df.cumToner))
        push!(corParts,cor(df.cumVol,df.cumParts))
        push!(corLabour,cor(df.cumVol,df.cumLabour))
        println("DOne for $item")
    end
    cordf = Dataframe(cat=unique(df[field]),TC= corToner,PC = corParts,LC = corLabour )
    return cordf
end

##attempt to analyse by category
newdf = cumulativeMod(df)
catsdf = []
for category in [:Model,:Customer,:iscolour,:Model_type,:Model]
    catdf = analyseByCategory(newdf,category)
    push!(catsdf,catdf)

end

finaldf = reduce(vcat,catsdf)
