using ODBC,StatsPlots,DataFrames,Statistics,StatsBase,ScikitLearn,VegaLite


#using ScikitLearn.CrossValidation: cross_val_score,cross_val_predict

##this program aims to look at the characteristic volume evolutions
#of each serial of every model In an attempt to get a more accurate
# gauge on the Running Costs of Models
#@sk_import linear_model: LinearRegression



conn = ODBC.Connection("ricohnz-db2")

query = """

with t1 as (
SELECT mm.Serial,isColour,Model,totalBlk,totalCOL,totalVol,totalTonerCost,case when totalCol = 0 then totalServiceCost else 0.2 * totalServiceCost end as totalBlackCost,case when totalCol = 0 then 0 else 0.8 * totalServiceCost end as totalColourCost,totalServiceCost

  FROM [Ricoh_SAS].[dbo].[MachineMileageTimeline] mt join Ricoh_SAS.pbi.Machine_Master mm
  on mm.Serial = mt.Serial)

  select *,totalBlackCost/nullif(totalBlk,0) as BlackCPC,totalColourCost/nullif(totalCOL,0) as colourCPC from t1
"""

df = DBInterface.execute(conn,query)|> DataFrame



function Evolution(df,model)

    df = df[df.Model .== model,:]
    serials = unique(df.Serial)
    #println("Excluding Serials")
    #@show sizeof(df)
    removals = removeSerials(df)
    #@show sizeof(df)
    df = df[df.Serial .∉ Ref(removals),:]
    #@show sizeof(df)
    #println("finding exceptions")

    @time outliers = IsolateOutliers(df,serials)
    if length(outliers) == 0
        outp_black,outp_colour,outblackCPCs,outcolourCPCs = [0],[0],[0],[0]
        @goto label6
    end
    outlierdf = df[df.Serial .∈ Ref(outliers),:]
    #println("Outputting Outliers")
    outp_black = OutputValues(outlierdf,:totalBlk,:totalBlackCost)
    outp_colour = OutputValues(outlierdf,:totalCOL,:totalColourCost)

    outblackCPCs,outcolourCPCs = retrieveCPCS(model,outliers)

    df = df[df.Serial .∉ Ref(outliers),:]

    @label label6

    #@show sizeof(df)
    #println("Sorting by Serial")
    sort!(df,[:Serial,:totalVol])
    #@show sizeof(df)
    ##reshape for the evolution of each serial
    #println("Outputting Values")

    p_col = OutputValues(df,:totalBlk,:totalBlackCost)
    p_blk = OutputValues(df,:totalCOL,:totalColourCost)

    blackCPCs,colourCPCs = retrieveCPCS(model,unique(df.Serial))



    return p_col,p_blk,blackCPCs,colourCPCs,outp_colour,outp_black,outblackCPCs,outcolourCPCs

end


function OutputValues(df,blackorColourVol::Symbol,blackColServiceCost::Symbol)

    #println("Pivoting...")
    newdf = unstack(df,blackorColourVol,:Serial,blackColServiceCost)
    xs = newdf[blackorColourVol]
    #println("Checking size of xs..")
    if length(xs) == 0
        println("No values found")
        return plot(),[0],[0]
    end

    #println("Checking size of df..")
    if sizeof(df)[1] == 0
            println("No values found")
            return plot(),[0],[0]
    end
    #println("Isolating ys...")
    ys = newdf[:,2:end]


    serials = names(ys)
    if length(serials) == 0
        return plot(),[0],[0]
    end
    #@show serials
    #println("Converting to matrix ...")
    ys = Matrix(ys)
    p1 = plot(xs,ys,xlabel = "$(string(blackorColourVol))",hover = serials,m=:o,ylabel= "$(string(blackColServiceCost))",leg=:false)

    #println("calculating CPCs")

    ##BlackCPs,ColourCPCs = retrieveCPCs(model,Blacklist)

    ##max_blackCPCs = [df[df.Serial .== serial,:].BlackCPC[end] for serial in unique(df.Serial)]
    ##max_colourCPCs = [df[df.Serial .== serial,:].colourCPC[end] for serial in unique(df.Serial)]









    return p1

end


function findOutliers(arr)
    ##metod1
    #@show arr
    outlierPos = []
    println(length(arr))
    subarr = arr[arr .> 0]
    if length(subarr) == 0
        return
    end
    q1 = quantile(subarr,0.25)
    q3 = quantile(subarr,0.75)
    iqr = q3 - q1
    UP = q3 + 1.5 * iqr
    LB = q1 - 1.5 * iqr
    for i in 1:length(arr)
        if arr[i] >= UP || arr[i] <= LB
            push!(outlierPos,i)
        end
    end
    return outlierPos
end


function findOutliers2(arr,threshold)
    ##metod1
    ##@show arr
    outlierPos = []
    println(length(arr))
    subarr = arr[arr .> 0]
    if length(subarr) == 0
        return
    end
    z = abs.(StatsBase.zscore(arr))

    for i in 1:length(arr)
        if z[i] >= threshold
            push!(outlierPos,i)
        end
    end
    return outlierPos
end

function findOutliers3(x,y)
    outlierPos = []
    if length(x) > 10 && length(y) > 10

        x = Vector{Float64}(x)
        y = Vector{Float64}(y)
        #@show x

        #@show y


        ##use linear Regression to Identify your outliers
        #
        #display(histogram(x))
        #display(histogram(Y))
        #lm = fit!(Lasso(),x,y)


        if length(unique(x)) == 1 && unique(x)[1] == 0.0
            return outlierPos
        end


        if length(y) == 0
            return outlierPos
        end


        x2 = [i for i in 1:length(x)]
        data = hcat(x2,x)



        preds = cross_val_predict(LinearRegression(),data,y,cv=10)

        residuals =  y .- preds
        stdRes = std(residuals)
        #isOutlier = (Y .> (preds .+ 2.5 .* stdRes)) .& (Y .< (preds .- 2.5 .* stdRes))
        ##returned indexes of outliers

        for i in 1:length(y)
            if y[i] >= (preds[i] + 2 * stdRes) || y[i] <= (preds[i] - 2 * stdRes)
                push!(outlierPos,i)
            end
        end
        return outlierPos




        #println("Linear Model Built")

        ##finding Outliers


    else return outlierPos

    end

end




function centreSpread1(CPCs)

    ##plain average
    centre = Statistics.mean(CPCs)
    stds = Statistics.stdm(CPCs,centre)
    return stds,centre
end



function centreSpread2(arr)

    ## average excluding outliers method 1
    testdf = DataFrame(vals=arr)
    dropmissing!(testdf)
    arr = testdf.vals
    subarr = arr[arr .> 0]
    norms = []
    if length(subarr) == 0
        return
    end
    z = abs.(StatsBase.zscore(arr))

    for i in 1:length(arr)
        if z[i] <= 3
            push!(norms,arr[i])
        end
    end

    if length(norms) == 0
        return
    end


    centre = Statistics.mean(norms)
    stds = Statistics.stdm(norms,centre)


    return stds,centre



end




function centreSpread3(CPCs)

    ## Median
    centre = 100 * median(skipmissing(CPCs))
    IQR = 100 * iqr(skipmissing(CPCs))


    return IQR,centre

end




##add a line of best fit



function modelEvolutionCPCs(df,model)


	p1blk,p1col,BlackCPCs,ColourCPCs,outpblk,outpcol,OutsBlackCPC,OutsColourCPC = Evolution(df,model)

	##outPutBlack CPCs then blackOutliers with error handling
	avgBlackCPCs = 0.0
	stdBlackCPCs = 0.0
	avgBlackOutCPCs = 0.0
	stdBlackOutCPCs = 0.0

	avgColourCPCs = 0.0
	stdColourCPCs = 0.0
	avgColourOutCPCs = 0.0
	stdColourOutCPCs = 0.0


	missingBool = ismissing.(BlackCPCs)
	if length(missingBool[missingBool .== 0]) == 0 ##all of the values are "missing"
		return

	else




		    avgBlackCPCs,stdBlackCPCs = centreSpread3(BlackCPCs)

        	plot!(p1blk,title="$model Black")
        	savefig(p1blk,"C://Users//Ajefferi//Documents//Model Lifetime Analysis//$model Black")

		##black Outliers
        @show outpblk
		if outpblk == [0] ##NO outliers
            		@goto toColour
        end


		missingBool = ismissing.(OutsBlackCPC)
		if length(missingBool[missingBool .== 0]) == 0 ##all of the values are "missing"
			@goto toColour

		else
			avgBlackOutCPCs,stdBlackOutCPCs = centreSpread3(OutsBlackCPC)

			plot!(outpblk,title="$model Black")
        	savefig(outpblk,"C://Users//Ajefferi//Documents//Model Lifetime Analysis//Outliers//$model Black")
		end
        end
	@label toColour
	missingBool = ismissing.(ColourCPCs)
	if length(missingBool[missingBool .== 0]) == 0 #all of the values are "missing"
		return
	else
        avgColourCPCs,stdColourCPCs = centreSpread3(ColourCPCs)

    	plot!(p1col,title="$model Colour")
    	savefig(p1col,"C://Users//Ajefferi//Documents//Model Lifetime Analysis//$model Colour")
		##black Outliers
        #@show outpcol
		if outpcol == [0] ##NO outliers
            return
        end


		missingBool = ismissing.(OutsColourCPC)
		if length(missingBool[missingBool .== 0]) == 0 ##all of the values are "missing"
			return

		else

			avgColourOutCPCs,stdColourOutCPCs = centreSpread3(OutsColourCPC)

			plot!(outpcol,title="$model Colour")
        	savefig(outpcol,"C://Users//Ajefferi//Documents//Model Lifetime Analysis//Outliers//$model Colour")

		end

        end

	##outPutCOlour CPCs then COlourOutliers with error handling

	#print the file with Both the Black CPCs,ColourCPCs,and Outlier CPCs


	outtext = "\nModel: $model\n Estimated Black CPC: $avgBlackCPCs \n Black Spread: $stdBlackCPCs \n EstimatedColourCPC: $avgColourCPCs \n Colour Spread: $stdColourCPCs"

	out2text = "\nModel: $model\n Estimated Black CPC Outliers: $avgBlackOutCPCs \n  Black Spread Outliers: $stdBlackOutCPCs \n EstimatedColourCPC Outliers: $avgColourOutCPCs \n Colour Spread Outliers: $stdColourOutCPCs"

	outputText = outtext  * "\n Outliers: " * out2text

	open("C://Users//Ajefferi//Documents//Model Lifetime Analysis//OutCPCs.txt","a") do io
	write(io, outputText )end

end


function filetoDF(n)
    data = readlines("C://Users//Ajefferi//Documents//Model Lifetime Analysis//OutCPCs.txt")
    data = string.(data)
    #number of Models
    dataMat = Array{Any}(nothing, (n,5))
    outlierMat = Array{Any}(nothing, (n,5))
    outlierBool = false
    ns = []
    i=1
    j = 1
    for line in data

        @show line
        if line == ""
            continue

        else
            @show name,value = split(line,": ")

            if name == " Outliers"
                outlierBool = true
                j += 1
            end
            if outlierBool == true
                if name == "Model"
                    dataMat[CartesianIndex(j,1)] = value
                elseif name == " Estimated Black CPC"
                    dataMat[CartesianIndex(j,2)] = value
                elseif name == " Black Spread"
                    dataMat[CartesianIndex(j,3)] = value
                elseif name == " EstimatedColourCPC"
                    dataMat[CartesianIndex(j,4)] = value
                elseif name == " Colour Spread"
                    dataMat[CartesianIndex(j,5)] = value
                end
                outlierBool = false

            else
                if name == "Model"
                    dataMat[CartesianIndex(j,1)] = value
                elseif name == " Estimated Black CPC"
                    dataMat[CartesianIndex(j,2)] = value
                elseif name == " Black Spread"
                    dataMat[CartesianIndex(j,3)] = value
                elseif name == " EstimatedColourCPC"
                    dataMat[CartesianIndex(j,4)] = value
                elseif name == " Colour Spread"
                    dataMat[CartesianIndex(j,5)] = value
                end

            end

        end
        i += 1
    end

    df = DataFrame(dataMat)
    @show df
    rename!(df,[:Model,:BlackCPC,:BlackSpread,:ColourCPC,:ColourSpread])
    outlierdf = DataFrame(outlierMat)

    rename!(outlierdf,[:Model,:BlackCPC,:BlackSpread,:ColourCPC,:ColourSpread])

    return df,outlierdf

end


function runProgram()
    i = 0

    file = open("C://Users//Ajefferi//Documents//Model Lifetime Analysis//OutCPCs.txt","w")
    write(file,"")
    close(file)
    models = sort(unique(df.Model),rev=true)
    for model in models
        i += 1
        try
            println("Calculating For Model $model, $i of $(length(unique(df.Model)))")
            modelEvolutionCPCs(df,model)
            println("Model Evolution Completed")
        catch e
            if isa(e,typeof(InterruptException()))
                return
            else
                println("$e Exception Occurred")
                #@show stacktrace(catch_backtrace())
                #@show catch_backtrace()
                continue
            end
        end
    end
end


function runProgram2(df)
    i = 0
    file = open("C://Users//Ajefferi//Documents//Model Lifetime Analysis//OutCPCs.txt","w")
    write(file,"")
    close(file)
    models = unique(df.Model)

    modelSubset = models

    for model in modelSubset
        try
            i += 1
            println("Calculating For Model $model, $i of $(length(unique(df.Model)))")
            @time modelEvolutionCPCs(df,model)
            println("Model Evolution Completed")
        catch e
            if isa(e,typeof(InterruptException()))
                return
            else
                println("$e Exception Occurred")
                #@show stacktrace(catch_backtrace())
                #@show catch_backtrace()
                continue
            end
        end
    end

    newdf,Outdf = filetoDF(i)
    return newdf,Outdf
end

#@time runProgram()




##scatter Plot of CPC evaluations for each Model

function runProgram3(df,field1,field2,blackcol)
    i = 0
    #file = open("C://Users//Ajefferi//Documents//Model Lifetime Analysis//Scatterplots","w")
    #write(file,"")
    #close(file)
    models = unique(df.Model)
    modelSubset = models


    for model in modelSubset
        try
            i += 1
            iscolour = false

            println("Plotting For Model $model, $i of $(length(unique(df.Model)))")

            modeldf = df[df.Model .== model,:]


            if blackcol == "Black"
                outstring = "$model Black"

            elseif blackcol == "Colour"
                outstring = "$model Black"
                iscolour = true
            else
                error("Retry with different string inputs")
            end

            ##vegalite plotting
            ##plotModelNice(modeldf,field1,field2,outstring)

            ###set increments

            increment = 10000

            ##@show n_increm = modeldf.totalBlk[end] / increment
            ##calcalate the gradient changes for each of the CPCs
            println("GradientCalcs..")

            gradients = gradientCalcs(modeldf,increment,iscolour)


            #println("Unpivoting...")

            ##potting gradients


            #@show head(gradients)


            unpivoted = stack(gradients,Not(:increments))

            #@show size(unpivoted)

            #@show size(modeldf)

            #@show head(unpivoted)


            #println("Removing Missing Rows")


            unpivoted = unpivoted[unpivoted.value .> 0.0,:]

            @show size(unpivoted)
            #@show head(unpivoted)


            #println("Plotting ...")

            p = plot(unpivoted.increments,unpivoted.value,m=:o,xlabel = "Volume Ranges ",ylabel= "Gradients")

            title!("Gradient Values for $model")
            #println("Plot Built")

            savefig(p,"C://Users//Ajefferi//Documents//Model Lifetime Analysis//Gradients//$model Gradients.html")











        catch e
            if isa(e,typeof(InterruptException()))
                return
            else
                println("$e Exception Occurred")
                #@show stacktrace(catch_backtrace())
                #@show catch_backtrace()
                return
            end

        end
    end

end





function plotModelNice(modeldf::DataFrame,field1::Symbol,field2::Symbol,outstring::String)
            p = modeldf |>
            @vlplot(
            transform=[{filter="datum.totalVol >= 100000"}],

            :line,
            selection={
                brush={
                    type=:interval,
                    resolve=:union,
                    on="[mousedown[event.shiftKey], window:mouseup] > window:mousemove!",
                    translate="[mousedown[event.shiftKey], window:mouseup] > window:mousemove!",
                    zoom="wheel![event.shiftKey]"
                },
                grid={
                    type=:interval,
                    resolve=:global,
                    bind=:scales,
                    translate="[mousedown[!event.shiftKey], window:mouseup] > window:mousemove!",
                    zoom="wheel![!event.shiftKey]"
                }
            },
            x=field1,
            y=field2,

            color = :Serial,
            width = 400,
            height = 500)



            #println("Plot Generated")

            VegaLite.save("$outstring.html",p)

            println("Model Scatter Saved")


end



function gradientCalcs(df::DataFrame,increment::Int,iscolour::Bool)
    ## calcualte the gradient changes for each CPC for every
    #println("gradients...")
    serials = unique(df.Serial)
    gradientMat = zeros(increment,length(serials))
   # @show length(gradientMat)
    model = df.Model[1]
    for serial in serials
        serialIndex = findfirst(x->x==serial,serials)

        if mod(serialIndex,50) == 0
            println("$serialIndex of $(length(serials))")

        end

        subdf = df[df.Serial .== serial,:]
        if iscolour == false
            maxBlk = subdf.totalBlk[end]
            global n_increm = maxBlk / increment
            global increments = 1:increment:maxBlk

           ## @show increments = Vector{Int}(increments)

            #println(" gradients for each serial...")
            gradients = gradientCalc(increments,subdf.totalBlk,subdf.BlackCPC)

            #@show length(gradients)

            #@show gradients


            [gradientMat[CartesianIndex(j,serialIndex)] = gradients[j] for j in 1:length(gradients)]





        else
            println(" Colour...")
            ##now write for colour


        end


    end


    df = DataFrame(gradientMat)

    rename!(df,Symbol.(serials))

    #@show df[1:5,1:5]



    rownames = [i * increment for i in 1:increment]


    outdf = hcat(DataFrame(increments=rownames),df)



    return  outdf
end

using IterTools

function gradientCalc(increments,vols,CPCs)
    grads = []
    for pair in collect(IterTools.partition(collect(increments),2,1))
        partVols = []
        partCPCs = []
        s,e = pair[1],pair[2]
        #@show pair
        for i in 1:length(vols)
            #@show vols[i]

            if vols[i] >= s
                if vols[i] > e
                    continue
                end
                push!(partVols,vols[i])
                push!(partCPCs,CPCs[i])

            end
        end

        if partVols == []
            push!(grads,0.0)


            continue

        elseif partCPCs == []
            push!(grads,0.0)

        end

        ΔX = partVols[end] - partVols[1]
        ΔY = partCPCs[end] - partCPCs[1]
        grad = ΔX/ΔY

        push!(grads,grad)
    end
   # println("grads iterated...")

    ##return a list of gradients for each of the Serals

    return grads

end







runProgram3(df,:totalBlk,:BlackCPC,"Black")
