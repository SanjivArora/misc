#Finance Profi
using ODBC,DataFrames,StatsBase,Statistics,Distributions,CSV,XLSX,StatsPlots

conn = ODBC.Connection("ricohnz-db2")



interest_rate = 0.15 ##interest we charge to the customer
α = 1 + interest_rate /12##monthly interest rate

ourInterest = 0.03
β = 1+ ourInterest/12 ##interest rate that we are charged


B_0 = 15000 #cost of goods sold
gst = 0.15 #0.15 before 2020-10-01,0.125 from 1989 - 2010-10 -01
Term = 48
document_fee = 326 ##unknown
MonthlyCharge = 600 ##this is a flat rate accross the board for argments sake however in reality


B1 = 0
equip_budget = B1 + B_0  ##value of used equipment
M1,M2,r1,r2,s1,s2,y = 10000,1000,0.01,0.06,0.01,0.06,1.14
mcc = 0.0053 ##accross the board for the last 12 months
ccc = 0.0372


FR(B_0,T,α,gst = 0.15,document_fee = 0) = document_fee + B_0 * ((gst*T*(α - 1) * α ^ T) / (α ^ T - 1 ) - gst + 1)

SC(msr,csr,mcc,ccc,mv,cv,mr,cr,T) = (T *  mv * mcc * msr /mr) + (T *  cv * ccc * csr /cr)

VC(Term,mr,minv,cr,cv) = Term * (mr * minv + cr*cv)

minSR(mr,cr,mv,cv,T,y=1.14) = 12 * mr  * mv * (y ^ (T/12) -1) / (y-1) + 12 * cr * cv * (y ^ (T/12)) -1  / (y-1)

FC(B_0,T,β,discount=1) = B_0 * ((discount * T * (β ^ T) * (β - 1)) / (β ^ T - 1))


SCx(msr,csr,mcc,ccc,mv,cv,mr,cr,x) = (x *  mv * mcc * msr /mr) + (x *  cv * ccc * csr /cr)

VCx(x,mr,minv,cr,cv) = x * (mr * minv + cr*cv)


minSRx(mr,cr,mv,cv,x,y=1.14) = 12 * mr  * mv * (y ^ (x÷12) -1) / (y-1) + 12 * cr * cv * (y ^ (x÷12)) -1  / (y-1)

using NPFinancial

##cost of interest + Principal at x months

function FCx(B_0,T,β,x,discount=1)
     interests = -1 * sum(ipmt.(β-1,[i for i in 1:T],T,B_0 * discount)[1:x])
     principals = -1 * sum(ppmt.(β-1,[i for i in 1:T],T,B_0 * discount)[1:x])
     return interests + principals
 end

##
calc_equip_budget(T,monthly_charge,gst,document_fee,α) = (T * (monthly_charge - (gst * document_fee / T)) * (1 - (1/(α ^T))))/ ((gst/gst-1) * (α -1) * T -(1- 1/(α ^T)) *(gst -1))
budget = calc_equip_budget(Term,MonthlyCharge,gst,document_fee,α)

equipBudgetLessCOGS = budget - B_0

##here we have the cos of Goods Sold being representative of the







function calc_COGS


end

function calcProf(B0,Term,α,β,MonthlyCharge,M1,M2,r1,r2,s1,s2,y,mcc,ccc)
    #=

    ##should be around 18,000
    @show VC(Term,r1,M1,r2,M2)

    @show FC(B_0,Term,β)

    @show SC(s1,s2,mcc,ccc,M1,M2,r1,r2,Term)

    @show minSR(r1,r2,M1,M2,Term,y)

    @show  Term * MonthlyCharge

    @show FR(B_0,Term,α,gst,326)
    =#

    total_cost = FC(B_0,Term,β) + SC(s1,s2,mcc,ccc,M1,M2,r1,r2,Term) + VC(Term,r1,M1,r2,M2)

    total_rev =  minSR(r1,r2,M1,M2,Term,y)  + Term * MonthlyCharge ## + FR(B_0,Term,α,gst,326) ##finance revenue already taken into account

    ##much closer
    total_profit = total_rev - total_cost

    return total_rev,total_cost,total_profit
end

function calcProfAtx(B0,Term,x,α,β,MonthlyCharge,M1,M2,r1,r2,s1,s2,y,mcc,ccc)
    #=

    ##should be around 18,000
    @show VC(Term,r1,M1,r2,M2)

    show FC(B_0,Term,β)

    @show SC(s1,s2,mcc,ccc,M1,M2,r1,r2,Term)

    @show minSR(r1,r2,M1,M2,Term,y)

    @show  Term * MonthlyCharge

    @show FR(B_0,Term,α,gst,326)
    =#
    FCx(B_0,Term,β,x) ##cost of interest



    total_cost = FCx(B_0,Term,β,x) + SCx(s1,s2,mcc,ccc,M1,M2,r1,r2,x) + VCx(x,r1,M1,r2,M2)

    total_rev =  minSRx(r1,r2,M1,M2,x,y)  + x * MonthlyCharge ## + FR(B_0,Term,α,gst,326) ##finance revenue already taken into account

    ##much closer
    total_profit = total_rev - total_cost

    return total_rev,total_cost,total_profit
end




B_0 = 15000

prof1 = calcProf(B_0,Term,α,β,MonthlyCharge,M1,M2,r1,r2,s1,s2,y,mcc,ccc)

x = 39
prof2 = calcProfAtx(B_0,Term,x,α,β,MonthlyCharge,M1,M2,r1,r2,s1,s2,y,mcc,ccc)

B_0 = 0 ##pure resign

prof2 = calcProf(B_0,α,β,MonthlyCharge,M1,M2,r1,r2,s1,s2,y,mcc,ccc)
prof2 - prof2

B_0 = 15000

r1,r2 = 0.06, 0.08



prof3 = calcProf(B_0,α,β,MonthlyCharge,M1,M2,r1,r2,s1,s2,y,mcc,ccc)



##should be achieving around 11K of profit here
##revenue from finance payments over the term
##over the whole life this does make sense
##service costs, ##lookup the price per coverage for an account
##client printing behaviour, mininmum volumnes, and the excess rate.
##run a bunch of scenarios for this togenerate profit stats for modifyable rates

#running simulations to output both realisitc and ideal parameters fro our model
#obtain Probability Distribtutions for each of our parameters

##





pipe(query) = DBInterface.execute(conn,query) |> DataFrame




##import your CSV file
df = XLSX.readtable("C://Users//Ajefferi//Downloads//CommonTableAccurate.xlsx","Sheet6") |> DataFrame


##volume fixing

df.FirstBlkMin = [vol > 9999999 ? 0 : vol for vol in df.FirstBlkMin]


df.FirstColMin = [vol > 9999999 ? 0 : vol for vol in df.FirstBlkMin]


##explore rate increases


##check comission percentages


















println()

















cp = pipe("SELECT cp.CP_11_SFID  SFID,
	 [CP_01_Term] term
      ,[CP_02_MonthlyLeasePayment] pmt
      ,[CP_04_ExcessMonoRate] exmr
      ,[CP_05_ExcessColourRate] excr

      ,[CP_07_IncludedMonoVolume] mv
      ,[CP_08_IncludedColourVolume] cv

  FROM [Ricoh2SYS21].[dbo].[LF_SF_CopyPlan] cp


  where CP_10_ContractNumber like 'CP%' or CP_10_ContractNumber like 'RC%'")


names(fc)

##obtain black and colour rates
rs = pipe("Select *


 from  ricoh_sas.dbo.SF_Rates sfr

 where sfr.Colour_rate < 999999.0000

 and sfr.SF_ID in

 (select cp.CP_11_SFID  SFID

  FROM [Ricoh2SYS21].[dbo].[LF_SF_CopyPlan] cp


  where CP_10_ContractNumber like 'CP%' or CP_10_ContractNumber like 'RC%')")



##FinanceNoEndSeq
fc = pipe("select * from RIcoh_SAS.dbo.RicohFinance_Contracts where SalesPlan = 'Copy Plan'")

rs = rs[:,[:BlkVol,:ColVol,:ColBook,:Black_Rate,:Colour_rate]]

fc = fc[:,[:Interest,:Principal,:Refinance,:Sales]]

cp = cp[:,[:term,:pmt,:exmr,:excr,:mv,:cv]]


function GenerateStats(df)
    ##create a table of stats for each of the parameters we want
    cols = eachcol(df)
    colStats = []
    titles = []

    for i in 1:length(cols)
        title = names(df)[i]
        col = cols[i]
        d = DataFrame()
        push!(colStats,genColStats(title,col))
        push!(titles,title)


    end
    statdict = Dict(titles .=> colStats)


    return statdict

end


function genColStats(title,data)

    data = skipmissing(data)

    data = collect(data)
    ##obtain weights from probability distribution
    #kde = generateDistribution(title,data)
    ##return dataset
    #probability that a gicen stat for a given vales are "Sane"
    #obtain

    ##here we gather all the stats we wnat e.g. max,in,range,median,mean,std,
    return data

end

function generateDistribution(title,data)
    #println("Showing  stats for $title")
    #display(boxplot(data,title = "$title"))
    U = kde((data))
    #display(violin(data,title ="$title"))
    ##display(histogram(data,title="histogram for $title"))

    return U


end





cpSpecific = GenerateStats(cp)


rateSpecific = GenerateStats(rs)

fcSpecific = GenerateStats(fc)


statdict = Dict(vcat(collect(keys(cpSpecific)),collect(keys(rateSpecific)),collect(keys(fcSpecific))) .=> vcat(collect(values(cpSpecific)),collect(values(rateSpecific)),collect(values(fcSpecific))))

#select a random value based on the existing values in these distributions
statdict
##run mulitple simulations and rank

function SimulateParams(statdict,n,N)
    xs=[]
    #B_0s =[]
    #Terms = []
    αs = []
    #βs = []
    #monthlycharges = []
    m1s = []
    m2s = []
    r1s= []
    r2s= []
    s1s= []
    s2s= []
    profEsts = []
    shortenedProfit = []
    for i in 1:n
        #chooseparameters -> out of the parameters that we want to modify
        ##ideally can do this with existing parameters and then step to suggest changes
        y = 1.14
        mcc = 0.0053 ##accross the board for the last 12 months
        ccc = 0.0372
        B_0 = 15000
        MonthlyCharge = 600
        Term = 48
        ourInterest = 0.03
        β = 1 + ourInterest/12 ##interest rate that we are charged
        B_0 = 15000 #cost of goods sold
        gst = 0.15 #0.15 before 2020-10-01,0.125 from 1989 - 2010-10 -01
        document_fee = 326
        M1,M2 =  10000,1000
        x = rand(collect(1:Term))
        interest_rate = rand(sample(statdict["Interest"],N))

        α = 1 + interest_rate /12##monthly interest rate

        #Term = sample(statdict["term"],N)
        #M1  = rand(sample(statdict["mv"],N))
        #M2  = rand(sample(statdict["cv"],N))
        r1= rand(sample(statdict["Black_Rate"],N))
        r2= rand(sample(statdict["Colour_rate"],N))
        s1= rand(sample(statdict["exmr"],N))
        s2= rand(sample(statdict["excr"],N))
        if (r1 >= s1) || (r2 >= s2)

            continue
        end
        rev,cost,estProf = calcProf(B_0,Term,α,β,MonthlyCharge,M1,M2,r1,r2,s1,s2,y,mcc,ccc)

        rev2,cost2,estProf2 = calcProfAtx(B_0,Term,x,α,β,MonthlyCharge,M1,M2,r1,r2,s1,s2,y,mcc,ccc)
        push!(xs,x)
        push!(αs,interest_rate)
        #βs = []
        #monthlycharges = []
        #push!(m1s,M1)
        #push!(m2s,M2)
        push!(r1s,r1)
        push!(r2s,r2)
        push!(s1s,s1)
        push!(s2s,s2)

        push!(profEsts,estProf)
        push!(shortenedProfit,estProf2)


    end
    profLost = profEsts .- shortenedProfit

    ##we have our fixed v
    df = DataFrame(Term = [Term for i in length(profEsts)],ActualTerm = xs,InterestCharged = αs,MonthlyCharge = 600,BlkRate = r1s,ClrRate = r2s,ExcessMono = s1s,ExcessColour = s2s ,EstimatedProfit = profEsts,profitAtx = shortenedProfit,profitLost = profLost)

    #return the simulated parameters as well as the estimataed profit
    return df


end

N = 1000
n = 1000

out = SimulateParams(statdict,n,N)

sort!(out,:EstimatedProfit,rev=true)



##output simulation values to file
CSV.write("C://Users//Ajefferi//Documents//estimatedprofitstest.csv",out)

##find the probabilities of paramter change within reason


##step to a realistic parameter from an existing parameter
