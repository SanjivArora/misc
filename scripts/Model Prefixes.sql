
select Prefix, Model
from Ricoh_SAS.dbo.CarboNZero_Energy e
left join (
select distinct model, substring(serialno,1,3) as Modf
from DataWarehouse.dbo.MIFDataDetails
where MIFDate >= DATEADD(month,-3,getdate())
group by model, substring(serialno,1,3)) v on e.Prefix = v.Modf
where Model is not null
order by Prefix