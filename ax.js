br_os.ls01_brazil_spectrum0907__bra369324

drop table if exists ywx913685.brazil_broadband_ls;
create table ywx913685.brazil_broadband_ls as 
SELECT 
a.carrier_name,
b.area_level1_name,
spectrum_type,
spectrum_count,
area_level,
carrier_rat,
carrier_code,
area_level1_index
from br_os.ls01_brazil_spectrum0907__bra369324 as a 
LEFT JOIN br_os.ls01_brazil_area_level_1_index0824__bra367592 as b
ON a.area_level1_abb = b.area_level1_abb

drop table if exists ywx913685.brazil_broadband_as;
create table ywx913685.brazil_broadband_as as 

select ad.carrier_name,bx.carrier_code,ad.area_level1_name from (
    select * ,row_number() over (partition by area_level1_name order by max_name) as cs from (
        select ax.area_level1_name,ax.carrier_name,SUM(ax.spectrum_count) AS max_name from 
        ywx913685.brazil_broadband_ls AS ax
        where ax.spectrum_type='2100M'
        GROUP BY ax.area_level1_name,ax.carrier_name
ORDER BY ax.area_level1_name) ac) ad
left join (select carrier_code,carrier_name from ywx913685.brazil_broadband_ls group by carrier_name,carrier_code) as bx on ad.carrier_name = bx.carrier_name
where ad.cs=4

select ad.carrier_name,bx.carrier_code,ad.area_level1_name from (
    select * ,row_number() over (partition by area_level1_name order by max_name) as cs from (
        select ax.area_level1_name,ax.carrier_name,SUM(ax.spectrum_count) AS max_name from 
        ywx913685.brazil_broadband_ls AS ax
        where ax.spectrum_type='700M'
        GROUP BY ax.area_level1_name,ax.carrier_name
ORDER BY ax.area_level1_name) ac) ad
left join (select carrier_code,carrier_name from ywx913685.brazil_broadband_ls group by carrier_name,carrier_code) as bx on ad.carrier_name = bx.carrier_name
where ad.cs=4





drop table if exists ywx913685.brazil_broadband_xs;
create table ywx913685.brazil_broadband_xs as 

select a.carrier_name,a.area_level1_name,a.spectrum_type,a.spectrum_count,a.area_level,a.carrier_rat,a.carrier_code,a.area_level1_index,
b.carrier_code as 'm700',c.carrier_code as 'm850',d.carrier_code as 'm1800',e.carrier_code as 'm2100',f.carrier_code as 'm2600',g.carrier_code as 'mall'
from ywx913685.brazil_broadband_ls a
left join (
    select ad.carrier_name,bx.carrier_code,ad.area_level1_name from (
        select * ,row_number() over (partition by area_level1_name order by max_name) as cs from (
            select ax.area_level1_name,ax.carrier_name,SUM(ax.spectrum_count) AS max_name from 
            ywx913685.brazil_broadband_ls AS ax
            where ax.spectrum_type='700M'
            GROUP BY ax.area_level1_name,ax.carrier_name
    ORDER BY ax.area_level1_name) ac) ad
    left join (select carrier_code,carrier_name from ywx913685.brazil_broadband_ls group by carrier_name,carrier_code) as bx on ad.carrier_name = bx.carrier_name
    where ad.cs=4
) b on a.area_level1_name=b.area_level1_name

left join (
    select ad.carrier_name,bx.carrier_code,ad.area_level1_name from (
        select * ,row_number() over (partition by area_level1_name order by max_name) as cs from (
            select ax.area_level1_name,ax.carrier_name,SUM(ax.spectrum_count) AS max_name from 
            ywx913685.brazil_broadband_ls AS ax
            where ax.spectrum_type='850M'
            GROUP BY ax.area_level1_name,ax.carrier_name
    ORDER BY ax.area_level1_name) ac) ad
    left join (select carrier_code,carrier_name from ywx913685.brazil_broadband_ls group by carrier_name,carrier_code) as bx on ad.carrier_name = bx.carrier_name
    where ad.cs=4
) c on a.area_level1_name=c.area_level1_name

left join (
    select ad.carrier_name,bx.carrier_code,ad.area_level1_name from (
        select * ,row_number() over (partition by area_level1_name order by max_name) as cs from (
            select ax.area_level1_name,ax.carrier_name,SUM(ax.spectrum_count) AS max_name from 
            ywx913685.brazil_broadband_ls AS ax
            where ax.spectrum_type='1800M'
            GROUP BY ax.area_level1_name,ax.carrier_name
    ORDER BY ax.area_level1_name) ac) ad
    left join (select carrier_code,carrier_name from ywx913685.brazil_broadband_ls group by carrier_name,carrier_code) as bx on ad.carrier_name = bx.carrier_name
    where ad.cs=4
) d on a.area_level1_name=d.area_level1_name

left join (
    select ad.carrier_name,bx.carrier_code,ad.area_level1_name from (
        select * ,row_number() over (partition by area_level1_name order by max_name) as cs from (
            select ax.area_level1_name,ax.carrier_name,SUM(ax.spectrum_count) AS max_name from 
            ywx913685.brazil_broadband_ls AS ax
            where ax.spectrum_type='2100M'
            GROUP BY ax.area_level1_name,ax.carrier_name
    ORDER BY ax.area_level1_name) ac) ad
    left join (select carrier_code,carrier_name from ywx913685.brazil_broadband_ls group by carrier_name,carrier_code) as bx on ad.carrier_name = bx.carrier_name
    where ad.cs=4
) e on a.area_level1_name=e.area_level1_name

left join (
    select ad.carrier_name,bx.carrier_code,ad.area_level1_name from (
        select * ,row_number() over (partition by area_level1_name order by max_name) as cs from (
            select ax.area_level1_name,ax.carrier_name,SUM(ax.spectrum_count) AS max_name from 
            ywx913685.brazil_broadband_ls AS ax
            where ax.spectrum_type='2600M'
            GROUP BY ax.area_level1_name,ax.carrier_name
    ORDER BY ax.area_level1_name) ac) ad
    left join (select carrier_code,carrier_name from ywx913685.brazil_broadband_ls group by carrier_name,carrier_code) as bx on ad.carrier_name = bx.carrier_name
    where ad.cs=4
) f on a.area_level1_name=f.area_level1_name

left join (
    select ad.carrier_name,bx.carrier_code,ad.area_level1_name from (
        select * ,row_number() over (partition by area_level1_name order by max_name) as cs from (
            select ax.area_level1_name,ax.carrier_name,SUM(ax.spectrum_count) AS max_name from 
            ywx913685.brazil_broadband_ls AS ax
            GROUP BY ax.area_level1_name,ax.carrier_name
    ORDER BY ax.area_level1_name) ac) ad
    left join (select carrier_code,carrier_name from ywx913685.brazil_broadband_ls group by carrier_name,carrier_code) as bx on ad.carrier_name = bx.carrier_name
    where ad.cs=4
) g on a.area_level1_name=g.area_level1_name





drop table if exists ywx913685.brazil_broadband_bs;
create table ywx913685.brazil_broadband_bs as 
select * ,row_number() over (partition by area_level1_name order by max_name) as cs from ywx913685.brazil_broadband_as;

drop table if exists ywx913685.brazil_broadband_cs;
create table ywx913685.brazil_broadband_cs as 
select 
a.carrier_name,a.area_level1_name,a.spectrum_type,a.spectrum_count,a.area_level,a.carrier_rat,a.carrier_code,a.area_level1_index,
b.max_name,b.carrier_name as carrier_name_index
from ywx913685.brazil_broadband_ls a left join ywx913685.brazil_broadband_bs b on cs=4 and a.area_level1_name = b.area_level1_name
