create view v_genband_cdr as
select 
	connect_datetime as call_datetime,
	clli.carrier_code as carrier,    
	case
		when (terminating_number between '5926590000' and '5926999999') and length(terminating_number) = 10
		or (terminating_number between '5925926590000' and '5925926999999') and length(terminating_number)= 13
		or (terminating_number between '6590000' and '6999999') and length(terminating_number)= 7
		or (terminating_number between '5926000000' and '5926049999') and length(terminating_number)= 10
		or (terminating_number between '5925926000000' and '5925926049999') and length(terminating_number)= 13
		or (terminating_number between '6000000' and '6049999') and length(terminating_number)= 7
		then 'digicel'
		when (terminating_number between '5926090000' and '5926589999') and length(terminating_number)= 10
		or (terminating_number between '5925926090000' and '5925926589999') and length(terminating_number)= 13
		or (terminating_number between '6090000' and '6589999') and length(terminating_number)= 7
		then 'gtt_cell'
		when (substring(terminating_number, 1, 1) <> '6' and length(terminating_number)= 7)
		or (substring(terminating_number, 1, 3) ='592' and substring(terminating_number, 4, 1) <> '6' and length(terminating_number)= 10)
		or (substring(terminating_number, 1, 6) ='592592' and substring(terminating_number, 7, 1) <> '6' and length(terminating_number)= 13)
		then 'gtt_land'
		else 'unknown'
    end as destination, 
    elapsed_time, 
    originating_number, 
    terminating_number
from genband_cdr cdr, dms300_cdr_clli_mappings clli
where 
	elapsed_time > 0
	and cast(substring(trunkid1, 3, 3) as smallint) = clli.clli
	and trunkid1 like '1%'
	and carrier_code <> ''
	and carrier_code <> 'air'
	and terminating_number not between '2532014'and '2532045'