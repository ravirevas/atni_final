create view v_airspan_cdr as
select 
    call_start_datetime as call_datetime,
    carrier,  
    case
		when (called_party_number between '5926590000' and '5926999999') and length(called_party_number) = 10
		or (called_party_number between '5925926590000' and '5925926999999') and length(called_party_number)= 13
		or (called_party_number between '6590000' and '6999999') and length(called_party_number)= 7
		or (called_party_number between '5926000000' and '5926049999') and length(called_party_number)= 10
		or (called_party_number between '5925926000000' and '5925926049999') and length(called_party_number)= 13
		or (called_party_number between '6000000' and '6049999') and length(called_party_number)= 7
		then 'digicel'
		when (called_party_number between '5926090000' and '5926589999') and length(called_party_number)= 10
		or (called_party_number between '5925926090000' and '5925926589999') and length(called_party_number)= 13
		or (called_party_number between '6090000' and '6589999') and length(called_party_number)= 7
		then 'gtt_cell'
		when (substring(called_party_number, 1, 1) <> '6' and length(called_party_number)= 7)
		or (substring(called_party_number, 1, 3) ='592' and substring(called_party_number, 4, 1) <> '6' and length(called_party_number)= 10)
		or (substring(called_party_number, 1, 6) ='592592' and substring(called_party_number, 7, 1) <> '6' and length(called_party_number)= 13)
		then 'gtt_land'
		else 'unknown'
    end as destination, 
	duration as elapsed_time, 
	calling_party_number as originating_number, 
	called_party_number as terminating_number,
	timeframe_day,
	timeframe_hr
	from airspan_cdr
left join airspan_cdr_ip_mappings clli on originator_gw_ip=ip
where 
	duration > 0
	and originator_gw_ip in 
		(select ip
		from airspan_cdr_ip_mappings)