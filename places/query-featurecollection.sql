with datas as (
select json(data) as data from files
where
json_extract(data, '$.properties.edtf:deprecated') is null and
json_extract(data, '$.properties.wof:placetype') in ('county') and
(select json_extract(value, '$.region_id') as region_id
 from json_each(json_extract(data, '$.properties.wof:hierarchy')))=85682075
)
select
json_object(
	'type', 'FeatureCollection',
	'features', (select json_group_array(data) from datas)
)
