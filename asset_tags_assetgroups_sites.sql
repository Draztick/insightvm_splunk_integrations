with "hostnames" as (
    select distinct "da"."asset_id" "asset_id", RTRIM("da"."host_name", '.whs.int') "host_name"
    from "dim_asset" "da"
), "pKey" as (
    select 
        "da"."asset_id" "asset_id",
        concat(extract(epoch from "dsc"."finished"), "da"."asset_id"::text)::dec "checkpoint"
    from "dim_asset" "da"
        join "dim_site_asset" "dsa" using ("asset_id")
        join "dim_site" "ds" using ("site_id")
        join "dim_scan" "dsc" on "ds"."last_scan_id"="dsc"."scan_id"
)
select 
    distinct on ("da"."mac_address") "da"."mac_address" "mac",
    "pKey"."checkpoint" "checkpoint",
    string_agg(DISTINCT "da"."ip_address"::text, '|') ip,
    string_agg(DISTINCT "hostnames"."host_name", '|') as "nt_host",
    string_agg(distinct CONCAT("hostnames"."host_name", '.whs.int'), '|') "dns",
    "daos"."vendor" "os_vendor",
    "daos"."name" "os_name",
    "daos"."certainty" "certainty",
    string_agg(DISTINCT "dst"."name", '|') "sites",
    string_agg(DISTINCT "dag"."name", '|') "asset_groups",
    string_agg(DISTINCT "dt"."name", '|') "tags",
    "dsc"."finished" "Last Scanned"
from "postgres"."public"."dim_asset" "da"
    join "hostnames" USING ("asset_id")
    join "pKey" USING ("asset_id")
    join "postgres"."public"."dim_asset_operating_system" "daos" using ("asset_id")
    join "postgres"."public"."dim_site_asset" "dsa" USING ("asset_id")
    join "postgres"."public"."dim_site" "dst" USING ("site_id")
    join "postgres"."public"."dim_scan" "dsc" ON "dst"."last_scan_id"="dsc"."scan_id"
    join "postgres"."public"."dim_asset_tag" "dat" USING ("asset_id")
    join "postgres"."public"."dim_asset_group" "dag" USING ("asset_group_id")
    join "postgres"."public"."dim_tag" "dt" USING ("tag_id")
where
    "pKey"."checkpoint" > ?
group by 
    "hostnames"."host_name",
    "pKey"."checkpoint",
    "da"."asset_id",
    "da"."mac_address",
    "da"."host_name",
    "da"."ip_address",
    "daos"."vendor",
    "daos"."name",
    "daos"."certainty",
    "dsc"."finished"
order by
    "da"."mac_address" ASC,
    "pKey"."checkpoint" ASC