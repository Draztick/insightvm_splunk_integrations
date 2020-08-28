with "pKey" as (
    select 
        "da"."asset_id" "asset_id",
        concat(extract(epoch from "fv"."first_discovered"), "da"."asset_id"::text)::dec "checkpoint"
    from "postgres"."public"."dim_asset" "da"
        join "postgres"."public"."fact_asset_vulnerability_instance" "favi" using ("asset_id")
        join "postgres"."public"."dim_vulnerability" "dv" using ("vulnerability_id")
        join "postgres"."public"."fact_vulnerability" "fv" using ("vulnerability_id")
)
select 
    distinct on ("da"."mac_address") "da"."mac_address" "mac",
    "pKey"."checkpoint" "checkpoint",
    "da"."ip_address" "ip", 
    "da"."host_name" "hostname", 
    "dv"."title" "vulnerability", 
    "dv"."description" "description", 
    substring("dv"."title", 'CVE-\d{4}-\d{1,}') "cve", 
    substring("dv"."title", '[Mm]{1}[Ss]{1}\d{2}-\d{3,}') "ms",
    "dv"."severity" "severity",
    "dv"."cvss_score" "cvss",
    "favi"."proof" "signature",
    'insightvm' "dvc",
    "fv"."first_discovered" "discovered"
from "postgres"."public"."dim_asset" "da"
    join "pKey" using ("asset_id")
    join "postgres"."public"."fact_asset_vulnerability_instance" "favi" using ("asset_id")
    join "postgres"."public"."dim_vulnerability" "dv" using ("vulnerability_id")
    join "postgres"."public"."fact_vulnerability" "fv" using ("vulnerability_id")
where
    "pKey"."checkpoint" > ?
group by
    "da"."mac_address",
    "pKey"."checkpoint",
    "da"."ip_address",
    "da"."host_name",
    "dv"."title",
    "dv"."description",
    "dv"."severity",
    "dv"."cvss_score",
    "favi"."proof",
    "fv"."first_discovered"
order by
    "da"."mac_address" ASC,
    "pKey"."checkpoint" ASC