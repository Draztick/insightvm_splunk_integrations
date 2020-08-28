WITH "hostnames" AS (
    SELECT DISTINCT "da"."asset_id" "asset_id", RTRIM("da"."host_name", '.whs.int') "host_name"
    FROM "dim_asset" "da"
), "pKey" AS (
    SELECT 
        "da"."asset_id" "asset_id",
        CONCAT(EXTRACT(EPOCH FROM "dsc"."finished"), "da"."asset_id"::text)::dec "checkpoint"
    FROM "dim_asset" "da"
        JOIN "dim_site_asset" "dsa" USING ("asset_id")
        JOIN "dim_site" "ds" USING ("site_id")
        JOIN "dim_scan" "dsc" ON "ds"."last_scan_id"="dsc"."scan_id"
)
SELECT 
    DISTINCT ON ("da"."mac_address") "da"."mac_address" "mac",
    "pKey"."checkpoint" "checkpoint",
    STRING_AGG(DISTINCT "da"."ip_address"::text, '|') "ip",
    STRING_AGG(DISTINCT "hostnames"."host_name", '|') as "nt_host",
    STRING_AGG(DISTINCT CONCAT("hostnames"."host_name", '.whs.int'), '|') "dns",
    "daos"."vendor" "os_vendor",
    "daos"."name" "os_name",
    "daos"."certainty" "certainty",
    STRING_AGG(DISTINCT "dst"."name", '|') "sites",
    STRING_AGG(DISTINCT "dag"."name", '|') "asset_groups",
    STRING_AGG(DISTINCT "dt"."name", '|') "tags",
    "dsc"."finished" "Last Scanned"
FROM "postgres"."public"."dim_asset" "da"
    JOIN "hostnames" USING ("asset_id")
    JOIN "pKey" USING ("asset_id")
    JOIN "postgres"."public"."dim_asset_operating_system" "daos" USING ("asset_id")
    JOIN "postgres"."public"."dim_site_asset" "dsa" USING ("asset_id")
    JOIN "postgres"."public"."dim_site" "dst" USING ("site_id")
    JOIN "postgres"."public"."dim_scan" "dsc" ON "dst"."last_scan_id"="dsc"."scan_id"
    JOIN "postgres"."public"."dim_asset_tag" "dat" USING ("asset_id")
    JOIN "postgres"."public"."dim_asset_group" "dag" USING ("asset_group_id")
    JOIN "postgres"."public"."dim_tag" "dt" USING ("tag_id")
WHERE
    "pKey"."checkpoint" > ?
GROUP BY 
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
ORDER BY
    "da"."mac_address" ASC,
    "pKey"."checkpoint" ASC