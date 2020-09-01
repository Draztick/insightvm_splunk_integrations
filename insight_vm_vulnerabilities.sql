WITH "pKey" AS (
    SELECT 
        "da"."asset_id" "asset_id",
        "dv"."vulnerability_id" "vulnerability_id",
        concat(extract(epoch FROM "fv"."first_discovered")::text, "da"."asset_id"::text)::dec "checkpoint"
    FROM "postgres"."public"."dim_asset" "da"
        JOIN "postgres"."public"."fact_asset_vulnerability_instance" "favi" USING ("asset_id")
        JOIN "postgres"."public"."dim_vulnerability" "dv" USING ("vulnerability_id")
        JOIN "postgres"."public"."fact_vulnerability" "fv" USING ("vulnerability_id")
    GROUP BY
        "da"."asset_id",
        "dv"."vulnerability_id",
        "fv"."first_discovered"
)
SELECT 
    DISTINCT ON ("da"."mac_address") "da"."mac_address" "mac",
    "pKey"."checkpoint" "checkpoint",
    "da"."ip_address" "ip", 
    "da"."host_name" "hostname", 
    "dv"."title" "vulnerability", 
    "dv"."description" "description", 
    SUBSTRING("dv"."title", 'CVE-\d{4}-\d{1,}') "cve", 
    SUBSTRING("dv"."title", '[Mm]{1}[Ss]{1}\d{2}-\d{3,}') "ms",
    "dv"."severity" "severity",
    "dv"."cvss_score" "cvss",
    "favi"."proof" "signature",
    'insightvm' "dvc",
    "fv"."first_discovered" "discovered"
FROM "postgres"."public"."dim_asset" "da"
    JOIN "pKey" USING ("asset_id")
    JOIN "postgres"."public"."fact_asset_vulnerability_instance" "favi" USING ("vulnerability_id")
    JOIN "postgres"."public"."dim_vulnerability" "dv" USING ("vulnerability_id")
    JOIN "postgres"."public"."fact_vulnerability" "fv" USING ("vulnerability_id")
WHERE
    "pKey"."checkpoint" > ?
GROUP BY
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
ORDER BY
    "da"."mac_address" ASC,
    "pKey"."checkpoint" ASC