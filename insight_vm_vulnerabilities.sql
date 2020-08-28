select distinct 
	da.ip_address ip, 
	da.host_name hostname, 
	dv.title vulnerability, 
	dv.description description, 
	substring(dv.title, 'CVE-\d{4}-\d{1,}') cve, 
	substring(dv.title, '[Mm]{1}[Ss]{1}\d{2}-\d{3,}') ms,
	dv.severity severity,
	dv.cvss_score cvss,
	favi.proof signature,
	'insightvm' dvc,
	fv.first_discovered discovered
from dim_asset da
	join fact_asset_vulnerability_instance favi using (asset_id)
	join dim_vulnerability dv using (vulnerability_id)
	join fact_vulnerability fv using (vulnerability_id)
where 
	fv.first_discovered >= now() - interval '1 day'