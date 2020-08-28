

# InsightVM Data Warehouse Integration with Splunk

This is a collection of SQL queries to be used with db_connect in Splunk to pull information from the InsightVM Data Warehouse, as well as their PGAdmin ready counterparts for running directly against the data warehouse.

## Quick Notes

In the dbconnect scripts, FROM and JOIN statements require the specification of the database name and schema.  This may differ depending on your Data Warehouse implementation, but should work with standard installations.  Using the FROM statement as an example:

`"postgres"."public"."dim_asset" "da"`

*postgres* is the database name, and *public* is the schema name.  Refer to your database and schema specifications and make the appropriate modifications, if needed.