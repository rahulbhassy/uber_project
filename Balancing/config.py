from pyspark.sql.types import StructType, StructField, StringType, LongType , TimestampType

layer = {
    "customerdetails" : "raw",
    "driverdetails" : "raw",
    "vehicledetails": "raw",
    "uberfares" : "raw",
    "tripdetails" : "raw",
    "fares" : "enrich",
    "uber" : "enrich",
    "weatherimpact": "enrich",
    "uberfaresenrich": "enrich",
    "customerprofile": "enrich",
    "customerpreference" : "enrich"
}

SCHEMA = StructType([
    StructField("table_name", StringType(), False),
    StructField("expected_count", LongType(), False),
    StructField("actual_count", LongType(), False),
    StructField("difference", LongType(), False),
    StructField("result", StringType(), False),
    StructField("date", TimestampType(), False)
])

CHECKS = {
    "uberfares":{
        "tables": ["uberfares"],
        "sourcequery" : "SELECT COUNT(DISTINCT _c0) AS expected_count FROM csv.`{uberfares}` WHERE _c0 != 'key'",
        "targetquery" : "SELECT COUNT(*) AS actual_count FROM delta.`{uberfares}`"
    },
    "tripdetails":{
        "tables": ["tripdetails"],
        "sourcequery" : "SELECT COUNT(DISTINCT _c0) AS expected_count FROM csv.`{tripdetails}` WHERE _c0 != 'trip_id'",
        "targetquery" : "SELECT COUNT(*) AS actual_count FROM delta.`{tripdetails}`"
    },
    "customerdetails":{
        "tables": ["customerdetails"],
        "sourcequery" : "SELECT COUNT(DISTINCT _c0) AS expected_count FROM csv.`{customerdetails}` WHERE _c0 != 'customer_id'",
        "targetquery" : "SELECT COUNT(*) AS actual_count FROM delta.`{customerdetails}`"
    },
    "driverdetails":{
        "tables": ["driverdetails"],
        "sourcequery" : "SELECT COUNT(DISTINCT _c0) AS expected_count FROM csv.`{driverdetails}` WHERE _c0 != 'driver_id'",
        "targetquery" : "SELECT COUNT(*) AS actual_count FROM delta.`{driverdetails}`"
    },
    "vehicledetails":{
        "tables": ["driverdetails"],
        "sourcequery" : "SELECT COUNT(DISTINCT _c0) AS expected_count FROM csv.`{driverdetails}` WHERE _c0 != 'driver_id'",
        "targetquery" : "SELECT COUNT(*) AS actual_count FROM delta.`{vehicledetails}`"
    },
    "fares": {
        "tables": ["uber", "tripdetails"],
        "sourcequery": "SELECT COUNT(u.trip_id) AS expected_count FROM delta.`{uber}` u INNER JOIN delta.`{tripdetails}` t ON u.trip_id = t.trip_id",
        "targetquery": "SELECT COUNT(*) AS actual_count FROM delta.`{fares}`"
    },
    "uber": {
        "tables": ["uberfares"],
        "sourcequery": "SELECT COUNT(trip_id) AS expected_count FROM delta.`{uberfares}`",
        "targetquery": "SELECT COUNT(*) AS actual_count FROM delta.`{uber}`"
    },
    "weatherimpact": {
        "tables": ["fares", "tripdetails"],
        "sourcequery": "SELECT COUNT(f.trip_id) AS expected_count FROM delta.`{fares}` f INNER JOIN delta.`{tripdetails}` t ON f.trip_id = t.trip_id",
        "targetquery": "SELECT COUNT(*) AS actual_count FROM delta.`{weatherimpact}`"
    },
    "uberfaresenrich": {
        "tables": ["uberfares"],
        "sourcequery": "SELECT COUNT(trip_id) AS expected_count FROM delta.`{uberfares}`",
        "targetquery": "SELECT COUNT(*) AS actual_count FROM delta.`{uberfaresenrich}`"
    },
    "customerprofile":{
        "tables" : ["customerdetails","tripdetails","fares"],
        "sourcequery": """
            WITH fares_trip AS (
                SELECT t.customer_id AS customer_id FROM delta.`{tripdetails}` t 
                INNER JOIN delta.`{fares}` f ON f.trip_id = t.trip_id
                GROUP BY t.customer_id
            )
            SELECT COUNT(c.customer_id) AS expected_count FROM delta.`{customerdetails}` c INNER JOIN fares_trip ft ON c.customer_id = ft.customer_id
                """,
        "targetquery" : "SELECT COUNT(customer_id) AS actual_count FROM delta.`{customerprofile}`"

    },
    "customerpreference": {
        "tables" : ["customerprofile","fares","tripdetails","uberfares"],
        "sourcequery": """
            WITH combined AS (
                SELECT t.customer_id AS customer_id FROM delta.`{tripdetails}` t 
                INNER JOIN delta.`{fares}` f ON f.trip_id = t.trip_id
                INNER JOIN delta.`{uberfares}` u ON u.trip_id = t.trip_id
                GROUP BY t.customer_id
            )
            SELECT COUNT(c.customer_id) AS expected_count FROM delta.`{customerprofile}` c INNER JOIN combined ft ON c.customer_id = ft.customer_id
        """,
        "targetquery": "SELECT COUNT(customer_id) AS actual_count FROM delta.`{customerpreference}`"
    }

}
