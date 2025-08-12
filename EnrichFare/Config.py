config = {
    "fares" : ["uber","tripdetails"],
    "weatherimpact" : ["fares","tripdetails"],
    "timeseries" : ["fares", "uberfares"]
}
layer = {
    "uber" : "enrich",
    "tripdetails" : "raw",
    "fares" : "enrich",
    "weatherimpact" : "enrich",
    "timeseries" : "enrich",
    "uberfares": "enrich"
}
