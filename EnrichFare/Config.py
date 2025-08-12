config = {
    "fares" : ["uber","tripdetails"],
    "weatherimpact" : ["fares"]
}
layer = {
    "uber" : "enrich",
    "tripdetails" : "raw",
    "fares" : "enrich",
    "weatherimpact" : "enrich"
}
