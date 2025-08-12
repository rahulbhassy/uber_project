config = {
    "fares" : ["uber","tripdetails"],
    "weatherimpact" : ["fares","tripdetails"]
}
layer = {
    "uber" : "enrich",
    "tripdetails" : "raw",
    "fares" : "enrich",
    "weatherimpact" : "enrich"
}
