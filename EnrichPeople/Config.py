config = {
    "customerprofile" : ["customerdetails","fares","tripdetails"],
    "driverprofile" : ["driverdetails","fares","tripdetails"]
}
layer = {
    "customerdetails" : "raw",
    "driverdetails" : "raw",
    "fares" : "enrich",
    "tripdetails" : "raw",
    "customerprofile" : "enrich",
    "driverprofile" : "enrich"
}