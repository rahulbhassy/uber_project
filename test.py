from Shared.FileIO import DataLakeIO

loadio = DataLakeIO(
    process='load',
    table='uberfares',
    loadtype='full'
)
print(loadio.filepath())

readio = DataLakeIO(
    process='read',
    table='uberfares',
    loadtype='full',
    state='current',
    layer= 'raw'
)
print(readio.filepath())
readio = DataLakeIO(
    process='read',
    table='uberfares',
    loadtype='full',
    state='delta',
    layer= 'raw'
)
print(readio.filepath())

readio = DataLakeIO(
    process='read',
    table='uberfares',
    loadtype='full',
    state='current',
    layer= 'enrich',
    runtype='dev'
)
print(readio.filepath())