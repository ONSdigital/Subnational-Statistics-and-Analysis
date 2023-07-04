#Functions for maps.
#WARNING: uses big query package, which is probably imported by the code at this point, but not done explicitly here.
def get_la_shapefile():
    try: 
        import geopandas
    except: 
        pip install geopandas
        import geopandas

    query = """SELECT LAD20CD, geom, BNG_E, BNG_N
    FROM `project.ingest_geography_dataset_name.ingest_geography_table_name`
    """
    query_job = client.query(query, location="location",)
    
    la_geo = query_job.to_geodataframe
    return(la_geo())
