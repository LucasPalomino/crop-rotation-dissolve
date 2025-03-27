import duckdb

polygons_path = "./data/sample_CSB_polygons.shp" 
output_path = "./data/combined_polygons.shp" 

# Create a database called polygons.db
conn = duckdb.connect('polygons.db')

# Install the spatial extension
conn.execute("INSTALL spatial;")
conn.execute("LOAD spatial;")

# Create new DuckDB table from dataframe
conn.execute("DROP TABLE IF EXISTS CSB_sample;")
conn.execute(f"CREATE TABLE CSB_sample AS SELECT CSBID, geom FROM ST_Read('{polygons_path}');")

# Create column for pattern and change some values for testing (1 = Corn, 5 = Soybean, 36 = Alfalfa)
conn.execute("ALTER TABLE CSB_sample ADD COLUMN pattern INTEGER[8] DEFAULT array_value(1, 5, 1, 5, 1, 5, 1, 5);")
conn.execute("UPDATE CSB_sample SET pattern = array_value(1, 1, 5, 1, 1, 5, 36, 36) WHERE CSBID IN(551623004870722, 551623004870682);")

# Create copy of table for dynamic use and add a column for merged IDs initially empty
conn.execute("CREATE OR REPLACE TABLE combined_poly AS SELECT * FROM CSB_sample;")
conn.execute("ALTER TABLE combined_poly ADD COLUMN merged_list VARCHAR[];")
conn.execute("UPDATE combined_poly SET merged_list = [];")

# Convert table to dataframe and convert it to dictionary
polygon_df = conn.sql("SELECT CSBID, ST_AsText(geom) AS geom, pattern FROM combined_poly").df()
polygon_dict = polygon_df.to_dict(orient='records')


def dissolve_polygons(polygon_dict, starting_ID):

    ''' Recursive function to dissolve polygons that share an edge and have identical 8-year rotation patterns
    Args:
        polygon_dict: A list of dictionaries, where each dictionary represents a polygon row with
                      CSBID, geometry, rotation pattern, and list of merged polygons IDs 
        starting_ID: An arbitrary number to assign newly merged polygons

    Returns:
        A new list of dictionaries with dissolved polygons, reflected in the combined_poly table as well
    '''
    for polygon in polygon_dict:

        # Retrieve merged ID list for current polygon
        merged_IDs = conn.sql(f"SELECT merged_list FROM combined_poly WHERE CSBID = '{polygon['CSBID']}';").fetchone()[0]

        # Find neighboring polygons and convert dataframe to list of dictionaries
        neighbor_df = conn.sql(f"SELECT CSBID, ST_AsText(geom) AS geom, pattern FROM combined_poly WHERE ST_Touches(geom, ST_GeomFromText('{polygon['geom']}'))").df()
        neighbor_dict = neighbor_df.to_dict(orient='records')

        for neighbor in neighbor_dict:

            # Find shared geometry type to prevent neighbors on a single vertex
            shared_geom = conn.sql(f"SELECT ST_AsText(ST_Intersection(ST_GeomFromText('{polygon['geom']}'), ST_GeomFromText('{neighbor['geom']}')))").fetchone()[0]
            geometry_type = conn.sql(f"SELECT ST_GeometryType(ST_GeomFromText('{shared_geom}'))").fetchone()[0]

            # Dissolve polygons if geometries share and edge and have the same pattern
            if (geometry_type != 'POINT') and (polygon['pattern'] == neighbor['pattern']).all():
                
                # Add CSBID of polygon and neighbor to merged_ID list
                merged_IDs.extend([polygon['CSBID'], neighbor['CSBID']])

                # Remove old polygons from list
                conn.sql(f"DELETE FROM combined_poly WHERE CSBID IN('{polygon['CSBID']}','{neighbor['CSBID']}');")

                # Combine neighboring polygons with identical pattern
                combined_geom = conn.sql(f"SELECT ST_AsText(ST_Union(ST_GeomFromText('{polygon['geom']}'), ST_GeomFromText('{neighbor['geom']}')));").fetchone()[0]
                conn.execute(f"INSERT INTO combined_poly VALUES({starting_ID}, ST_GeomFromText('{combined_geom}'), {polygon['pattern'].tolist()}, {merged_IDs});")

                # Update dictionary for recursion
                updated_df = conn.sql("SELECT CSBID, ST_AsText(geom) AS geom, pattern FROM combined_poly;").df()
                updated_dict = updated_df.to_dict(orient='records')

                starting_ID += 1

                return dissolve_polygons(updated_dict, starting_ID)
  
    return polygon_dict


# Run algorithm to combine identical polygons
dissolve_polygons(polygon_dict, 0)

# Save dissolved polygons to shapefile
conn.sql(f"COPY (SELECT CSBID, geom, list_aggr(pattern, 'string_agg', ', ') AS pattern, list_aggr(merged_list, 'string_agg', ', ') AS merged_IDs  FROM combined_poly) TO '{output_path}' WITH (FORMAT gdal, DRIVER 'ESRI Shapefile', SRS '3857');")
print (f"File saved to {output_path}")