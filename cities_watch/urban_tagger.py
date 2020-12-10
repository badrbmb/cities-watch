import os
import ee
import copy
import json
import pandas as pd
from shapely.geometry import shape
from shapely.ops import unary_union
from tqdm.notebook import tqdm

from cities_watch import config
from cities_watch.gcloud_utils import list_objects_from_bucket
from cities_watch.osm_utils import get_tagged_nodes
from cities_watch.reverse_geo_utils import tag_nodes_to_shapes
from cities_watch.bigquery_utils import push_records_to_bq


def load_country_shape(country):

    # Load country bbox
    aoi_collection = ee.FeatureCollection("USDOS/LSIB_SIMPLE/2017") \
        .filter(ee.Filter.eq('country_na', country))

    t = aoi_collection.geometry().getInfo()
    return shape(t).buffer(0)


def load_cities_shapes(aoi_props, ref_year, buffer_coeff=5 * 1e-3):
    # Load city shapes
    ff_prefix = f"{aoi_props['country_code_gaul']}_{aoi_props['iso3c']}"
    prefix = f"{config.FOLDER}/{ff_prefix}/{ref_year}"

    files = list_objects_from_bucket(prefix=prefix)
    files = [t for t in files if (config.FILENAME in t.key) & (t.key.endswith('.geojson'))]

    all_shapes = []
    for file in tqdm(files):
        try:
            file_content = file.get()['Body'].read().decode('utf-8')
            json_content = json.loads(file_content)
            all_shapes += json_content['features']
        except Exception as e:
            print(e)
            print(f'Failed loading shape from {file.key}')

    # merge shapes potentially split
    # process shapes to correct split features and holes
    all_geoms = [shape(t['geometry']).buffer(buffer_coeff).buffer(-buffer_coeff) for t in all_shapes]
    all_geoms = unary_union(all_geoms)

    return all_geoms


if __name__ == '__main__':

    # Define the desired countries and years to process
    selected_countries = ['India']  # Set to None for all countries
    years_list = [2015, 2016, 2017, 2018, 2019]

    # Load registry of countries with metadata
    file_path = os.path.join(config.ROOT_DIR, 'data', 'references', 'un_countries_sampled.csv')
    aois_metas = pd.read_csv(file_path).drop(['shape_area', 'status'], axis=1).to_dict('records')

    for country_name in selected_countries:
        aoi_meta = [t for t in aois_metas if t['country_name'] == country_name][0]

        print('Loading country shape and nodes from OSM ...')
        country_shape = load_country_shape(aoi_meta['country_na_LSIB'])
        node_name = f"{aoi_meta['country_code_gaul']}_{aoi_meta['iso3c']}.csv"
        file_node = os.path.join(config.OSM_NODES_FOLDER, node_name)
        try:
            df_nodes = pd.read_csv(file_node)
        except Exception as e:
            print('Loading nodes from OSM ...')
            df_nodes = get_tagged_nodes(country_shape, out_path=file_node)

        for year in tqdm(years_list):
            # Get country metadata
            props = copy.deepcopy(aoi_meta)
            props['year'] = year
            props.pop('country_na_LSIB')

            # Load shapes form cloud storage
            print(f"Loading city shapes for {aoi_meta}, year={year}")
            city_geometries = load_cities_shapes(aoi_meta, year)

            # Tag nodes to each shape
            print('Tagging nodes ...')
            all_records = tag_nodes_to_shapes(df_nodes, city_geometries, metadata=props, add_ranks=True)

            # save results local
            print(f'Saving results for year={year} ...')
            file_name = f"{aoi_meta['country_code_gaul']}_{aoi_meta['iso3c']}_{year}.json"
            file_path = os.path.join(config.RESUlTS_FOLDER, file_name)
            with open(file_path, 'w') as f:
                json.dump(all_records, f)

            # Push to BigQuery table
            path_to_schema = os.path.join(config.SCHEMA_FOLDER, f"{config.TABLE_NAME}.json")
            table_id = f"{config.PROJECT_NAME}.{config.DATASET_NAME}.{config.TABLE_NAME}"
            fails = push_records_to_bq(bq_records=all_records, table_id=table_id, path_to_schema=path_to_schema)

            if len(fails) > 0:
                # save failed records to local
                print(f'Saving failed push records for year={year} ...')
                file_name = f"{aoi_meta['country_code_gaul']}_{aoi_meta['iso3c']}_{year}.json"
                file_path = os.path.join(config.FAILS_FOLDER, file_name)
                with open(file_path, 'w') as f:
                    json.dump(fails, f)




