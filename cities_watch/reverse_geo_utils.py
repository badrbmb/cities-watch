import json
import multiprocessing
from functools import partial

import geopandas as gpd
import numpy as np
import pandas as pd
import pyproj
from joblib import Parallel, delayed
from scipy.spatial import cKDTree
from shapely.geometry import mapping
from shapely.ops import transform
from tqdm import tqdm


def ckdnearest(gd1, gd2, geom_field1='geometry', geom_field2='geometry'):
    # adapted from https://gis.stackexchange.com/questions/222315/geopandas-find-nearest-point-in-other-dataframe
    n1 = np.array(list(gd1[geom_field1].apply(lambda x: (x.x, x.y))))
    n2 = np.array(list(gd2[geom_field2].apply(lambda x: (x.x, x.y))))
    btree = cKDTree(n2)
    dist, idx = btree.query(n1, k=1)
    gdf = pd.concat(
        [gd1.reset_index(drop=True), gd2.loc[idx, gd2.columns != geom_field2].reset_index(drop=True)], axis=1)
    return gdf


def compute_area(geom, multiplier=1e6):
    geom_area = transform(
        partial(
            pyproj.transform,
            pyproj.Proj(pyproj.crs.CRS('EPSG:4326')),
            pyproj.Proj(
                proj='aea',
                lat_1=geom.bounds[1],
                lat_2=geom.bounds[3]),
            always_xy=True),
        geom)
    return round(geom_area.area / multiplier, 3)


def form_new_city_record(idx, df_i, metadata=None, id_prefix=None):
    # Replace nan values by None
    df_i = df_i.where(pd.notnull(df_i), None)
    list_cities_i = df_i.drop(['index', 'geometry'], axis=1).to_dict('records')
    # drop None fields
    list_cities_i = [{u: v for u, v in t.items() if v} for t in list_cities_i]
    major_city = sorted(list_cities_i, key=lambda i: i.get('pageviews') if i.get('pageviews') else 0,
                        reverse=True)[0]
    geom = df_i['geometry'].values[0]
    new_record = {'geometry': json.dumps(mapping(geom)),
                  'cities': list_cities_i,
                  'area': compute_area(geom)}
    new_record.update(major_city)
    if metadata:
        new_record.update(metadata)
    # add uid
    if id_prefix:
        new_record['id'] = f"{id_prefix}_{idx}"
    return new_record


def add_city_ranking(all_records):
    df = pd.DataFrame(all_records)
    df.sort_values('area', ascending=False, inplace=True)
    df = df.reset_index(drop=True).reset_index()
    df.rename(columns={'index': 'rank'}, inplace=True)
    df['rank'] += 1

    # Make sure no nan values are left
    df = df.where(pd.notnull(df), None)

    return df.to_dict(orient='records')


def tag_nodes_to_shapes(df_nodes, city_geometries, metadata=None, add_ranks=True):
    # convert to geo-DataFrames
    gdf_nodes = gpd.GeoDataFrame(df_nodes)
    df_cities = gpd.GeoDataFrame(city_geometries, columns=['geometry'])
    df_cities.reset_index(inplace=True)

    # get first all cities containing OSM nodes
    df_contained = gpd.sjoin(df_cities, gdf_nodes, op='contains', how='inner').drop(['index_right'], axis=1)
    df_contained['tag_method'] = 'contain'

    # get all cities near OSM node but not containing one
    df_closest = df_cities[~df_cities['index'].isin(df_contained['index'].unique())].copy()
    df_closest['centroid'] = df_closest['geometry'].apply(lambda x: x.centroid)
    gdf_nodes_c = gdf_nodes.copy()
    gdf_nodes_c['node_geometry'] = gdf_nodes_c['geometry']
    df_closest = ckdnearest(df_closest, gdf_nodes_c, geom_field1='centroid')
    df_closest['tag_method'] = 'nearest'

    # concatenate the results
    df_cities_tagged = pd.concat([df_contained, df_closest[df_contained.columns]], axis=0)

    # post-process to get the final results
    num_cores = multiprocessing.cpu_count()
    # set prefix of uid of a record
    id_prefix = f"{metadata['country_code_gaul']}_{metadata['iso3c']}_{metadata['year']}"
    all_records = Parallel(n_jobs=num_cores)(delayed(form_new_city_record)(idx, df_i, metadata, id_prefix)
                                             for idx, df_i in tqdm(df_cities_tagged.groupby(['index']),
                                                                   total=len(df_cities_tagged['index'].unique())))

    if add_ranks:
        all_records = add_city_ranking(all_records)

    return all_records
