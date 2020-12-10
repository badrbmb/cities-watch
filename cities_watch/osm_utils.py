import multiprocessing
import time

import overpy
import pandas as pd
from joblib import delayed, Parallel
from shapely.geometry import Point
from tqdm import tqdm

from cities_watch import config
from cities_watch.wiki_utils import get_city_pageviews


def query_osm_places(geom, filters=config.OSM_TAGS, admin_levels=config.OSM_ADMIN_LEVELS, verbose=config.VERBOSE,
                     timeout=config.OSM_TIMEOUT, count=0):
    api = overpy.Overpass()

    # get bbox from shapely geometry
    west, south, east, north = geom.bounds

    if verbose:
        print(f'Querying OSM for places within bbox: w={west}, s={south}, e={east}, n={north}')

    params = {}
    for p, t in zip([filters, admin_levels], ['filter', 'admin_level']):
        if isinstance(p, list):
            params[t] = '|'.join(filters)
        else:
            raise ValueError(f'Expected filters param as a list, got {p}')

    # fetch all nodes
    try:
        result = api.query(f"""
            [timeout:{timeout}];
            node({south}, {west}, {north}, {east})[place~"^({params['filter']})$"];
            foreach(
              out;
              is_in;
              area._[admin_level~"{params['admin_level']}"]["place"!="county"];
              out;
            );
            """)
    except overpy.exception.OverpassTooManyRequests:
        print('Too many overpass requests, sleeping ...')
        if count > 3:
            raise ValueError('Overpass query limits exceed !')

        # sleep and try again
        time.sleep(30)
        result = query_osm_places(geom, filters=filters,
                                  admin_levels=admin_levels,
                                  verbose=verbose,
                                  timeout=timeout,
                                  count=count + 1)

    return result


def get_node_infos(r):
    new_node = r.tags.copy()
    new_node['geometry'] = Point(r.lon, r.lat)
    new_node['node_id'] = r.id
    return new_node


def get_tagged_nodes(geom, filters=config.OSM_TAGS, admin_levels=config.OSM_ADMIN_LEVELS, out_path=None):
    result = query_osm_places(geom, filters=filters, admin_levels=admin_levels)

    df_nodes = pd.DataFrame([get_node_infos(n) for n in result.nodes])
    props_to_keep = ['node_id', 'name', 'name:en', 'alt_name', 'place', 'geometry', 'wikipedia']
    df_nodes = df_nodes[props_to_keep].copy()

    # Get page views for each node
    names = df_nodes['name'].values.tolist()
    alt_names = df_nodes['alt_name'].values.tolist()
    wikis = df_nodes['wikipedia'].values.tolist()

    num_cores = multiprocessing.cpu_count()
    results = Parallel(n_jobs=num_cores)(delayed(get_city_pageviews)(name, alt_names, wiki)
                                         for name, alt_names, wiki in tqdm(zip(names, alt_names, wikis),
                                                                           total=len(names)))

    df_nodes['pageviews'] = results

    # rename columns to contain only authorized characters
    df_nodes.rename(columns={t: t.replace(':', '_') for t in df_nodes.columns}, inplace=True)

    if out_path:
        df_nodes.to_csv(out_path)

    return df_nodes
