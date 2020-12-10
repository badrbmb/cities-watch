import math

import numpy as np
import pandas as pd
from shapely.geometry import box, Polygon, MultiPolygon, GeometryCollection, mapping, shape
from sklearn.cluster import DBSCAN

from cities_watch import config


def get_clusters_from_geom(f_geom, min_sample_poly=config.MIN_SAMPLE_POLYGONS,
                           max_distance=math.sqrt(config.MAX_AREA)):
    # form cluster of polygons
    centroids = np.array([t.centroid.xy for t in list(f_geom)])
    centroids = centroids.reshape(centroids.shape[0], -1)

    # dbscan
    dbscan = DBSCAN(eps=max_distance, min_samples=min_sample_poly)
    clusters = dbscan.fit(centroids)

    df_clusters = pd.DataFrame(f_geom, columns=['geometry'])
    df_clusters['clusters'] = clusters.labels_
    clusters_geoms = df_clusters.groupby('clusters').agg({'geometry': lambda x: MultiPolygon(list(x))})[
        'geometry'].values

    return clusters_geoms


def area_split(geom, max_area, max_count=250, count=0, return_singles=False):
    bounds = geom.bounds
    width = bounds[2] - bounds[0]
    height = bounds[3] - bounds[1]

    if (geom.area <= max_area) or (count == max_count):
        return [geom]

    if height >= width:
        # split left to right
        a = box(bounds[0], bounds[1], bounds[2], bounds[1] + height / 2)
        b = box(bounds[0], bounds[1] + height / 2, bounds[2], bounds[3])
    else:
        # split top to bottom
        a = box(bounds[0], bounds[1], bounds[0] + width / 2, bounds[3])
        b = box(bounds[0] + width / 2, bounds[1], bounds[2], bounds[3])

    result = []
    for d in (a, b,):
        c = geom.intersection(d)
        if not isinstance(c, GeometryCollection):
            c = [c]
        for e in c:
            if isinstance(e, (Polygon, MultiPolygon)):
                result.extend(area_split(e, max_area, count=(count + 1), return_singles=False))

    if count > 0:
        # Go back to the upper level in the recursion
        return result

    if return_singles:
        # convert multipart into single parts
        single_results = []
        for g in result:
            if isinstance(g, MultiPolygon):
                single_results.extend(g)
            else:
                single_results.append(g)
        return single_results
    else:
        return result


def split_feature(feature, extra_props=None, max_area=config.MAX_AREA,
                  return_singles=config.RETURN_SINGLES):
    f_shape = feature.geometry().getInfo()

    # Fix potential bad shapes with buffer 0
    f_geom = shape(f_shape).buffer(0)

    # Form clusters
    list_geoms = get_clusters_from_geom(f_geom)

    # Split big geometries
    list_aois = []
    for t in list_geoms:
        list_aois += area_split(t, max_area=max_area, return_singles=return_singles)

    out_list = []
    for i, aoi in enumerate(list_aois):
        geo_s = mapping(aoi)
        geo_s['properties'] = {'split_id': i}
        if extra_props:
            geo_s['properties'].update(extra_props)
        out_list.append(geo_s)

    return out_list
