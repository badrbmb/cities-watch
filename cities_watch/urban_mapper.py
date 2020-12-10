import json
import os
import time

import ee
import pandas as pd
from tqdm import tqdm

from cities_watch import config
from cities_watch.gcloud_utils import export_shapes_to_bucket, list_objects_from_bucket
from cities_watch.geom_utils import split_feature
from cities_watch.models import map_urban_areas, load_model


def get_file_name(aoi_props, ref_year):
    """

    :param aoi_props:
    :param ref_year:
    :return: File name (e.g: FOLDER/85_FRA/2015/predicted_shapes_1.geosjon)
    """
    ff_prefix = f"{aoi_props['country_code_gaul']}_{aoi_props['iso3c']}"
    sp_id = aoi_props['split_id']
    return f"{config.FOLDER}/{ff_prefix}/{ref_year}/{config.FILENAME}_{sp_id}"


def main(list_metas, list_years, model, verbose=config.VERBOSE):
    output = []

    for aoi_meta in tqdm(list_metas):
        # Load the country borders from the Large Scale International Boundary Polygons - Simplified 2017 version.
        aoi_collection = ee.FeatureCollection("USDOS/LSIB_SIMPLE/2017") \
            .filter(ee.Filter.eq('country_na', aoi_meta['country_na_LSIB']))

        # Split the country when the shape is too big
        if verbose:
            print('AOI split check ...')
        aois = split_feature(aoi_collection, extra_props=aoi_meta)
        print(f'AOI split into {len(aois)} part(s)')

        for year in list_years:
            start_date = f"{year}-01-01"
            end_date = f"{year}-12-31"

            # Run processing for each split of country shape, by year
            for aoi in aois:
                # Check if existing file available
                file_name = get_file_name(aoi['properties'], year)
                old_content = list_objects_from_bucket(file_name)

                if (len(old_content) == 0) | config.UPDATE:
                    _, city_vectors = map_urban_areas(aoi=aoi, start_date=start_date, end_date=end_date,
                                                      model=model, vectorized=True)

                    # Export shapes to cloud storage
                    description = f"{aoi['properties']['country_name']}_{year}_{aoi['properties']['split_id']}"
                    task = export_shapes_to_bucket(city_vectors=city_vectors,
                                                   file_name=file_name,
                                                   description=description,
                                                   wait_finish=False)
                    task_summary = task.status()
                    task_summary['aoi'] = aoi
                    output.append(task_summary)
                else:
                    print(f'Found {len(old_content)} existing record for filename={file_name}, UPDATE={config.UPDATE}')

    return output


if __name__ == '__main__':

    # Define the desired countries and years to process
    selected_countries = ['France']  # Set to None for all countries
    years_list = [2015, 2016, 2017, 2018, 2019]

    # Load registry of countries with metadata
    file_path = os.path.join(config.ROOT_DIR, 'data', 'references', 'un_countries_sampled.csv')
    aois_metas = pd.read_csv(file_path).drop(['shape_area', 'status'], axis=1).to_dict('records')

    if selected_countries:
        aois_metas = [pp for pp in aois_metas if pp['country_name'] in selected_countries]

    # Load the trained model
    loaded_model = load_model()

    # Run city shapes predictions and export to cloud storage
    results = main(list_metas=aois_metas, list_years=years_list, model=loaded_model)

    # Write summary with task ids
    if len(results) > 0:
        out_summary = os.path.join(config.SUMMARY_FOLDER, f'run_summary_{int(time.time())}.json')
        with open(out_summary, 'w') as f:
            json.dump(results, f)
        print(f'Run summary written at {out_summary}')
