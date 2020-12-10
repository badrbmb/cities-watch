import time

import ee
from boto3.session import Session

from cities_watch import config


def list_objects_from_bucket(prefix=None, bucket_name=config.BUCKET_NAME, s3=None):
    if not s3:
        session = Session(aws_access_key_id=config.AWS_ACCESS_KEY_ID,
                          aws_secret_access_key=config.AWS_SECRET_ACCESS_KEY,
                          region_name=config.BUCKET_REGION)
        s3 = session.resource('s3',
                              endpoint_url=config.END_POINT_URL,
                              )

    bucket = s3.Bucket(bucket_name)

    if prefix:
        return list(bucket.objects.filter(Prefix=prefix))
    else:
        return list(bucket.objects.all())


def export_shapes_to_bucket(city_vectors, description, file_name, wait_finish=True, refresh=10):
    # Export shapes to bucket
    task = ee.batch.Export.table.toCloudStorage(
        collection=city_vectors,
        description=description,
        bucket=config.BUCKET_NAME,
        fileNamePrefix=file_name,
        fileFormat='GeoJSON'
    )
    task.start()
    print(f'Exporting shapes {description} to bucket={config.BUCKET_NAME} - task_id={task.id}')

    if wait_finish:
        while task.active():
            time.sleep(refresh)

        # Error condition
        if task.status()['state'] != 'COMPLETED':
            print('Error with image export.')
        else:
            print('Collection export completed.')

    return task
