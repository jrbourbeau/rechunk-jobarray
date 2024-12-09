#!/usr/bin/env python
# coding: utf-8


if __name__ == "__main__":

    import xarray as xr
    from rechunker import rechunk
    import zarr
    import os
    import numpy as np
    import dask
    from dask.distributed import Client, LocalCluster
    from dask.distributed import performance_report
    import time
    import shutil
    import fsspec

    storage_options = {'endpoint_url':'https://ncsa.osn.xsede.org',
       'key':os.environ['AWS_ACCESS_KEY_ID'],
       'secret':os.environ['AWS_SECRET_ACCESS_KEY']
     }
    # storage_options = {}

    fs = fsspec.filesystem('s3', **storage_options)
    
    # For c7g.8xlarge (32cpu, 64GB RAM)
    n_workers=30
    mem = 64
    cluster = LocalCluster(n_workers=n_workers, threads_per_worker=1)
    client = Client(cluster)

    vars = [
    '10m_u_component_of_wind',
    '10m_v_component_of_wind',
    '10m_wind_gust_since_previous_post_processing',
    '2m_dewpoint_temperature',
    '2m_temperature',
    'surface_pressure'
    ]
    #    region = 'europe'
    region = 'usa'

    coords = {'europe': {'longitude': [-45, 82], 'latitude': [78, 19]},
            'usa': {'longitude':[-130, -60], 'latitude':[65, 25]}}

    tchk = 24 * 12
    print("INFO: Opening the dataset.")
    era5 = xr.open_dataset(
        "gs://gcp-public-data-arco-era5/ar/full_37-1h-0p25deg-chunk-1.zarr-v3",  
        engine='zarr', chunks={'time': tchk}
        )
    print("INFO: The dataset has been opened.")

    era5 = era5.assign_coords(longitude=(((era5.longitude + 180) % 360) - 180)).sortby('longitude')
    lons = coords[region]['longitude']
    lats = coords[region]['latitude']
    era5 = era5[vars].sel(
        longitude=slice(lons[0], lons[1]),
        latitude=slice(lats[0], lats[1]),
        time=slice('1950', '2022')
        )
    nt = len(era5.time)
    nt_chunk = 24 * 360 * 4  # It's hourly data so this is number of years processed in one cycle below
    nchunks = int(np.ceil(nt / nt_chunk))
    max_mem = f'{mem/n_workers*0.7:.2f}GB'  
    print(f"INFO: max_mem={max_mem}")
    print("INFO: Starting with the rechunking.")
    start_time = time.time()
    i = int(os.environ['COILED_ARRAY_TASK_ID'])
    print(f"INFO: Iteration #{i}")
    i0 = i * nt_chunk
    i1 = min((i + 1) * nt_chunk, nt)
    ds = era5.isel(time=slice(i0, i1))
    print(ds.time)
    intermediate_path = "/tmp/dask-tempdir-custom"
    shutil.rmtree(intermediate_path, ignore_errors=True)
    os.makedirs(intermediate_path, exist_ok=True)

    # target_url = f"s3://oss-scratch-space/jrbourbeau/era5_{region}_{i:02d}.zarr"
    target_url = f"s3://esip/rsignell/era5_{region}_{i:02d}.zarr"
    try:
        fs.rm(target_url, recursive=True)
    except:
        pass
    target_path = fs.get_mapper(target_url)

    rechunked = rechunk(
        ds.chunk({'time': tchk}),
        target_chunks={'time': -1, 'latitude': 20, 'longitude': 20},
        target_store=target_path,
        max_mem=max_mem,
        temp_store=intermediate_path)
    # Execute the rechunking using the Dask client
    rechunked.execute(scheduler=client, retries=10)

    client.close()
    print("--- %s seconds ---" % (time.time() - start_time))
    cluster.close()
