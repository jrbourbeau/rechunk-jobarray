#!/usr/bin/env python
# coding: utf-8


import xarray as xr
from rechunker import rechunk
import zarr
from dotenv import load_dotenv
import os
import numpy as np
import dask
from dask.distributed import Client, LocalCluster
from dask.distributed import performance_report
import time
import shutil


if __name__ == "__main__":

    compute_instance = os.environ['CI_NAME']
    load_dotenv(f"/mnt/batch/tasks/shared/LS_root/mounts/clusters/{compute_instance}/code/Users/CMRE_Blob_Keys.env")
    AZURE_STORAGE_CONNECTION_STRING = os.environ['AZURE_STORAGE_CONNECTION_STRING']

    storage_options = {
       'connection_string': AZURE_STORAGE_CONNECTION_STRING
    }

    cluster = LocalCluster(
        n_workers=24,
        threads_per_worker=1,  # try then with maybe 2-4 threads per worker for concurrency?
        memory_limit='100GB',
        ip='0.0.0.0',
        processes=True,
    )
    client = Client(cluster)

    print(cluster)

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
    print("INFO: Reading the zarr dataset.")
    era5 = xr.open_zarr(
       "gs://gcp-public-data-arco-era5/ar/full_37-1h-0p25deg-chunk-1.zarr-v3",  # --> open_dataset w/ engine=zarr
       chunks={'time': tchk}
    )
    print("INFO: The dataset has been read.")

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
    max_mem = '200GB'

    print("INFO: Starting with the rechunking.")
    start_time = time.time()
    for i in range(nchunks):
        print(f"INFO: Iteration #{i}")
        i0 = i * nt_chunk
        i1 = min((i + 1) * nt_chunk, nt)
        ds = era5.isel(time=slice(i0, i1))

        #intermediate_path = f"abfs://cmre-cc01/ondrej/experiment/optimization-20/scratch-rechunk/intermediate__{i:02d}.zarr"
        intermediate_path = "/tmp/dask-tempdir-custom"
        shutil.rmtree(intermediate_path, ignore_errors=True)

        target_path = f"abfs://cmre-cc01/rsignell2/data/era5/era5_{region}_{i:02d}.zarr"

        rechunked = rechunk(
            ds.chunk({'time': tchk}),
            target_chunks={'time': -1, 'latitude': 20, 'longitude': 20},
            target_store=target_path,
            max_mem=max_mem,
            temp_store=intermediate_path,
            temp_options={'storage_options': storage_options, 'overwrite': True}
        )
        # Execute the rechunking using the Dask client
        with performance_report(filename=f"dask-report-{i}.html"):
            rechunked.execute(scheduler=client)
    client.close()
    print("--- %s seconds ---" % (time.time() - start_time))
    cluster.close()
