# rechunk-jobarray

Trying out [Coiled's SLURM-style job arrays](https://docs.coiled.io/blog/slurm-job-arrays.html).

Here we are trying out a rechunking workflow where we rechunk a large dataset in pieces using job array, generating a collection of rechunked zarr datasets.   

We submit the job to Coiled using the script `submit_coiled_batch.sh`, which passes the credentials stored locally as environment variables along to Coiled. 

