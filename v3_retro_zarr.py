import logging
from pathlib import Path
from typing import Tuple

from dask.distributed import Client, LocalCluster
import geopandas as gpd
import s3fs
import xarray as xr

logger = logging.getLogger(__name__)

def open_s3_store(url: str) -> s3fs.S3Map:
    """Open an s3 store from a given url."""
    return s3fs.S3Map(url, s3=s3fs.S3FileSystem(anon=True))

def load_zarr_datasets() -> xr.Dataset:
    """Load zarr datasets from S3 within the specified time range."""
    # if a LocalCluster is not already running, start one
    if not Client(timeout="2s"):
        cluster = LocalCluster()    
    forcing_vars = ["lwdown", "precip", "psfc", "q2d", "swdown", "t2d", "u2d", "v2d"]
    s3_urls = [
        f"s3://noaa-nwm-retrospective-3-0-pds/CONUS/zarr/forcing/{var}.zarr"
        for var in forcing_vars
    ]
    s3_stores = [open_s3_store(url) for url in s3_urls]
    dataset = xr.open_mfdataset(s3_stores, parallel=True, engine="zarr")
    return dataset


def clip_dataset_to_bounds(
    dataset: xr.Dataset, bounds: Tuple[float, float, float, float], start_time: str, end_time: str
) -> xr.Dataset:
    """Clip the dataset to specified geographical bounds."""
    dataset = dataset.sel(
        x=slice(bounds[0], bounds[2]),
        y=slice(bounds[1], bounds[3]),
        time=slice(start_time, end_time),
    )
    logger.info("Selected time range and clipped to bounds")
    return dataset


def cache_store_locally(stores: xr.Dataset, cached_nc_path: Path) -> xr.Dataset:    
    stores.to_netcdf(cached_nc_path)
    data = xr.open_mfdataset(cached_nc_path, parallel=True, engine="h5netcdf")
    return data


def create_forcings(start_time: str, end_time: str) -> None:
    projection = xr.open_dataset('template.nc', engine="h5netcdf").crs.esri_pe_string
    gdf = gpd.read_file('wb-15444_subset.gpkg', layer="divides").to_crs(projection)

    lazy_store = load_zarr_datasets()

    clipped_store = clip_dataset_to_bounds(lazy_store, gdf.total_bounds, start_time, end_time)

    local_data = cache_store_locally(clipped_store, 'cached.nc')

    


if __name__ == "__main__":
    # Example usage
    start_time = "2010-01-01 00:00"
    end_time = "2010-01-02 00:00"
    create_forcings(start_time, end_time)
