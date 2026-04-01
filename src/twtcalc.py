import os
import shutil
import datetime
import rioxarray
import xarray as xr
import zipfile
from rasterio.enums import Resampling
import numpy as np
import rasterio
from rasterio import warp

def calculate_strm_permanence(
    *,
    fname_perc_inundation=None,
    fname_strm_mask=None,
    verbose=False,
    overwrite=False,
    atol=1e-6 # tolerance for treating 100% as perennial
):
    """
    In-memory stream permanence computation

    Outputs (float32, NaN nodata embedded in pixel values):
      - perennial_strms_*.tiff: 1 where perennial (~100%), NaN elsewhere
      - nonperennial_strms_*.tiff: percent where 0 < perc < 100 - atol on streams, NaN elsewhere
    """
    if verbose:
        print("calling calculate_strm_permanence")

    if any(v is None for v in [fname_perc_inundation, fname_strm_mask]):
        raise ValueError("Required: fname_perc_inundation and fname_strm_mask")

    # Build output names, robust to .tif/.tiff
    base = os.path.basename(fname_perc_inundation)
    stem, _ = os.path.splitext(base)
    dstr = stem.replace("percent_inundated_grid_", "")
    out_dir = os.path.dirname(fname_perc_inundation)
    fname_p  = os.path.join(out_dir, f"perennial_strms_{dstr}.tiff")
    fname_np = os.path.join(out_dir, f"nonperennial_strms_{dstr}.tiff")

    if (os.path.isfile(fname_p) and os.path.isfile(fname_np)) and not overwrite:
        if verbose:
            print(f"found existing outputs:\n  {fname_p}\n  {fname_np}")
        return fname_p, fname_np

    if verbose:
        print(f"writing:\n  {fname_p}\n  {fname_np}")

    # Open rasters
    perc_da = rioxarray.open_rasterio(fname_perc_inundation, masked=True)
    mask_da = rioxarray.open_rasterio(fname_strm_mask, masked=True)

    # Ensure single band; drop band dimension if present
    if "band" in perc_da.dims:
        if perc_da.sizes["band"] != 1:
            raise ValueError("Percent-inundation raster must be single-band.")
        perc_da = perc_da.squeeze("band", drop=True)
    if "band" in mask_da.dims:
        if mask_da.sizes["band"] != 1:
            raise ValueError("Stream mask raster must be single-band.")
        mask_da = mask_da.squeeze("band", drop=True)

    # Align mask to percent grid using nearest-neighbor (categorical-safe)
    mask_da = mask_da.rio.reproject_match(perc_da)

    # Load fully into memory
    perc_da = perc_da.load()
    mask_da = mask_da.load()

    # Normalize nodata to NaN using nodata values from rioxarray
    nd_perc = perc_da.rio.nodata
    if nd_perc is not None and not (isinstance(nd_perc, float) and np.isnan(nd_perc)):
        perc_da = perc_da.where(perc_da != nd_perc, other=np.nan)

    nd_mask = mask_da.rio.nodata
    if nd_mask is not None and not (isinstance(nd_mask, float) and np.isnan(nd_mask)):
        mask_da = mask_da.where(mask_da != nd_mask, other=np.nan)

    # Build boolean stream mask: True where mask == 1 (NaNs -> False)
    stream_mask_bool = (mask_da == 1)

    # Restrict perc to streams; outside -> NaN
    perc_on_streams = perc_da.where(stream_mask_bool, other=np.nan)

    # Perennial where approx 100% within tolerance (NaNs evaluate to False)
    is_perennial = np.isfinite(perc_on_streams) & (np.abs(perc_on_streams - 100.0) <= atol)
    perennial_da = xr.where(is_perennial, 1.0, np.nan).astype("float32")

    # Non-perennial where on streams, finite, >0 and not perennial
    nonperennial_mask = stream_mask_bool & np.isfinite(perc_da) & (perc_da > 0.0) & (~is_perennial)
    nonperennial_da = xr.where(nonperennial_mask, perc_da, np.nan).astype("float32")

    # Preserve georeferencing from percent grid and clear nodata tag (use NaNs)
    perennial_da = perennial_da.rio.write_crs(perc_da.rio.crs, inplace=False)
    perennial_da = perennial_da.rio.write_transform(perc_da.rio.transform(), inplace=False)
    perennial_da = perennial_da.rio.write_nodata(None, inplace=False)

    nonperennial_da = nonperennial_da.rio.write_crs(perc_da.rio.crs, inplace=False)
    nonperennial_da = nonperennial_da.rio.write_transform(perc_da.rio.transform(), inplace=False)
    nonperennial_da = nonperennial_da.rio.write_nodata(None, inplace=False)

    # Write outputs using rioxarray defaults (no extra to_raster options)
    perennial_da.rio.to_raster(fname_p)
    nonperennial_da.rio.to_raster(fname_np)

    if verbose:
        print("done.")

    return fname_p, fname_np

def calculate_inundation_slow(
    *,
    dt_start: datetime.datetime,
    dt_end: datetime.datetime,
    wtd_raw_dir: str,
    wtd_resampled_dir: str,
    inundation_out_dir: str,
    fname_twi: str,
    fname_twi_mean: str,
    fname_soil_trans: str,
    verbose: bool = False,
    overwrite: bool = False,
):
    #
    #
    if verbose: print('calling calculate_inundation_slow')
    need = _check_exist(inundation_out_dir,dt_start,dt_end)
    if not need:
        if verbose: print( f' found existing inundation calculations in {inundation_out_dir}')
        return
    #
    #
    os.makedirs(inundation_out_dir, exist_ok=True)
    twi         = rioxarray.open_rasterio(filename=fname_twi,masked=True)
    twi_mean    = rioxarray.open_rasterio(filename=fname_twi_mean,masked=True)
    twi_mean    = twi_mean.rio.reproject_match(twi, resampling=Resampling.bilinear)
    soil_transm = rioxarray.open_rasterio(filename=fname_soil_trans,masked=True)
    soil_transm = soil_transm.rio.reproject_match(twi, resampling=Resampling.bilinear)
    #
    #
    if "band" in twi.dims and twi.sizes["band"] == 1:
        twi = twi.squeeze("band", drop=True)
    if "band" in twi_mean.dims and twi_mean.sizes["band"] == 1:
        twi_mean = twi_mean.squeeze("band", drop=True)
    if "band" in soil_transm.dims and soil_transm.sizes["band"] == 1:
        soil_transm = soil_transm.squeeze("band", drop=True)
    #
    # 
    threshold = xr.where(soil_transm != 0, - (twi - twi_mean) / soil_transm, np.nan)
    threshold = threshold.astype("float32")
    #
    #
    idt = dt_start
    while idt <= dt_end:
        dt_str             = idt.strftime('%Y%m%d')
        fname_wtd_mean_raw = os.path.join(wtd_raw_dir,f'wtd_{dt_str}.tiff')
        fname_inund        = os.path.join(inundation_out_dir,f'inundation_{dt_str}.tiff')
        if not os.path.isfile(fname_wtd_mean_raw):
            raise Exception(f'calculate_inundation could not find {fname_wtd_mean_raw}')
        if not os.path.isfile(fname_inund) or overwrite:
            if verbose: print(f' processing {dt_str}')
            with rioxarray.open_rasterio(fname_wtd_mean_raw, masked=True) as rioxds_wtd_raw:
                wtd_mean = rioxds_wtd_raw.rio.reproject_match(twi, resampling=Resampling.bilinear)
            if "band" in wtd_mean.dims and wtd_mean.sizes["band"] == 1:
                wtd_mean = wtd_mean.squeeze("band", drop=True)
            wtd_mean = -wtd_mean
            wtd_mean = xr.where(wtd_mean >= threshold, 1.0, np.nan)
            wtd_mean = wtd_mean.astype("float32").rio.write_nodata(np.nan)
            wtd_mean.rio.to_raster(
                fname_inund,
                compress="deflate",
                tiled=True,
                BIGTIFF="IF_SAFER",
            )
        idt += datetime.timedelta(days=1)

def _read_base_grid_and_array(fname):
    """
    Read the base grid (TWI) as float32 and return:
    - arr: np.ndarray float32 (height, width), nodata -> np.nan
    - profile: rasterio profile with transform, crs, width, height
    """
    with rasterio.open(fname) as src:
        profile = src.profile.copy()
        arr = src.read(1, out_dtype="float32")
        nodata = src.nodata
        if nodata is not None and not np.isnan(nodata):
            arr[arr == nodata] = np.nan
        # Ensure single band float32; do not store NaN as nodata in metadata
        profile.update(
            count=1,
            dtype="float32",
        )
        # Remove nodata key if present; we will keep NaNs in data
        profile.pop("nodata", None)
    return arr, profile

def _reproject_to_target(src_path, dst_shape, dst_transform, dst_crs,
                         resampling=Resampling.bilinear, num_threads=None, dst_dtype="float32"):
    """
    Reproject a raster (first band) to a given target grid. Returns np.ndarray float32 with NaNs as nodata.
    """
    with rasterio.open(src_path) as src:
        dst = np.empty(dst_shape, dtype=dst_dtype)
        warp.reproject(
            source=rasterio.band(src, 1),
            destination=dst,
            src_transform=src.transform,
            src_crs=src.crs,
            src_nodata=src.nodata,
            dst_transform=dst_transform,
            dst_crs=dst_crs,
            dst_nodata=np.nan,
            resampling=resampling,
            num_threads=(num_threads or os.cpu_count() or 1),
        )
    return dst

def _write_geotiff(fname, 
                   arr, 
                   profile, 
                   compress="deflate", 
                   bigtiff="IF_SAFER",
                   blocksize=512, 
                   zlevel=1, 
                   predictor=2):
    """
    Write a single-band float32 GeoTIFF with tiling and compression.
    """
    out_profile = profile.copy()
    out_profile.update(
        driver="GTiff",
        compress=compress,
        BIGTIFF=bigtiff,
        tiled=True,
        blockxsize=blocksize,
        blockysize=blocksize,
    )
    # Add codec-specific options
    if compress in ("deflate", "zstd"):
        out_profile["zlevel"] = zlevel
        out_profile["predictor"] = predictor
    elif compress in ("lzw",):
        out_profile["predictor"] = predictor

    # Remove None-valued keys to avoid GDAL warnings
    out_profile = {k: v for k, v in out_profile.items() if v is not None}

    with rasterio.open(fname, "w", **out_profile) as dst:
        dst.write(arr, 1)

def calculate_inundation(*,
    dt_start: datetime.datetime,
    dt_end: datetime.datetime,
    wtd_raw_dir: str,
    inundation_out_dir: str,
    fname_twi: str,
    fname_twi_mean: str,
    fname_soil_trans: str,
    wtd_resampled_dir: str = None,
    verbose: bool = False,
    overwrite: bool = False,
    resampling=Resampling.bilinear,
    warp_threads: int | None = 4,
    compress: str | None = "deflate",
    blocksize: int = 512,
    zlevel: int = 1,
    predictor: int = 2):

    if verbose:
        print('calling calculate_inundation')

    need = _check_exist(inundation_out_dir, dt_start, dt_end)
    if not need:
        if verbose:
            print(f' found existing inundation calculations in {inundation_out_dir}')
        return

    os.makedirs(inundation_out_dir, exist_ok=True)

    # Enable multi-threaded GDAL inside each reprojection
    gdal_env = rasterio.Env(GDAL_NUM_THREADS="ALL_CPUS", NUM_THREADS="ALL_CPUS")

    with gdal_env:
        # 1) Read/define target grid from TWI
        twi_arr, base_profile = _read_base_grid_and_array(fname_twi)
        height = base_profile['height']
        width  = base_profile['width']
        dst_transform = base_profile['transform']
        dst_crs = base_profile['crs']
        dst_shape = (height, width)

        # 2) Reproject twi_mean and soil_trans to target grid
        twi_mean_arr = _reproject_to_target(
            fname_twi_mean, dst_shape, dst_transform, dst_crs,
            resampling=resampling, num_threads=warp_threads, dst_dtype="float32"
        )
        soil_trans_arr = _reproject_to_target(
            fname_soil_trans, dst_shape, dst_transform, dst_crs,
            resampling=resampling, num_threads=warp_threads, dst_dtype="float32"
        )

        # 3) Compute threshold once: threshold = -(twi - twi_mean) / soil_trans, soil_trans != 0 else NaN
        with np.errstate(divide='ignore', invalid='ignore'):
            threshold = np.where(
                soil_trans_arr != 0.0,
                -(twi_arr - twi_mean_arr) / soil_trans_arr,
                np.nan
            ).astype(np.float32)

        # 4) Sequentially process each day
        idt = dt_start
        while idt <= dt_end:
            dt_str = idt.strftime('%Y%m%d')
            fname_wtd_mean_raw = os.path.join(wtd_raw_dir, f'wtd_{dt_str}.tiff')
            fname_inund        = os.path.join(inundation_out_dir, f'inundation_{dt_str}.tiff')

            if not os.path.isfile(fname_wtd_mean_raw):
                raise FileNotFoundError(f'calculate_inundation could not find {fname_wtd_mean_raw}')

            if not os.path.isfile(fname_inund) or overwrite:
                if verbose:
                    print(f' processing {dt_str}')

                # Reproject WTD to target grid
                wtd_arr = _reproject_to_target(
                    fname_wtd_mean_raw, dst_shape, dst_transform, dst_crs,
                    resampling=resampling, num_threads=warp_threads, dst_dtype="float32"
                )

                # Apply logic: wtd_mean = -wtd; inundation = 1.0 where wtd_mean >= threshold, else NaN
                wtd_mean = -wtd_arr
                out = np.full(dst_shape, np.nan, dtype=np.float32)
                valid = (~np.isnan(wtd_mean)) & (~np.isnan(threshold))
                out[valid & (wtd_mean >= threshold)] = 1.0

                # Write result
                _write_geotiff(
                    fname_inund, out, base_profile,
                    compress=compress, bigtiff="IF_SAFER",
                    blocksize=blocksize, zlevel=zlevel, predictor=predictor
                )

            idt += datetime.timedelta(days=1)

def calculate_summary_perc_inundated(**kwargs):
    dt_start           = kwargs.get('dt_start',               None)
    dt_end             = kwargs.get('dt_end',                 None)
    inundation_raw_dir = kwargs.get('inundation_raw_dir',     None)
    inundation_sum_dir = kwargs.get('inundation_summary_dir', None)
    fname_dem          = kwargs.get('fname_dem',              None)
    verbose            = kwargs.get('verbose',                False)
    overwrite          = kwargs.get('overwrite',              False)

    if verbose:
        print('calling calculate_summary_perc_inundated')
    os.makedirs(inundation_sum_dir, exist_ok=True)

    dt_fmt = "%Y%m%d"
    start_s = dt_start.strftime(dt_fmt)
    end_s   = dt_end.strftime(dt_fmt)
    n_days = (dt_end - dt_start).days + 1
    fname_output = os.path.join(
        inundation_sum_dir,
        f"percent_inundated_grid_{start_s}_to_{end_s}.tiff"
    )
    if os.path.isfile(fname_output) and not overwrite:
        if verbose:
            print(f' found existing summary percent inundated grid {fname_output}')
        return fname_output
    if verbose:
        print(f' writing summary percent inundation grid {fname_output}')

    # Read base grid once
    with rasterio.open(fname_dem) as src:
        base_profile = src.profile.copy()
        sumgrid_data = src.read(1, out_dtype="int32")
        sumgrid_data[:] = 0  # Initialize to zeros

    # Accumulate inundation counts
    idt = dt_start
    while idt <= dt_end:
        dt_str = idt.strftime(dt_fmt)
        fname_inund_dti = os.path.join(inundation_raw_dir, f'inundation_{dt_str}.tiff')
        
        with rasterio.open(fname_inund_dti) as src:
            inun_data = src.read(1, out_dtype="int32")
            # Replace NaN with 0 directly in the array
            inun_data = np.nan_to_num(inun_data, nan=0.0).astype(np.int32)
            sumgrid_data += inun_data
        
        idt += datetime.timedelta(days=1)

    # Convert to percentage
    scale = 100.0 / float(n_days)
    perc_inun = sumgrid_data.astype(np.float32) * scale
    perc_inun[perc_inun <= 0.0] = np.nan

    # Write output
    _write_geotiff(fname_output, perc_inun, base_profile)
    
    return fname_output

def _zip_inundation(raw_dir, compression=zipfile.ZIP_LZMA, compresslevel=9):
    """
    Create a ZIP archive of 'raw_dir' including the directory itself as the top-level entry,
    then remove the original directory.

    Parameters:
        raw_dir       : path to the 'raw' directory
        compression   : zipfile compression (e.g., ZIP_STORED, ZIP_DEFLATED, ZIP_BZIP2, ZIP_LZMA)
        compresslevel : int or None; higher is smaller/slower (supported for DEFLATED/BZIP2/LZMA)

    Returns:
        - Path to the created ZIP file (string) if a ZIP is created.
        - None if the directory does not exist or is empty.

    Notes:
        - ZIP_DEFLATED is widely compatible; BZIP2/LZMA can compress smaller but may not open in all tools.
    """
    abs_raw = os.path.abspath(raw_dir)
    parent = os.path.dirname(abs_raw)
    zip_path = abs_raw + '.zip'
    kwargs = {}
    if compresslevel is not None:
        kwargs['compresslevel'] = compresslevel
    with zipfile.ZipFile(zip_path, mode='w', compression=compression, **kwargs) as zf:
        for dirpath, dirnames, filenames in os.walk(abs_raw):
            rel_dir = os.path.relpath(dirpath, parent)
            zf.write(dirpath, arcname=rel_dir)
            for name in filenames:
                full_path = os.path.join(dirpath, name)
                arcname = os.path.relpath(full_path, parent)
                zf.write(full_path, arcname=arcname)
    shutil.rmtree(abs_raw)
    return zip_path

def _check_exist(inundation_out_dir:str,dt_start:datetime.datetime,dt_end:datetime.datetime):
    idt = dt_start
    while idt <= dt_end:
        dt_str = idt.strftime('%Y%m%d')
        fname  = f'inundation_{dt_str}.tiff'
        if not os.path.isfile(os.path.join(inundation_out_dir,fname)):
            return True
        idt += datetime.timedelta(days=1)
    return False