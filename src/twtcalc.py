
import os,numpy,shutil,datetime,rioxarray,xarray

def calculate_strm_permanence(
    *,
    fname_perc_inundation=None,
    fname_strm_mask=None,
    verbose=False,
    overwrite=False,
    compress="LZW",            # unused; kept for API compatibility
    atol=1e-6                  # tolerance for treating 100% as perennial
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
    if nd_perc is not None and not (isinstance(nd_perc, float) and numpy.isnan(nd_perc)):
        perc_da = perc_da.where(perc_da != nd_perc, other=numpy.nan)

    nd_mask = mask_da.rio.nodata
    if nd_mask is not None and not (isinstance(nd_mask, float) and numpy.isnan(nd_mask)):
        mask_da = mask_da.where(mask_da != nd_mask, other=numpy.nan)

    # Build boolean stream mask: True where mask == 1 (NaNs -> False)
    stream_mask_bool = (mask_da == 1)

    # Restrict perc to streams; outside -> NaN
    perc_on_streams = perc_da.where(stream_mask_bool, other=numpy.nan)

    # Perennial where approx 100% within tolerance (NaNs evaluate to False)
    is_perennial = numpy.isfinite(perc_on_streams) & (numpy.abs(perc_on_streams - 100.0) <= atol)
    perennial_da = xarray.where(is_perennial, 1.0, numpy.nan).astype("float32")

    # Non-perennial where on streams, finite, >0 and not perennial
    nonperennial_mask = stream_mask_bool & numpy.isfinite(perc_da) & (perc_da > 0.0) & (~is_perennial)
    nonperennial_da = xarray.where(nonperennial_mask, perc_da, numpy.nan).astype("float32")

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

def calculate_summary_perc_inundated(**kwargs):
    """
    Calculates the percent of days inundated for a date range and writes a GeoTIFF.

    kwargs:
      dt_start (datetime): inclusive start date
      dt_end   (datetime): exclusive end date
      inundation_raw_dir (str): directory with daily inundation_{YYYYMMDD}.tiff
      inundation_summary_dir (str): output directory
      fname_dem (str): template raster for grid/CRS/mask
      verbose (bool): print progress
      overwrite (bool): overwrite existing output

    Returns:
      str: path to the output GeoTIFF
    """
    dt_start           = kwargs.get("dt_start", None)
    dt_end             = kwargs.get("dt_end", None)
    inundation_raw_dir = kwargs.get("inundation_raw_dir", None)
    inundation_sum_dir = kwargs.get("inundation_summary_dir", None)
    fname_dem          = kwargs.get("fname_dem", None)
    verbose            = kwargs.get("verbose", False)
    overwrite          = kwargs.get("overwrite", False)

    # Validation
    if not (dt_start and dt_end and inundation_raw_dir and inundation_sum_dir and fname_dem):
        raise ValueError("Missing required kwarg(s): dt_start, dt_end, inundation_raw_dir, inundation_summary_dir, fname_dem.")
    if dt_end <= dt_start:
        raise ValueError("dt_end must be greater than dt_start (exclusive end).")

    os.makedirs(inundation_sum_dir, exist_ok=True)
    fname_output = os.path.join(
        inundation_sum_dir,
        f"percent_inundated_grid_{dt_start.strftime('%Y%m%d')}_to_{dt_end.strftime('%Y%m%d')}.tiff",
    )

    if os.path.isfile(fname_output) and not overwrite:
        if verbose:
            print(f"found existing summary percent inundation grid {fname_output}")
        return fname_output

    if verbose:
        print("calling calculate_summary_perc_inundated")
        print(f"writing summary percent inundation grid {fname_output}")

    # Load DEM as template (and close the file)
    with rioxarray.open_rasterio(fname_dem, masked=True) as dem_da:
        dem = dem_da.sel(band=1).load()
    dem_mask = dem.isnull().data  # boolean mask where DEM is nodata
    height = dem.sizes.get("y")
    width = dem.sizes.get("x")

    # Accumulator (float32 for generality)
    acc = numpy.zeros((height, width), dtype=numpy.float32)

    # Iterate dates [dt_start, dt_end) and accumulate
    total_days = 0
    idt = dt_start
    while idt < dt_end:
        dt_str = idt.strftime("%Y%m%d")
        fpath = os.path.join(inundation_raw_dir, f"inundation_{dt_str}.tiff")
        if not os.path.exists(fpath):
            raise FileNotFoundError(f"Missing daily inundation file: {fpath}")

        with rioxarray.open_rasterio(fpath, masked=True) as da:
            inun = da.sel(band=1).load()

        # Shape check
        if inun.sizes.get("y") != height or inun.sizes.get("x") != width:
            raise ValueError(
                f"Raster shape mismatch for {fpath}: expected {width}x{height}, "
                f"got {inun.sizes.get('x')}x{inun.sizes.get('y')}"
            )

        # Treat input nodata as 0 for summation
        arr = inun.fillna(0).values.astype(numpy.float32, copy=False)
        acc += arr

        total_days += 1
        if verbose and (total_days % 10 == 0):
            print(f"  accumulated {total_days} rasters (latest: {dt_str})")

        idt += datetime.timedelta(days=1)

    if total_days == 0:
        raise ValueError("Empty date range (no days between dt_start and dt_end).")

    # Compute percentage and apply masks
    perc_np = (acc / float(total_days)) * 100.0
    perc_np[perc_np <= 0.0] = numpy.nan       # set 0% to NaN
    perc_np[dem_mask] = numpy.nan             # preserve DEM nodata

    # Build output DataArray preserving geospatial metadata from DEM
    perc_da = dem.copy(data=perc_np.astype(numpy.float32))
    perc_da.rio.write_nodata(numpy.nan, inplace=True)

    # Remove CF-encoding keys that can conflict during write
    for key in ("_FillValue", "missing_value", "scale_factor", "add_offset"):
        perc_da.attrs.pop(key, None)
        perc_da.encoding.pop(key, None)
    # Clear any leftover encoding to avoid serialization conflicts
    perc_da.encoding = {}

    # Write output
    perc_da.rio.to_raster(
        fname_output,
        compress="LZW",
    )

    if verbose:
        print(f"wrote {fname_output}")

    return fname_output

def calculate_inundation(
    *,
    dt_start,
    dt_end,
    wtd_raw_dir,
    inundation_out_dir,
    fname_twi,
    fname_twi_mean,
    fname_soil_transmissivity,
    wtd_resampled_dir=None,
    verbose=False,
    overwrite=False,
    chunks=None,
):
    """
    Compute inundation rasters for dt_start..dt_end inclusive using rioxarray only.

    Inundation is 1 where: WTD_resampled <= (twi - twi_mean) / soil_transmissivity, else 0.

    Notes:
      - Precomputes the static term (twi - twi_mean) / soil_transmissivity once per run.
      - Uses nearest-neighbor resampling by default (no rasterio imports); if you must use bilinear,
        you'll need to allow rasterio enums, or accept the nearest-neighbor default here.
    """
    if verbose:
        print("calling calculate_inundation (rioxarray-only)")

    do_calc = overwrite or _check_exist(inundation_out_dir, dt_start, dt_end)
    if not do_calc:
        if verbose:
            print(f"found existing inundation calculations in {inundation_out_dir}")
        return

    if verbose:
        print(f"writing output to {inundation_out_dir}")
    os.makedirs(inundation_out_dir, exist_ok=True)
    if wtd_resampled_dir is not None:
        os.makedirs(wtd_resampled_dir, exist_ok=True)
    #
    #
    twi = rioxarray.open_rasterio(filename=fname_twi, masked=True, chunks=chunks).sel(band=1)
    twi_mean = rioxarray.open_rasterio(filename=fname_twi_mean, masked=True, chunks=chunks).sel(band=1)
    soil_transm = rioxarray.open_rasterio(filename=fname_soil_transmissivity, masked=True, chunks=chunks).sel(band=1)
    twi_mean = twi_mean.rio.reproject_match(twi)
    soil_transm = soil_transm.rio.reproject_match(twi)
    if twi.dtype != numpy.float32:
        twi = twi.astype(numpy.float32)
    if twi_mean.dtype != numpy.float32:
        twi_mean = twi_mean.astype(numpy.float32)
    if soil_transm.dtype != numpy.float32:
        soil_transm = soil_transm.astype(numpy.float32)
    #
    #
    threshold = (twi - twi_mean) / soil_transm # Static threshold reused for all days
    #
    #
    idt = dt_start
    one_day = datetime.timedelta(days=1)
    while idt <= dt_end:
        #
        #
        dt_str = idt.strftime("%Y%m%d")
        if verbose: print(f' processing {dt_str}')
        fname_wtd_raw = os.path.join(wtd_raw_dir, f"wtd_{dt_str}.tiff")
        fname_inund = os.path.join(inundation_out_dir, f"inundation_{dt_str}.tiff")
        if not os.path.isfile(fname_wtd_raw):
            raise FileNotFoundError(f"calculate_inundation could not find {fname_wtd_raw}")
        #
        #
        if overwrite or (not os.path.isfile(fname_inund)):
            with rioxarray.open_rasterio(fname_wtd_raw, masked=True, chunks=chunks) as wtd_da:
                wtd_da = wtd_da.squeeze(drop=True)
                wtd_resampled = wtd_da.rio.reproject_match(twi)  # default resampling
            inund = (wtd_resampled <= threshold).astype(numpy.uint8) # Inundation mask: 1 where condition holds, else 0
            inund.rio.write_crs(twi.rio.crs, inplace=True)
            inund.rio.write_nodata(0, inplace=True)
            inund.rio.to_raster(fname_inund)
            if wtd_resampled_dir is not None:
                fname_wtd_resampled = os.path.join(wtd_resampled_dir, f"wtd_{dt_str}.tiff")
                wtd_resampled.rio.write_crs(twi.rio.crs, inplace=True)
                wtd_resampled.rio.to_raster(fname_wtd_resampled)
        idt += one_day
    if verbose:
        print("calculate_inundation completed.")

def _zip_iundation(dt):
    domain = dt['domain']
    dirraw = os.path.join(domain.iloc[0]['output'],'raw')
    try:
        if os.path.isdir(dirraw) and len(os.listdir(dirraw)) > 0:
            shutil.make_archive(dirraw, 'zip', dirraw)
            shutil.rmtree(dirraw)
        return None
    except Exception as e:
        return e

def _check_exist(inundation_out_dir:str,dt_start:datetime.datetime,dt_end:datetime.datetime):
    idt = dt_start
    while idt <= dt_end:
        dt_str = idt.strftime('%Y%m%d')
        fname  = f'inundation_{dt_str}.tiff'
        if not os.path.isfile(os.path.join(inundation_out_dir,fname)):
            return True
        idt += datetime.timedelta(days=1)
    return False