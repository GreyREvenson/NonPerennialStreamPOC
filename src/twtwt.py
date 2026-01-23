import os,sys,math,datetime,geopandas,hf_hydrodata,rasterio,numpy,twtnamelist,multiprocessing,shapely,twtutils

def set_wtd_main(namelist:twtnamelist.Namelist):
    if namelist.options.verbose: print('calling set_wtd_main')
    hf_hydrodata.register_api_pin(namelist.options.hf_hydrodata_un, namelist.options.hf_hydrodata_pin)
    domain = geopandas.read_file(namelist.fnames.domain)
    args = list(zip([domain.iloc[[i]] for i in range(len(domain))],
                    [namelist.time.datetime_dim[0]]  * len(domain),
                    [namelist.time.datetime_dim[-1]] * len(domain),
                    [namelist.options.overwrite]     * len(domain),
                    [namelist.options.verbose]       * len(domain)))
    twtutils.call_func(_set_wtd,args,namelist)

def _set_wtd(domain:geopandas.GeoDataFrame,dt_start:datetime.datetime,dt_end:datetime.datetime,overwrite:bool,verbose:bool):
    try:
        e = _download_hydroframe_data(domain,dt_start,dt_end,overwrite,verbose)
        if e is not None: return e
    except Exception as e:
        return e

def _get_parflow_conus1_bbox(domain:geopandas.GeoDataFrame):
    latlon_tbounds = domain.to_crs(epsg=4326).total_bounds
    conus1grid_minx, conus1grid_miny = hf_hydrodata.from_latlon("conus1", latlon_tbounds[1], latlon_tbounds[0])
    conus1grid_maxx, conus1grid_maxy = hf_hydrodata.from_latlon("conus1", latlon_tbounds[3], latlon_tbounds[2])
    conus1grid_minx, conus1grid_miny = math.floor(conus1grid_minx), math.floor(conus1grid_miny)
    conus1grid_maxx, conus1grid_maxy = math.ceil(conus1grid_maxx),  math.ceil(conus1grid_maxy)
    return tuple([conus1grid_minx, conus1grid_miny, conus1grid_maxx, conus1grid_maxy])

def _hf_query(domain:geopandas.GeoDataFrame,dt_start:datetime.datetime,dt_end:datetime.datetime,verbose:bool):
    if verbose: print(f'calling _hf_query for domain {domain.iloc[0]['domain_id']}')
    try:
        grid_bounds    = _get_parflow_conus1_bbox(domain)
        start_date_str = dt_start.strftime('%Y-%m-%d')
        end_date_str   = (dt_end + datetime.timedelta(days=1)).strftime('%Y-%m-%d')
        options_wtd    = {"dataset"             : "conus1_baseline_mod",
                        "variable"            : "water_table_depth",
                        "temporal_resolution" : "daily",
                        "start_time"          : start_date_str,
                        "end_time"            : end_date_str,
                        "grid_bounds"         : grid_bounds}
        hf_data = hf_hydrodata.get_gridded_data(options_wtd)
        if hf_data is None or not hasattr(hf_data, 'shape'):
            sys.exit('ERROR hf_hydrodata query failed')
        expected_days = (dt_end - dt_start).days + 1
        if hf_data.shape[0] != expected_days:
            sys.exit('ERROR hf_hydrodata returned data of unexpected time length or invalid structure')
        return hf_data
    except Exception as e:
        return e

def _download_hydroframe_data(domain:geopandas.GeoDataFrame,dt_start:datetime.datetime,dt_end:datetime.datetime,overwrite:bool,verbose:bool):
    if verbose: print(f'calling _download_hydroframe_data for domain {domain.iloc[0]['domain_id']}')
    try:
        dirraw = os.path.join(domain.iloc[0]['input'],'wtd','raw')
        os.makedirs(dirraw, exist_ok=True)
        download = False
        dt = dt_start
        while dt <= dt_end:
            fname = os.path.join(dirraw,'wtd_'+dt.strftime('%Y%m%d')+'.tiff')
            if not os.path.isfile(fname) or overwrite:
                download = True
                break
            dt += datetime.timedelta(days=1)
        if download:
            conus1_proj, _, conus1_transform, conus1_shape = _get_parflow_conus1_grid_info()
            hf_data = _hf_query(domain, dt_start, dt_end, verbose)
            if isinstance(hf_data, Exception):
                return hf_data
            hf_conus1grid_temp = numpy.empty(shape=conus1_shape,dtype=numpy.float64)
            shp = shapely.ops.unary_union(domain.to_crs(conus1_proj).buffer(distance=1000).geometry)
            grid_bounds = _get_parflow_conus1_bbox(domain)
            for i in range(hf_data.shape[0]):
                dt = dt_start + datetime.timedelta(days=i)
                fname = os.path.join(dirraw,'wtd_'+dt.strftime('%Y%m%d')+'.tiff')
                if not os.path.isfile(fname) or overwrite:
                    hf_conus1grid_temp[grid_bounds[1]:grid_bounds[3],
                                       grid_bounds[0]:grid_bounds[2]] = hf_data[i,:,:]
                    with rasterio.io.MemoryFile() as memfile:
                        hf_conus1data = memfile.open(driver    = "GTiff", 
                                                    height    = hf_conus1grid_temp.shape[0], 
                                                    width     = hf_conus1grid_temp.shape[1], 
                                                    crs       = conus1_proj, 
                                                    transform = conus1_transform, 
                                                    nodata    = numpy.nan, 
                                                    count     = 1, 
                                                    dtype     = numpy.float64)
                        hf_conus1data.write(hf_conus1grid_temp,1)
                        wtd_data, wtd_transform = rasterio.mask.mask(dataset    = hf_conus1data, 
                                                                    shapes      = [shp], 
                                                                    crop        = True, 
                                                                    all_touched = True, 
                                                                    filled      = True, 
                                                                    pad         = True,
                                                                    nodata      = numpy.nan)
                        wtd_meta = hf_conus1data.meta
                        wtd_meta.update({"driver"   : "GTiff",
                                        "height"    : wtd_data.shape[1],
                                        "width"     : wtd_data.shape[2],
                                        "transform" : wtd_transform, 
                                        "nodata"    : numpy.nan})
                        with rasterio.open(fname,'w',**wtd_meta) as wtd_dataset:
                            wtd_dataset.write(wtd_data[0,:,:],1)
            del hf_data, hf_conus1grid_temp
            return None
        else:
            return None
    except Exception as e:
        return e

def _get_latlon_parflow_grid(grid_minx,grid_miny,grid_maxx,grid_maxy):
    """Get latlon bbox from ParFlow CONUS1 grid xy bbox"""
    latlon_bounds = hf_hydrodata.to_latlon("conus1", *[grid_minx,grid_miny,grid_maxx,grid_maxy])
    lon_min = latlon_bounds[1]
    lat_min = latlon_bounds[0]
    lon_max = latlon_bounds[3]
    lat_max = latlon_bounds[2]
    return lon_min,lat_min,lon_max,lat_max

def _get_parflow_conus1_bbox(domain:geopandas.GeoDataFrame):
    latlon_tbounds = domain.to_crs(epsg=4326).total_bounds
    conus1grid_minx, conus1grid_miny = hf_hydrodata.from_latlon("conus1", latlon_tbounds[1], latlon_tbounds[0])
    conus1grid_maxx, conus1grid_maxy = hf_hydrodata.from_latlon("conus1", latlon_tbounds[3], latlon_tbounds[2])
    conus1grid_minx, conus1grid_miny = math.floor(conus1grid_minx), math.floor(conus1grid_miny)
    conus1grid_maxx, conus1grid_maxy = math.ceil(conus1grid_maxx),  math.ceil(conus1grid_maxy)
    return tuple([conus1grid_minx, conus1grid_miny, conus1grid_maxx, conus1grid_maxy])

def _set_parflow_conus2_bbox(namelist:twtnamelist.Namelist):
    """Set domain ParFlow bounding box"""
    if namelist.options.verbose: print('calling _get_parflow_conus2_bbox')
    if not os.path.isfile(namelist.fnames.domain): sys.exit('ERROR could not find '+namelist.fnames.domain)
    conus2grid_minx, conus2grid_miny = hf_hydrodata.from_latlon("conus2", namelist.bbox_domain.lat_min, namelist.bbox_domain.lon_min)
    conus2grid_maxx, conus2grid_maxy = hf_hydrodata.from_latlon("conus2", namelist.bbox_domain.lat_max, namelist.bbox_domain.lon_max)
    conus2grid_minx, conus2grid_miny = math.floor(conus2grid_minx), math.floor(conus2grid_miny)
    conus2grid_maxx, conus2grid_maxy = math.ceil(conus2grid_maxx),  math.ceil(conus2grid_maxy)
    return conus2grid_minx,conus2grid_miny,conus2grid_maxx,conus2grid_maxy

def _get_parflow_conus1_grid_info():
    """Parflow CONUS1 grid info - see https://hf-hydrodata.readthedocs.io/en/latest/available_grids.html"""
    conus1_proj      = '+proj=lcc +lat_1=33 +lat_2=45 +lon_0=-96.0 +lat_0=39 +a=6378137.0 +b=6356752.31'
    conus1_spatext   = tuple([-121.47939483437318, 31.651836025255015, -76.09875469594509, 50.49802132270979])
    conus1_transform = rasterio.transform.Affine(1000.0,0.0,-1885055.4995,0.0,1000.0,-604957.0654)
    conus1_shape     = (1888,3342)
    return conus1_proj,conus1_spatext,conus1_transform,conus1_shape

def _get_parflow_conus2_grid_info():
    """Parflow CONUS2 grid info - see https://hf-hydrodata.readthedocs.io/en/latest/available_grids.html"""
    conus2_proj      = '+proj=lcc +lat_1=30 +lat_2=60 +lon_0=-97.0 +lat_0=40.0000076294444 +a=6370000.0 +b=6370000'
    conus2_spatext   = tuple([-126.88755692881833, 21.8170599154073, -64.7677149695924, 53.20274381640737])
    conus2_transform = rasterio.transform.Affine(1000.0,0.0,-2208000.30881173,0.0,1000.0,-1668999.65483222)
    conus2_shape     = (3256,4442)
    return conus2_proj,conus2_spatext,conus2_transform,conus2_shape