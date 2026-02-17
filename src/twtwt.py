import os,sys,math,datetime,geopandas,hf_hydrodata,rasterio,numpy,twtnamelist,shapely

def _get_parflow_conus1_bbox(domain:geopandas.GeoDataFrame):
    latlon_tbounds = domain.to_crs(epsg=4326).total_bounds
    conus1grid_minx, conus1grid_miny = hf_hydrodata.from_latlon("conus1", latlon_tbounds[1], latlon_tbounds[0])
    conus1grid_maxx, conus1grid_maxy = hf_hydrodata.from_latlon("conus1", latlon_tbounds[3], latlon_tbounds[2])
    conus1grid_minx, conus1grid_miny = math.floor(conus1grid_minx), math.floor(conus1grid_miny)
    conus1grid_maxx, conus1grid_maxy = math.ceil(conus1grid_maxx),  math.ceil(conus1grid_maxy)
    return tuple([conus1grid_minx, conus1grid_miny, conus1grid_maxx, conus1grid_maxy])

def hf_query_nc(**kwargs):
    dt_start = kwargs.get('dt_start', None)
    dt_end   = kwargs.get('dt_end',   None)
    savedir  = kwargs.get('savedir',  None)
    verbose  = kwargs.get('verbose',  False)
    huc_id   = kwargs.get('huc_id',   None)
    domain   = kwargs.get('domain',   None)
    if verbose: print('calling hf_query')
    if dt_start is None or not isinstance(dt_start,datetime.datetime):
        raise Exception(f'hf_query missing required argument dt_start or is not a valid datetime')
    if dt_end is None or not isinstance(dt_end,datetime.datetime):
        raise Exception(f'hf_query missing required argument dt_end or is not a valid datetime')
    if savedir is None:
        raise Exception(f'hf_query missing required argument savedir')
    if huc_id is not None and not isinstance(huc_id,str):
        raise Exception(f'hf_query argument huc_id is not valid str')
    if domain is not None and not isinstance(domain,geopandas.GeoDataFrame):
        raise Exception(f'hf_query argument domain is not a valid geopandas.GeoDataFrame')
    if not os.path.isdir(savedir): os.makedirs(savedir,exist_ok=True)
    start_date_str = dt_start.strftime('%Y-%m-%d')
    end_date_str   = (dt_end + datetime.timedelta(days=1)).strftime('%Y-%m-%d')
    options_wtd    = {"dataset"             : "conus1_baseline_mod",
                      "temporal_resolution" : "daily",
                      "start_time"          : start_date_str,
                      "end_time"            : end_date_str}
    if huc_id is not None: options_wtd['huc_id'] = huc_id
    else:                  options_wtd['grid_bounds'] = _get_parflow_conus1_bbox(domain)
    if verbose: 
        hf_hydrodata.get_gridded_files(options_wtd,
                                       variables=['water_table_depth'],
                                       filename_template=os.path.join(savedir,"{dataset}_{variable}_{wy}.nc"),
                                       verbose=True)
    else: 
        hf_hydrodata.get_gridded_files(options_wtd,
                                       variables=['water_table_depth'],
                                       filename_template=os.path.join(savedir,"{dataset}_{variable}_{wy}.nc"))

def hf_query(**kwargs):
    dt_start = kwargs.get('dt_start', None)
    dt_end   = kwargs.get('dt_end',   None)
    huc_id   = kwargs.get('huc_id',   None)
    domain   = kwargs.get('domain',   None)
    if dt_start is None or not isinstance(dt_start,datetime.datetime):
        raise Exception(f'hf_query missing required argument dt_start or is not a valid datetime')
    if dt_end is None or not isinstance(dt_end,datetime.datetime):
        raise Exception(f'hf_query missing required argument dt_end or is not a valid datetime')
    if huc_id is not None and not isinstance(huc_id,str):
        raise Exception(f'hf_query argument huc_id is not valid str')
    if huc_id is not None and len(huc_id) not in (2,4,6,8,10):
        raise Exception(f'hf_query argument huc_id must be level 2, 4, 6, 8, or 10')
    if domain is not None and not isinstance(domain,geopandas.GeoDataFrame):
        raise Exception(f'hf_query argument domain is not a valid geopandas.GeoDataFrame')
    if domain is None and huc_id is None:
        raise Exception(f'hf_query missing required argument domain or huc_id')
    start_date_str = dt_start.strftime('%Y-%m-%d')
    end_date_str   = (dt_end + datetime.timedelta(days=1)).strftime('%Y-%m-%d')
    options_wtd    = {"dataset"             : "conus1_baseline_mod",
                      "variable"            : "water_table_depth",
                      "temporal_resolution" : "daily",
                      "start_time"          : start_date_str,
                      "end_time"            : end_date_str}
    if huc_id is not None: options_wtd['huc_id'] = huc_id
    else:                  options_wtd['grid_bounds'] = _get_parflow_conus1_bbox(domain)
    hf_data = hf_hydrodata.get_gridded_data(options_wtd)
    if hf_data is None: raise Exception(f'hf_query call to hf_hydrodata.get_gridded_data failed - result is None')
    expected_days = (dt_end - dt_start).days + 1
    if hf_data.shape[0] != expected_days:
        raise Exception(f'hf_hydrodata returned data of unexpected time length or invalid structure')
    return hf_data

def _set_download_flag(**kwargs):
    dt_start  = kwargs.get('dt_start',  None)
    dt_end    = kwargs.get('dt_end',    None)
    savedir   = kwargs.get('savedir',   None)
    overwrite = kwargs.get('overwrite', False)
    download_flag = False
    idt = dt_start
    while idt <= dt_end:
        fname = os.path.join(savedir,'wtd_'+idt.strftime('%Y%m%d')+'.tiff')
        if not os.path.isfile(fname) or overwrite:
            download_flag = True
            break
        idt += datetime.timedelta(days=1)
    return download_flag

def download_hydroframe_data(**kwargs):
    domain    = kwargs.get('domain',    None)
    dt_start  = kwargs.get('dt_start',  None)
    dt_end    = kwargs.get('dt_end',    None)
    savedir   = kwargs.get('savedir',   None)
    verbose   = kwargs.get('verbose',   False)
    overwrite = kwargs.get('overwrite', False)
    if verbose: print('calling download_hydroframe_data')
    if domain is None or not isinstance(domain,geopandas.GeoDataFrame):
        raise Exception(f'download_hydroframe_data missing required argument domain or is not a valid geopandas.GeoDataFrame')
    if dt_start is None or not isinstance(dt_start,datetime.datetime):
        raise Exception(f'download_hydroframe_data missing required argument dt_start or is not a valid datetime')
    if dt_end is None or not isinstance(dt_end,datetime.datetime):
        raise Exception(f'download_hydroframe_data missing required argument dt_end or is not a valid datetime')
    if savedir is None:
        raise Exception(f'download_hydroframe_data missing required argument savedir')
    if not os.path.isdir(savedir): os.makedirs(savedir,exist_ok=True)
    download = _set_download_flag(**kwargs)
    if download:
        if verbose: print(f' using hf_hydrodata to download parflow water table depth simulations to {savedir}')
        conus1_proj, _, conus1_transform, conus1_shape = _get_parflow_conus1_grid_info()
        domain        = geopandas.GeoDataFrame(domain.drop(columns=['geometry']), 
                                               geometry=domain.to_crs(conus1_proj).buffer(distance=1000),              
                                               crs=conus1_proj)
        kwargs['domain']   = domain # overwriting with buffered gdf
        hf_data            = hf_query(**kwargs)
        hf_conus1grid_temp = numpy.empty(shape=conus1_shape,dtype=numpy.float64)
        grid_bounds        = _get_parflow_conus1_bbox(domain)
        for i in range(hf_data.shape[0]):
            idt = dt_start + datetime.timedelta(days=i)
            fname = os.path.join(savedir,'wtd_'+idt.strftime('%Y%m%d')+'.tiff')
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
                                                                shapes      = [domain.geometry.union_all()], 
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
    else:
        if verbose: print(f' using existing parflow simulation data in {savedir}')

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