import os,sys,math,datetime,geopandas,hf_hydrodata,rasterio,numpy,twtnamelist,multiprocessing,shapely

def set_wtd(namelist:twtnamelist.Namelist):
    """Get water table depth data for domain"""
    if namelist.options.verbose: print('calling set_wtd')
    _download_hydroframe_data_main(namelist)
    #_calc_avgwtd_grid(namelist)

def _pp_func(func,args:tuple,namelist:twtnamelist.Namelist,MAX_PP_TIME_LENGTH_SECONDS:int=900):
    with multiprocessing.Pool(processes=min(namelist.options.core_count, len(args))) as pool:
        if isinstance(args[0], list) or isinstance(args[0], tuple):
            _async_out = [pool.apply_async(func, arg) for arg in args]
        else:
            _async_out = [pool.apply_async(func, (arg,)) for arg in args]
        for _out in _async_out:
            try: 
                _out.get(timeout=MAX_PP_TIME_LENGTH_SECONDS)
            except multiprocessing.TimeoutError: 
                print(f'WARNING: multiprocessing timeout {func.__name__}')
        print(_async_out)

def _download_hydroframe_data_main(namelist:twtnamelist.Namelist):
    if namelist.options.verbose: print('calling _download_hydroframe_data_main')
    #TODO change to catch timeout errors for _breach_dem and call _breach_dem_filled only for those
    domain = geopandas.read_file(namelist.fnames.domain)
    args = list()
    for i,r in domain.iterrows():
        if not all([os.path.isfile(os.path.join(r['dirname_wtd_raw'],f'wtd_{dt.strftime('%Y%m%d')}')) 
                    for dt in namelist.time.datetime_dim]) or namelist.options.overwrite_flag:
            args.append([domain.loc[[i]],namelist.time.datetime_dim[0],namelist.time.datetime_dim[-1]])
    if len(args) > 0:
        if namelist.options.pp: 
            _pp_func(_download_hydroframe_data,args,namelist)
        else:
            for arg in args: _download_hydroframe_data(*arg)
    for _,r in domain.iterrows():
        if not all([os.path.isfile(os.path.join(r['dirname_wtd_raw'],f'wtd_{dt.strftime('%Y%m%d')}.tiff')) 
                    for dt in namelist.time.datetime_dim]):
            sys.exit(f'ERROR _download_hydroframe_data failed for domain {r['domain_id']}')

def _download_hydroframe_data(domain:geopandas.GeoDataFrame,dt_start:datetime.datetime,dt_end:datetime.datetime):
    try:
        conus1_proj      = '+proj=lcc +lat_1=33 +lat_2=45 +lon_0=-96.0 +lat_0=39 +a=6378137.0 +b=6356752.31'
        conus1_transform = rasterio.transform.Affine(1000.0,0.0,-1885055.4995,0.0,1000.0,-604957.0654)
        conus1_shape     = (1888,3342)
        latlon_tbounds = domain.to_crs(epsg=4326).total_bounds
        conus1grid_minx, conus1grid_miny = hf_hydrodata.from_latlon("conus1", latlon_tbounds[1], latlon_tbounds[0])
        conus1grid_maxx, conus1grid_maxy = hf_hydrodata.from_latlon("conus1", latlon_tbounds[3], latlon_tbounds[2])
        conus1grid_minx, conus1grid_miny = math.floor(conus1grid_minx), math.floor(conus1grid_miny)
        conus1grid_maxx, conus1grid_maxy = math.ceil(conus1grid_maxx),  math.ceil(conus1grid_maxy)
        start_date_str = dt_start.strftime('%Y-%m-%d')
        end_date_str   = (dt_end + datetime.timedelta(days=1)).strftime('%Y-%m-%d')
        options_wtd =  {"dataset"             : "conus1_baseline_mod",
                        "variable"            : "water_table_depth",
                        "temporal_resolution" : "daily",
                        "start_time"          : start_date_str,
                        "end_time"            : end_date_str,
                        "grid_bounds"         : [conus1grid_minx,
                                                conus1grid_miny,
                                                conus1grid_maxx,
                                                conus1grid_maxy]}
        hf_data = hf_hydrodata.get_gridded_data(options_wtd)
        if hf_data is None or not hasattr(hf_data, 'shape'):
            sys.exit('ERROR hf_hydrodata query failed')
        expected_days = (dt_end - dt_start).days + 1
        if hf_data.shape[0] != expected_days:
            sys.exit('ERROR hf_hydrodata returned data of unexpected time length or invalid structure')
        hf_conus1grid_temp = numpy.empty(shape=conus1_shape,dtype=numpy.float64)
        for i in range(hf_data.shape[0]):
            hf_conus1grid_temp[conus1grid_miny:conus1grid_maxy,
                                conus1grid_minx:conus1grid_maxx] = hf_data[i,:,:]
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
                                                            shapes      = [shapely.ops.unary_union(domain.to_crs(conus1_proj).buffer(distance=1000).geometry)], 
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
                dt = dt_start + datetime.timedelta(days=i)
                fname = os.path.join(domain.iloc[0]['dirname_wtd_raw'],
                                    'wtd_'+dt.strftime('%Y%m%d')+'.tiff')
                with rasterio.open(fname,'w',**wtd_meta) as wtd_dataset:
                    wtd_dataset.write(wtd_data[0,:,:],1)
        del hf_data
        return ''
    except Exception as e:
        return f'{domain.iloc[0]['domain_id']} {e}'

def _calc_avgwtd_grid(namelist:twtnamelist.Namelist):
    """Calculate mean wtd"""
    if namelist.options.verbose: print('calling _calc_avgwtd_grid')
    if not os.path.isfile(namelist.fnames.domain_mask):
        sys.exit(f'ERROR could not find {namelist.fnames.domain_mask}')
    start_string = namelist.time.datetime_dim[0].strftime('%Y%m%d')
    end_string   = namelist.time.datetime_dim[len(namelist.time.datetime_dim)-1].strftime('%Y%m%d')
    fname        = "".join(['mean_wtd_',start_string,'_to_',end_string,'.tiff'])
    fname_output = os.path.join(namelist.dirnames.output_summary,fname)
    if not os.path.isfile(fname_output) or namelist.options.overwrite_flag:
        domain_mask  = rasterio.open(namelist.fnames.domain_mask,'r').read(1) 
        sum = numpy.zeros(shape=domain_mask.shape,dtype=numpy.float64)
        cnt = 0
        for i in range(len(namelist.time.datetime_dim)):
            idatetime = namelist.time.datetime_dim[i]
            fname_wtd = os.path.join(namelist.dirnames.wtd_parflow_resampled,
                                     f'wtd_{idatetime.strftime("%Y%m%d")}_{namelist.options.name_resample_method}.tiff')
            if os.path.isfile(fname_wtd):
                wtd_data = rasterio.open(fname_wtd,'r').read(1)
                wtd_data = numpy.where(domain_mask==1,wtd_data,0.)
                sum += wtd_data
                cnt += 1
            else:
                sys.exit(f'ERROR could not find {fname_wtd}')
        mean_wtd = sum / float(len(namelist.time.datetime_dim))
        mean_wtd = numpy.where(domain_mask==1,mean_wtd,numpy.nan)
        with rasterio.open(fname_output, "w", **rasterio.open(fname_wtd,'r').meta) as summary_dataset:
            summary_dataset.write(mean_wtd,1)

def _get_latlon_parflow_grid(grid_minx,grid_miny,grid_maxx,grid_maxy):
    """Get latlon bbox from ParFlow CONUS1 grid xy bbox"""
    latlon_bounds = hf_hydrodata.to_latlon("conus1", *[grid_minx,grid_miny,grid_maxx,grid_maxy])
    lon_min = latlon_bounds[1]
    lat_min = latlon_bounds[0]
    lon_max = latlon_bounds[3]
    lat_max = latlon_bounds[2]
    return lon_min,lat_min,lon_max,lat_max

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