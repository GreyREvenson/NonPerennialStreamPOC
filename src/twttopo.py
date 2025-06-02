import os,sys,numpy,geopandas,py3dep,rasterio,shutil,twtnamelist,pygeohydro,whitebox,shapely
from scipy.ndimage import uniform_filter
import multiprocessing

def set_topo(namelist:twtnamelist.Namelist):
    if namelist.options.verbose: print('calling calc_topo')
    if os.path.isfile(namelist.fnames.dem_user):
        _break_dem_main(namelist)
    else: 
        _download_dem_main(namelist)
    _set_huc_crs(namelist)
    _set_huc_mask_main(namelist)
    _breach_dem_main(namelist)
    _calc_facc_main(namelist)
    _calc_strm_mask_main(namelist)
    _calc_slope_main(namelist)
    _calc_twi_main(namelist)
    _calc_twi_mean_main(namelist)
    #_mask_pp_grids(namelist)
    #_mosaic_tiffs_main(namelist)

def _set_huc_crs(namelist:twtnamelist.Namelist):
    hucs = geopandas.read_file(namelist.fnames.hucs)
    crss = [rasterio.open(fname,'r').crs.to_string() for fname in hucs['fname_dem'].tolist()]
    if len(set(crss)) != 1: sys.exit('ERROR _set_huc_crs dems have multiple crs')
    if hucs.crs != crss[0]:
        hucs = hucs.to_crs(crs=crss[0])
        hucs.to_file(namelist.fnames.hucs,index=True)

def _pp_func(func,args:tuple,namelist:twtnamelist.Namelist,MAX_PP_TIME_LENGTH_SECONDS:int=900):
    with multiprocessing.Pool(processes=min(namelist.pp.core_count, len(args))) as pool:
        if isinstance(args[0], list) or isinstance(args[0], tuple):
            _async_out = [pool.apply_async(func, arg) for arg in args]
        else:
            _async_out = [pool.apply_async(func, (arg,)) for arg in args]
        for _out in _async_out:
            try: _out.get(timeout=MAX_PP_TIME_LENGTH_SECONDS)
            except multiprocessing.TimeoutError: pass

def _breach_dem_main(namelist:twtnamelist.Namelist):
    if namelist.options.verbose: print('calling _breach_dem_main')
    #TODO change to catch timeout errors for _breach_dem and call _breach_dem_filled only for those
    hucs = geopandas.read_file(namelist.fnames.hucs)
    args = [hucs.iloc[[i]] for i in range(len(hucs))]
    args = [arg for arg in args
            if not os.path.isfile(arg.iloc[0]['fname_dem_breached']) or namelist.options.overwrite_flag]
    if len(args) > 0:
        if namelist.pp.flag: 
            _pp_func(_breach_dem,args,namelist)
        else:
            for arg in args: _breach_dem(arg)
    args = [arg for arg in args
            if not os.path.isfile(arg.iloc[0]['fname_dem_breached'])]
    if len(args) > 0:
        if namelist.options.verbose: 
            print(f'calling _breach_dem_filled for')
            for hucid in [hucid for hucid,row in hucs.iterrows()
                          if not os.path.isfile(row['fname_dem_breached'])]: 
                print(f'  {hucid}')
        if namelist.pp.flag:
            _pp_func(_breach_dem_fill,args,namelist)
        else:
            for arg in args: _breach_dem_fill(arg)
    if not all([os.path.isfile(fname) 
                for fname in hucs['fname_dem_breached'].to_list()]):
        print('ERROR _breach_dem and _breach_dem_filled failed for hucs')
        for hucid in [hucid for hucid,row in hucs.iterrows()
                      if not os.path.isfile(row['fname_dem_breached'])]: 
            print(f'  {hucid}')
        sys.exit(0)

def _calc_facc_main(namelist:twtnamelist.Namelist):
    if namelist.options.verbose: print('calling _calc_facc_main')
    hucs = geopandas.read_file(namelist.fnames.hucs)
    args = [hucs.iloc[[i]] for i in range(len(hucs))]
    args = [arg for arg in args
            if not os.path.isfile(arg.iloc[0]['fname_facc']) or namelist.options.overwrite_flag]
    if len(args) > 0:
        if namelist.pp.flag:
            _pp_func(_calc_facc,args,namelist)
        else:
            for arg in args: _calc_facc(arg)
    if not all([os.path.isfile(fname) 
                for fname in hucs['fname_facc'].to_list()]):
        print('ERROR _calc_facc failed for hucs')
        for hucid in [hucid for hucid,row in hucs.iterrows()
                      if not os.path.isfile(row['fname_facc'])]: 
            print(f'  {hucid}')
        sys.exit(0)

def _calc_strm_mask_main(namelist:twtnamelist.Namelist):
    if namelist.options.verbose: print('calling _calc_stream_mask')
    hucs = geopandas.read_file(namelist.fnames.hucs)
    args = list(zip([hucs.iloc[[i]] for i in range(len(hucs))],
                    [namelist.vars.facc_strm_threshold]*len(hucs)))
    args = [arg for arg in args
            if not os.path.isfile(arg[0].iloc[0]['fname_strm_mask']) or namelist.options.overwrite_flag]
    if len(args) > 0:
        if namelist.pp.flag:
            _pp_func(_calc_stream_mask,args,namelist)
        else:
            for arg in args: _calc_stream_mask(arg)
    if not all([os.path.isfile(fname) 
                for fname in hucs['fname_strm_mask'].to_list()]):
        print('ERROR _calc_stream_mask failed for hucs')
        for hucid in [hucid for hucid,row in hucs.iterrows()
                      if not os.path.isfile(row['fname_strm_mask'])]: 
            print(f'  {hucid}')
        sys.exit(0)

def _calc_slope_main(namelist:twtnamelist.Namelist):
    if namelist.options.verbose: print('calling _calc_slope')
    hucs = geopandas.read_file(namelist.fnames.hucs)
    args = [hucs.iloc[[i]] for i in range(len(hucs))]
    args = [arg for arg in args
            if not os.path.isfile(arg.iloc[0]['fname_slope']) or namelist.options.overwrite_flag]
    if len(args) > 0:
        if namelist.pp.flag:
            _pp_func(_calc_slope,args,namelist)
        else:
            for arg in args: _calc_slope(arg)
    if not all([os.path.isfile(fname) 
                for fname in hucs['fname_slope'].to_list()]):
        print('ERROR _calc_slope failed for hucs')
        for hucid in [hucid for hucid,row in hucs.iterrows()
                      if not os.path.isfile(row['fname_slope'])]: 
            print(f'  {hucid}')
        sys.exit(0)

def _calc_twi_main(namelist:twtnamelist.Namelist):
    if namelist.options.verbose: print('calling _calc_twi')
    hucs = geopandas.read_file(namelist.fnames.hucs)
    args = tuple([hucs.iloc[[i]] for i in range(len(hucs))])
    args = [arg for arg in args
            if not os.path.isfile(arg.iloc[0]['fname_twi']) or namelist.options.overwrite_flag]
    if len(args) > 0:
        if namelist.pp.flag:
            _pp_func(_calc_twi,args,namelist)
        else:
            for arg in args: _calc_twi(arg)
    if not all([os.path.isfile(fname) 
                for fname in hucs['fname_twi'].to_list()]):
        print('ERROR _calc_twi failed for hucs')
        for hucid in [hucid for hucid,row in hucs.iterrows()
                      if not os.path.isfile(row['fname_twi'])]: 
            print(f'  {hucid}')
        sys.exit(0)

def _calc_twi_mean_main(namelist:twtnamelist.Namelist):
    if namelist.options.verbose: print('calling _calc_mean_twi')
    hucs = geopandas.read_file(namelist.fnames.hucs)
    args = tuple([hucs.iloc[[i]] for i in range(len(hucs))])
    args = [arg for arg in args
            if not os.path.isfile(arg.iloc[0]['fname_twi_mean']) or namelist.options.overwrite_flag]
    if len(args) > 0:
        if namelist.pp.flag:
            _pp_func(_calc_mean_twi,args,namelist)
        else:
            for arg in args: _calc_mean_twi(arg)
    if not all([os.path.isfile(fname) 
                for fname in hucs['fname_twi_mean'].to_list()]):
        print('ERROR _calc_mean_twi failed for hucs')
        for hucid in [hucid for hucid,row in hucs.iterrows()
                      if not os.path.isfile(row['fname_twi_mean'])]: 
            print(f'  {hucid}')
        sys.exit(0)

def _mosaic_tiffs(fname_in,fname_out):
    from rasterio.merge import merge
    mosaic, out_trans = merge(fname_in)
    out_meta          = rasterio.open(fname_in[0],'r').meta.copy()
    out_meta.update({"driver"    : "GTiff",
                     "height"    : mosaic.shape[1],
                     "width"     : mosaic.shape[2],
                     "transform" : out_trans})
    with rasterio.open(fname_out,"w",**out_meta) as riods:
        riods.write(mosaic)

def _download_dem_main(namelist:twtnamelist.Namelist):
    if namelist.options.verbose: print('calling _download_dem_main')
    hucs = geopandas.read_file(namelist.fnames.hucs)
    args = tuple([hucs.iloc[[i]] 
                  for i in range(len(hucs))])
    args = [arg for arg in args 
            if not os.path.isfile(arg.iloc[0]['fname_dem']) or namelist.options.overwrite_flag]
    if len(args) > 0:
        if namelist.pp.flag:
            _pp_func(_download_dem,args,namelist,MAX_PP_TIME_LENGTH_SECONDS=10000)
        else:
            for arg in args: _download_dem(arg)

def _download_dem(huc:geopandas.GeoDataFrame):
    """Get DEM using py3dep"""
    if len(huc) != 1: sys.exit('ERROR _set_dem must be called for single huc')
    fname = huc.iloc[0]['fname_dem']
    if not os.path.isfile(fname):
        huc_buf    = huc.to_crs('EPSG:5070').buffer(distance=1000).to_crs(huc.crs)
        huc_geom   = shapely.ops.unary_union(huc_buf.geometry)
        avail      = py3dep.check_3dep_availability(bbox=tuple(huc_buf.total_bounds),
                                                    crs=huc_buf.crs)
        if   '10m' in avail and avail['10m']:
            rez = 10
        elif '30m' in avail and avail['30m']:
            rez = 30
        elif '60m' in avail and avail['60m']:
            rez = 60
        else:
            sys.exit('ERROR _set_dem could not find dem resolution via py3dep')
        dem = py3dep.get_dem(geometry   = huc_geom,
                             resolution = rez,
                             crs        = huc_buf.crs)
        dem.rio.to_raster(fname)

def _set_huc_mask_main(namelist:twtnamelist.Namelist):
    if namelist.options.verbose: print('calling _set_huc_mask_main')
    hucs = geopandas.read_file(namelist.fnames.hucs)
    args = tuple([hucs.iloc[[i]] 
                  for i in range(len(hucs))])
    args = [arg for arg in args 
            if not os.path.isfile(arg.iloc[0]['fname_huc_mask']) or namelist.options.overwrite_flag]
    if len(args) > 0:
        if namelist.pp.flag:
            _pp_func(_set_huc_mask,args,namelist,MAX_PP_TIME_LENGTH_SECONDS=10000)
        else:
            for arg in args: _set_huc_mask(arg)

def _set_huc_mask(huc:geopandas.GeoDataFrame):
    huc.to_file(huc.iloc[0]['fname_huc'])
    with rasterio.open(huc.iloc[0]['fname_dem'],'r') as dem:
        huc_mask = rasterio.features.rasterize(shapes        = [huc.to_crs(dem.crs).iloc[0]['geometry']],
                                               out_shape     = dem.shape,
                                               transform     = dem.transform,
                                               fill          = 0,
                                               all_touched   = True,
                                               dtype         = rasterio.uint8,
                                               default_value = 1)
        huc_meta = dem.meta.copy()
        huc_meta.update({"driver"    : "GTiff",
                         "height"    : dem.shape[0],
                         "width"     : dem.shape[1],
                         "transform" : dem.transform,
                         "dtype"     : rasterio.uint8,
                         "nodata"    : 0})
        with rasterio.open(huc.iloc[0]['fname_huc_mask'], 'w', **huc_meta) as dst:
            dst.write(huc_mask,indexes=1)

def _break_dem_main(namelist:twtnamelist.Namelist):
    if namelist.options.verbose: print('calling _break_dem')
    hucs = geopandas.read_file(namelist.fnames.hucs)
    args = tuple(zip([hucs.iloc[[i]] for i in range(len(hucs))],
                      [namelist.fnames.dem_user]*len(hucs)))
    args = [arg for arg in args 
            if not os.path.isfile(arg[0].iloc[0]['fname_dem']) or namelist.options.overwrite_flag]
    for arg in args: _break_dem(*arg) #TODO parallelize by putting user dem into shared memory

def _break_dem(huc:geopandas.GeoDataFrame,fname_dem:str):
    with rasterio.open(fname_dem,'r') as riods_dem:
        huc_buf = huc.to_crs('EPSG:5070').buffer(distance=1000).to_crs(riods_dem.crs.to_string())
        masked_dem_array, masked_dem_transform = rasterio.mask.mask(dataset     = riods_dem, 
                                                                    shapes      = [shapely.ops.unary_union(huc_buf)], 
                                                                    crop        = True, 
                                                                    all_touched = True, 
                                                                    filled      = False, 
                                                                    nodata      = numpy.nan)
        masked_dem_meta = riods_dem.meta.copy()
        masked_dem_meta.update({"height"    : masked_dem_array.shape[1],
                                "width"     : masked_dem_array.shape[2],
                                "transform" : masked_dem_transform,
                                "nodata"    : numpy.nan,
                                "dtype"     : numpy.float32})
        with rasterio.open(huc.iloc[0]['fname_dem'],'w',**masked_dem_meta) as riods_masked_dem:
            riods_masked_dem.write(masked_dem_array[0],1)

def _mask(args):
    fname, geom = args
    with rasterio.open(fname,'r') as riods:
        masked_array, masked_transform = rasterio.mask.mask(dataset     = riods, 
                                                            shapes      = geom, 
                                                            crop        = True, 
                                                            all_touched = True, 
                                                            filled      = False, 
                                                            nodata      = numpy.nan)
        masked_meta = riods.meta.copy()
        masked_meta.update({"height"    : masked_array.shape[1],
                            "width"     : masked_array.shape[2],
                            "transform" : masked_transform,
                            "nodata"    : numpy.nan,
                            "dtype"     : numpy.float32})
    return masked_array,masked_meta

def _merge(fname_col,fname_out,namelist):
    args = zip(namelist.pp.hucs[fname_col],namelist.pp.hucs['geometry'])
    with multiprocessing.Pool(processes=min(namelist.options.pp_core_count,len(args))) as pool:
        masked_data = pool.map(_mask, args)
    masked_arrays,masked_metas = masked_data
    try:
        riodss, memfiles = list(), list()
        for masked_array,masked_meta in zip(masked_arrays,masked_metas):
            memfile = rasterio.io.MemoryFile()
            riods = memfile.open(**masked_meta)
            riods.write(masked_array)
            riodss.append(riods)
            memfiles.append(memfile)
        from rasterio.merge import merge
        mosaic, out_trans = merge(riodss)
        out_meta = riodss[0].meta.copy()
        out_meta.update({"driver"   : "GTiff",
                        "height"    : mosaic.shape[1],
                        "width"     : mosaic.shape[2],
                        "transform" : out_trans})
        with rasterio.open(fname_out,"w",**out_meta) as riods:
            riods.write(mosaic)
    finally:
        for riods in riodss:
            if riods and not riods.closed:
                 riods.close()
        for memfile in memfiles:
            if memfile and not memfile.closed:
                memfile.close()

def _breach_dem(huc:geopandas.GeoDataFrame):
    wbt = whitebox.WhiteboxTools()
    wbt.breach_depressions_least_cost(dem    = huc.iloc[0]['fname_dem'],
                                      output = huc.iloc[0]['fname_dem_breached'],
                                      fill   = False,
                                      dist   = 1000)
    
def _breach_dem_fill(huc:geopandas.GeoDataFrame):
    wbt = whitebox.WhiteboxTools()
    wbt.breach_depressions_least_cost(dem    = huc.iloc[0]['fname_dem'],
                                      output = huc.iloc[0]['fname_dem_breached'],
                                      fill   = True,
                                      dist   = 20)

def _calc_facc(huc:geopandas.GeoDataFrame):
    wbt = whitebox.WhiteboxTools()
    wbt.d_inf_flow_accumulation(i        = huc.iloc[0]['fname_dem_breached'],
                                output   = huc.iloc[0]['fname_facc'],
                                out_type = 'sca',
                                log      = False)

def _calc_stream_mask(huc:geopandas.GeoDataFrame,threshold:int):
    wbt = whitebox.WhiteboxTools()
    wbt.extract_streams(flow_accum      = huc.iloc[0]['fname_facc'],
                        output          = huc.iloc[0]['fname_strm_mask'],
                        threshold       = threshold,
                        zero_background = True)

def _calc_slope(huc:geopandas.GeoDataFrame):
    wbt = whitebox.WhiteboxTools()
    wbt.slope(dem    = huc.iloc[0]['fname_dem_breached'],
              output = huc.iloc[0]['fname_slope'],
              units  = 'degrees')

def _calc_twi(huc:geopandas.GeoDataFrame):
    wbt = whitebox.WhiteboxTools()
    wbt.wetness_index(sca    = huc.iloc[0]['fname_facc'],
                      slope  = huc.iloc[0]['fname_slope'],
                      output = huc.iloc[0]['fname_twi'])

def _calc_mean_twi(huc:geopandas.GeoDataFrame):
    with rasterio.open(huc.iloc[0]['fname_twi'],'r') as riods_twi:
        x_rez = riods_twi.transform[0]
        twi = riods_twi.read(1)
        twi = numpy.where(numpy.isclose(twi,riods_twi.meta['nodata']),numpy.nan,twi)
        twi_filled = numpy.where(numpy.isnan(twi),numpy.nanmean(twi),twi)
        twi_mean = uniform_filter(input=twi_filled, size=int(500//x_rez)) # TODO: hardcoded value. change this
        twi_mean = numpy.where(numpy.isnan(twi),numpy.nan,twi_mean)
        twi_mean_meta = riods_twi.meta.copy()
        twi_mean_meta.update({'nodata':numpy.nan})
        with rasterio.open(huc.iloc[0]['fname_twi_mean'],'w',**twi_mean_meta) as riods_twi_mean:
            riods_twi_mean.write(twi_mean, 1)
