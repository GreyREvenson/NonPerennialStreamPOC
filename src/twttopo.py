import os,sys,numpy,geopandas,py3dep,rasterio,shutil,twtnamelist,pygeohydro,whitebox,shapely,multiprocessing,twtutils
from scipy.ndimage import uniform_filter

def calc_topo_main(namelist:twtnamelist.Namelist):
    if namelist.options.verbose: print('calling calc_topo')
    domain = geopandas.read_file(namelist.fnames.domain)
    args = list(zip([domain.iloc[[i]] for i in range(len(domain))],
                    [namelist.fnames.dem_usr]      * len(domain),
                    [namelist.options.facc_thresh] * len(domain),
                    [namelist.options.overwrite]   * len(domain)))
    if namelist.options.pp and len(args) > 1:
        with multiprocessing.Pool(processes=min(namelist.options.core_count, len(args))) as pool:
            _async_out = [pool.apply_async(_calc_topo, arg) for arg in args]
            for i in range(len(_async_out)):
                try: 
                    _async_out[i] = _async_out[i].get()
                except Exception as e: 
                    _async_out[i] = e
    for i in range(len(_async_out)):
        if _async_out[i] is not None: 
            id = args[i][0].iloc[0]['domain_id']
            print(f'ERROR set_topo failed for domain {id} with error {_async_out[i]}')

def _calc_topo(domain:geopandas.GeoDataFrame,fname_dem_usr:str,facc_thresh:float,overwrite:bool=False):
    if os.path.isfile(fname_dem_usr):
        e = _break_dem(domain,overwrite)
        if e is not None: return e
    else:
        e = _download_dem(domain,overwrite)
        if e is not None: return e
    e = _set_domain_mask(domain,overwrite)
    if e is not None: return e
    e = _breach_dem(domain,ovwerwrite)
    if e is not None:
        e = _breach_dem_fill(domain,overwrite)
        if e is None: return e
    e = _calc_facc(domain,overwrite)
    if e is not None: return e
    e = _calc_stream_mask(domain,facc_thresh,overwrite)
    if e is not None: return e
    e = _calc_slope(domain,overwrite)
    if e is not None: return e
    e = _calc_twi(domain,overwrite)
    if e is not None: return e
    e = _calc_mean_twi(domain,overwrite)
    if e is not None: return e
    
def _break_dem(domain:geopandas.GeoDataFrame,fname_dem_usr:str,overwrite:bool=False):
    try:
        dem_out = domain.iloc[0]['fname_dem']
        if not os.path.isfile(fname_dem_usr):
            return f'ERROR _break_dem could not find file {fname_dem_usr}'
        with rasterio.open(fname_dem_usr,'r') as riods_dem:
            domain = domain.to_crs('EPSG:5070')
            domain.geometry = domain.geometry.buffer(distance=1000)
            domain = domain.to_crs(riods_dem.crs)
            dem_bbox = shapely.geometry_polygon.box(riods_dem.bounds)
            if not dem_bbox.contains(domain):
                return f'ERROR _break_dem domain is not covered by dem {fname_dem_usr}'
            if not os.path.isfile(dem_out) or overwrite:
                masked_dem_array, masked_dem_transform = rasterio.mask.mask(dataset     = riods_dem, 
                                                                            shapes      = [shapely.ops.unary_union(domain.iloc[0].geometry)], 
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
                with rasterio.open(dem_out,'w',**masked_dem_meta) as riods_masked_dem:
                    riods_masked_dem.write(masked_dem_array[0],1)
        return None
    except Exception as e:
        return e

def _download_dem(domain:geopandas.GeoDataFrame,overwrite:bool=False):
    try:
        fname_dem = domain.iloc[0]['fname_dem']
        if not os.path.isfile(fname_dem) or overwrite:
            domain_buf  = domain.to_crs('EPSG:5070').buffer(distance=1000).to_crs(domain.crs)
            domain_geom = shapely.ops.unary_union(domain_buf.geometry)
            avail       = py3dep.check_3dep_availability(bbox=tuple(domain_buf.total_bounds),
                                                        crs=domain_buf.crs)
            if   '10m' in avail and avail['10m']:
                rez = 10
            elif '30m' in avail and avail['30m']:
                rez = 30
            elif '60m' in avail and avail['60m']:
                rez = 60
            else:
                sys.exit('ERROR _set_dem could not find dem resolution via py3dep')
            dem = py3dep.get_dem(geometry   = domain_geom,
                                resolution = rez,
                                crs        = domain_buf.crs)
            dem.rio.to_raster(fname_dem)
        return None
    except Exception as e:
        return e

def _set_domain_mask(domain:geopandas.GeoDataFrame,overwrite:bool=False):
    try:
        fname_dem = domain.iloc[0]['fname_dem']
        fname_out = domain.iloc[0]['fname_domain_mask']
        if not os.path.isfile(fname_dem):
            return f'ERROR _set_domain_mask could not find file {fname_dem}'
        if not os.path.isfile(fname_out) or overwrite:
            with rasterio.open(fname_dem,'r') as riods_dem:
                shape = domain.to_crs(riods_dem.crs).iloc[0]['geometry']
                domain_mask = rasterio.features.rasterize(shapes        = shape,
                                                        out_shape     = riods_dem.shape,
                                                        transform     = riods_dem.transform,
                                                        fill          = 0,
                                                        all_touched   = True,
                                                        dtype         = rasterio.uint8,
                                                        default_value = 1)
                domain_mask_meta = riods_dem.meta.copy()
                domain_mask_meta.update({"driver"    : "GTiff",
                                        "height"    : riods_dem.shape[0],
                                        "width"     : riods_dem.shape[1],
                                        "transform" : riods_dem.transform,
                                        "dtype"     : rasterio.uint8,
                                        "nodata"    : 0})
                with rasterio.open(domain.iloc[0]['fname_domain_mask'], 'w', **domain_mask_meta) as dst:
                    dst.write(domain_mask,indexes=1)
        return None
    except Exception as e:
        return e

def _breach_dem(domain:geopandas.GeoDataFrame,ovwerwrite:bool=False):
    try:
        dem    = domain.iloc[0]['fname_dem']
        output = domain.iloc[0]['fname_dem_breached']
        if not os.path.isfile(dem):
            return f'ERROR _breach_dem could not find file {dem}'
        if not os.path.isfile(output) or ovwerwrite:
            wbt = whitebox.WhiteboxTools()
            wbt.breach_depressions_least_cost(dem    = dem,
                                            output = output,
                                            fill   = False,
                                            dist   = 1000)
        return None
    except Exception as e:
        return e
    
def _breach_dem_fill(domain:geopandas.GeoDataFrame,ovwerwrite:bool=False):
    try:
        dem    = domain.iloc[0]['fname_dem']
        output = domain.iloc[0]['fname_dem_breached']
        if not os.path.isfile(dem):
            return f'ERROR _breach_dem could not find file {dem}'
        if not os.path.isfile(output) or ovwerwrite:
            wbt = whitebox.WhiteboxTools()
            wbt.breach_depressions_least_cost(dem    = dem,
                                              output = output,
                                              fill   = True,
                                              dist   = 20)
        return None
    except Exception as e:
        return e

def _calc_facc(domain:geopandas.GeoDataFrame,ovwerwrite:bool=False):
    try:
        i      = domain.iloc[0]['fname_dem_breached']
        output = domain.iloc[0]['fname_facc']
        if not os.path.isfile(dem):
            return f'ERROR _calc_facc could not find file {i}'
        if not os.path.isfile(output) or ovwerwrite:
            wbt = whitebox.WhiteboxTools()
            wbt.d_inf_flow_accumulation(i        = i,
                                        output   = output,
                                        out_type = 'cells',
                                        log      = False)
        return None
    except Exception as e:
        return e

def _calc_stream_mask(domain:geopandas.GeoDataFrame,facc_thresh:int=0.5,overwrite:bool=False):
    try:
        flow_accum = domain.iloc[0]['fname_facc']
        output     = domain.iloc[0]['fname_strm_mask']
        if not os.path.isfile(flow_accum):
            return f'ERROR _calc_stream_mask could not find file {flow_accum}'
        if not os.path.isfile(output) or overwrite:
            wbt = whitebox.WhiteboxTools()
            wbt.extract_streams(flow_accum      = domain.iloc[0]['fname_facc'],
                                output          = domain.iloc[0]['fname_strm_mask'],
                                threshold       = facc_thresh,
                                zero_background = True)
            with rasterio.open(domain.iloc[0]['fname_facc'],'r') as riods_facc:
                facc = riods_facc.read(1)
                strm_mask = numpy.where(facc >= 0.1, 1, 0).astype(rasterio.uint8)
                strm_mask_meta = riods_facc.meta.copy()
                strm_mask_meta.update({'nodata':0,'dtype':rasterio.uint8})
                with rasterio.open(domain.iloc[0]['fname_strm_mask'],'w',**strm_mask_meta) as riods_strm_mask:
                    riods_strm_mask.write(strm_mask,1)
        return None
    except Exception as e:
        return e

def _calc_slope(domain:geopandas.GeoDataFrame,overwrite:bool=False):
    try:
        dem    = domain.iloc[0]['fname_dem_breached']
        output = domain.iloc[0]['fname_slope']
        if not os.path.isfile(dem):
            return f'ERROR _calc_slope could not find file {dem}'
        if not os.path.isfile(output) or overwrite:
            wbt = whitebox.WhiteboxTools()
            wbt.slope(dem    = domain.iloc[0]['fname_dem_breached'],
                      output = domain.iloc[0]['fname_slope'],
                      units  = 'degrees')
        return None
    except Exception as e:
        return e

def _calc_twi(domain:geopandas.GeoDataFrame,overwrite:bool=False):
    try:    
        sca    = domain.iloc[0]['fname_facc']
        slope  = domain.iloc[0]['fname_slope']
        output = domain.iloc[0]['fname_twi']
        if not os.path.isfile(sca):
            return f'ERROR _calc_twi could not find file {sca}'
        if not os.path.isfile(slope):
            return f'ERROR _calc_twi could not find file {slope}'
        if not os.path.isfile(output) or overwrite:
            wbt = whitebox.WhiteboxTools()
            wbt.wetness_index(sca    = sca,
                              slope  = slope,
                              output = output)
        return None
    except Exception as e:
        return e

def _calc_mean_twi(domain:geopandas.GeoDataFrame,overwrite:bool=False):
    try:
        fname_twi      = domain.iloc[0]['fname_twi']
        fname_twi_mean = domain.iloc[0]['fname_twi_mean']
        if not os.path.isfile(fname_twi): 
            return f'ERROR _calc_mean_twi could not find file {fname_twi}'
        if not os.path.isfile(fname_twi_mean) or overwrite:
            with rasterio.open(fname_twi,'r') as riods_twi:
                x_rez = riods_twi.transform[0]
                twi = riods_twi.read(1)
                twi = numpy.where(numpy.isclose(twi,riods_twi.meta['nodata']),numpy.nan,twi)
                twi_filled = numpy.where(numpy.isnan(twi),numpy.nanmean(twi),twi)
                twi_mean = uniform_filter(input=twi_filled, size=int(500//x_rez)) # TODO: hardcoded value. change this
                twi_mean = numpy.where(numpy.isnan(twi),numpy.nan,twi_mean)
                twi_mean_meta = riods_twi.meta.copy()
                twi_mean_meta.update({'nodata':numpy.nan})
                with rasterio.open(fname_twi_mean,'w',**twi_mean_meta) as riods_twi_mean:
                    riods_twi_mean.write(twi_mean, 1)
        return None
    except Exception as e:
        return e
