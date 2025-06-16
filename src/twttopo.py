import os,sys,numpy,geopandas,py3dep,rasterio,shutil,twtnamelist,pygeohydro,whitebox,shapely,multiprocessing,twtutils
from scipy.ndimage import uniform_filter

def set_topo(namelist:twtnamelist.Namelist):
    if namelist.options.verbose: print('calling calc_topo')
    if os.path.isfile(namelist.fnames.dem_user):
        _break_dem_main(namelist)
    else: 
        _download_dem_main(namelist)
    _set_domain_crs(namelist)
    _set_domain_mask_main(namelist)
    _breach_dem_main(namelist)
    _calc_facc_main(namelist)
    _calc_strm_mask_main(namelist)
    _calc_slope_main(namelist)
    _calc_twi_main(namelist)
    _calc_twi_mean_main(namelist)
    #_mask_pp_grids(namelist)
    #_mosaic_tiffs_main(namelist)

def _set_domain_crs(namelist:twtnamelist.Namelist):
    domain = geopandas.read_file(namelist.fnames.domain)
    crss = [rasterio.open(fname,'r').crs.to_string() for fname in domain['fname_dem'].tolist()]
    if len(set(crss)) != 1: sys.exit('ERROR _set_domain_crs dems have multiple crs')
    if domain.crs != crss[0]:
        domain = domain.to_crs(crs=crss[0])
        domain.to_file(namelist.fnames.domain, driver='GPKG')

def _breach_dem_main(namelist:twtnamelist.Namelist):
    if namelist.options.verbose: print('calling _breach_dem_main')
    #TODO change to catch timeout errors for _breach_dem and call _breach_dem_filled only for those
    domain = geopandas.read_file(namelist.fnames.domain)
    args = [domain.iloc[[i]] for i in range(len(domain))]
    args = [arg for arg in args
            if not os.path.isfile(arg.iloc[0]['fname_dem_breached']) or namelist.options.overwrite_flag]
    if len(args) > 0:
        if namelist.options.pp: 
            twtutils.pp_func(_breach_dem,args,namelist)
        else:
            for arg in args: _breach_dem(arg)
    args = [arg for arg in args
            if not os.path.isfile(arg.iloc[0]['fname_dem_breached'])]
    if len(args) > 0:
        if namelist.options.verbose: 
            print(f'calling _breach_dem_filled for')
            for domain_id in [r['domain_id'] for _,r in domain.iterrows()
                          if not os.path.isfile(r['fname_dem_breached'])]: 
                print(f'  {domain_id}')
        if namelist.options.pp:
            _pp_func(_breach_dem_fill,args,namelist)
        else:
            for arg in args: _breach_dem_fill(arg)
    if not all([os.path.isfile(fname) 
                for fname in domain['fname_dem_breached'].to_list()]):
        print('ERROR _breach_dem and _breach_dem_filled failed for domain')
        for domain_id in [r['domain_id'] for _,r in domain.iterrows()
                      if not os.path.isfile(r['fname_dem_breached'])]: 
            print(f'  {domain_id}')
        sys.exit(0)

def _calc_facc_main(namelist:twtnamelist.Namelist):
    if namelist.options.verbose: print('calling _calc_facc_main')
    domain = geopandas.read_file(namelist.fnames.domain)
    args = [domain.iloc[[i]] for i in range(len(domain))]
    args = [arg for arg in args
            if not os.path.isfile(arg.iloc[0]['fname_facc']) or namelist.options.overwrite_flag]
    if len(args) > 0:
        if namelist.options.pp:
            _pp_func(_calc_facc,args,namelist)
        else:
            for arg in args: _calc_facc(arg)
    if not all([os.path.isfile(fname) 
                for fname in domain['fname_facc'].to_list()]):
        print('ERROR _calc_facc failed for domains')
        for domain_id in [r['domain_id'] for _,r in domain.iterrows()
                      if not os.path.isfile(r['fname_facc'])]: 
            print(f'  {domain_id}')
        sys.exit(0)

def _calc_strm_mask_main(namelist:twtnamelist.Namelist):
    if namelist.options.verbose: print('calling _calc_stream_mask')
    domain = geopandas.read_file(namelist.fnames.domain)
    args = list(zip([domain.iloc[[i]] for i in range(len(domain))],
                    [namelist.options.facc_strm_threshold]*len(domain)))
    args = [arg for arg in args
            if not os.path.isfile(arg[0].iloc[0]['fname_strm_mask']) or namelist.options.overwrite_flag]
    if len(args) > 0:
        if namelist.options.pp:
            _pp_func(_calc_stream_mask,args,namelist)
        else:
            for arg in args: _calc_stream_mask(arg)
    if not all([os.path.isfile(fname) 
                for fname in domain['fname_strm_mask'].to_list()]):
        print('ERROR _calc_stream_mask failed for domain')
        for domain_id in [r['domain_id'] for _,r in domain.iterrows()
                      if not os.path.isfile(r['fname_strm_mask'])]: 
            print(f'  {domain_id}')
        sys.exit(0)

def _calc_slope_main(namelist:twtnamelist.Namelist):
    if namelist.options.verbose: print('calling _calc_slope')
    domain = geopandas.read_file(namelist.fnames.domain)
    args = [domain.iloc[[i]] for i in range(len(domain))]
    args = [arg for arg in args
            if not os.path.isfile(arg.iloc[0]['fname_slope']) or namelist.options.overwrite_flag]
    if len(args) > 0:
        if namelist.options.pp:
            _pp_func(_calc_slope,args,namelist)
        else:
            for arg in args: _calc_slope(arg)
    if not all([os.path.isfile(fname) 
                for fname in domain['fname_slope'].to_list()]):
        print('ERROR _calc_slope failed for sub-domains')
        for domain_id in [r['domain_id'] for _,r in domain.iterrows()
                      if not os.path.isfile(r['fname_slope'])]: 
            print(f'  {domain_id}')
        sys.exit(0)

def _calc_twi_main(namelist:twtnamelist.Namelist):
    if namelist.options.verbose: print('calling _calc_twi')
    domain = geopandas.read_file(namelist.fnames.domain)
    args = tuple([domain.iloc[[i]] for i in range(len(domain))])
    args = [arg for arg in args
            if not os.path.isfile(arg.iloc[0]['fname_twi']) or namelist.options.overwrite_flag]
    if len(args) > 0:
        if namelist.options.pp:
            _pp_func(_calc_twi,args,namelist)
        else:
            for arg in args: _calc_twi(arg)
    if not all([os.path.isfile(fname) 
                for fname in domain['fname_twi'].to_list()]):
        print('ERROR _calc_twi failed for domain')
        for domain_id in [r['domain_id'] for _,r in domain.iterrows()
                      if not os.path.isfile(r['fname_twi'])]: 
            print(f'  {domain_id}')
        sys.exit(0)

def _calc_twi_mean_main(namelist:twtnamelist.Namelist):
    if namelist.options.verbose: print('calling _calc_mean_twi')
    domain = geopandas.read_file(namelist.fnames.domain)
    args = tuple([domain.iloc[[i]] for i in range(len(domain))])
    args = [arg for arg in args
            if not os.path.isfile(arg.iloc[0]['fname_twi_mean']) or namelist.options.overwrite_flag]
    if len(args) > 0:
        if namelist.options.pp:
            _pp_func(_calc_mean_twi,args,namelist)
        else:
            for arg in args: _calc_mean_twi(arg)
    if not all([os.path.isfile(fname) 
                for fname in domain['fname_twi_mean'].to_list()]):
        print('ERROR _calc_mean_twi failed for domain')
        for domain_id in [r['domain_id'] for _,r in domain.iterrows()
                      if not os.path.isfile(r['fname_twi_mean'])]: 
            print(f'  {domain_id}')
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
    domain = geopandas.read_file(namelist.fnames.domain)
    args = tuple([domain.iloc[[i]] 
                  for i in range(len(domain))])
    args = [arg for arg in args 
            if not os.path.isfile(arg.iloc[0]['fname_dem']) or namelist.options.overwrite_flag]
    if len(args) > 0:
        if namelist.options.pp:
            _pp_func(_download_dem,args,namelist,MAX_PP_TIME_LENGTH_SECONDS=10000)
        else:
            for arg in args: _download_dem(arg)

def _download_dem(domain:geopandas.GeoDataFrame):
    """Get DEM using py3dep"""
    if len(domain) != 1: sys.exit('ERROR _download_dem must be called for single sub-domain geometry')
    if not os.path.isfile(domain.iloc[0]['fname_dem']):
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
        dem.rio.to_raster(domain.iloc[0]['fname_dem'])

def _set_domain_mask_main(namelist:twtnamelist.Namelist):
    if namelist.options.verbose: print('calling _set_domain_mask_main')
    domain = geopandas.read_file(namelist.fnames.domain)
    args = tuple([domain.iloc[[i]] 
                  for i in range(len(domain))])
    args = [arg for arg in args 
            if not os.path.isfile(arg.iloc[0]['fname_domain_mask']) or not os.path.isfile(arg.iloc[0]['fname_domain']) or namelist.options.overwrite_flag]
    if len(args) > 0:
        if namelist.options.pp:
            _pp_func(_set_domain_mask,args,namelist)
        else:
            for arg in args: _set_domain_mask(arg)

def _set_domain_mask(domain:geopandas.GeoDataFrame):
    print(domain.iloc[0]['fname_domain'])
    domain.to_file(domain.iloc[0]['fname_domain'],driver='GPKG')
    with rasterio.open(domain.iloc[0]['fname_dem'],'r') as dem:
        domain_mask = rasterio.features.rasterize(shapes        = [domain.to_crs(dem.crs).iloc[0]['geometry']],
                                                  out_shape     = dem.shape,
                                                  transform     = dem.transform,
                                                  fill          = 0,
                                                  all_touched   = True,
                                                  dtype         = rasterio.uint8,
                                                  default_value = 1)
        domain_mask_meta = dem.meta.copy()
        domain_mask_meta.update({"driver"    : "GTiff",
                                 "height"    : dem.shape[0],
                                 "width"     : dem.shape[1],
                                 "transform" : dem.transform,
                                 "dtype"     : rasterio.uint8,
                                 "nodata"    : 0})
        with rasterio.open(domain.iloc[0]['fname_domain_mask'], 'w', **domain_mask_meta) as dst:
            dst.write(domain_mask,indexes=1)

def _break_dem_main(namelist:twtnamelist.Namelist):
    if namelist.options.verbose: print('calling _break_dem_main')
    domain = geopandas.read_file(namelist.fnames.domain)
    if not all([os.path.isfile(fname) for fname in domain['fname_dem'].to_list()]):
        domain = geopandas.read_file(namelist.fnames.domain)
        dem_bbox = shapely.geometry_polygon.box(rasterio.open(namelist.fnames.dem_user,'r').bounds)
        if not dem_bbox.contains(domain):
            print('ERROR _break_dem_main dem does not cover domain')
            sys.exit(0)
        domain = domain.to_crs('EPSG:5070')
        domain.geometry = domain.geometry.buffer(distance=1000)
        with rasterio.open(namelist.fnames.dem_user,'r') as riods_dem:
            domain = domain.to_crs(riods_dem.crs)
            for i,r in domain.iterrows():
                if not os.path.isfile(r['fname_dem']) or namelist.options.overwrite_flag:
                    masked_dem_array, masked_dem_transform = rasterio.mask.mask(dataset     = riods_dem, 
                                                                                shapes      = [shapely.ops.unary_union(r.geometry)], 
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
                    with rasterio.open(r['fname_dem'],'w',**masked_dem_meta) as riods_masked_dem:
                        riods_masked_dem.write(masked_dem_array[0],1)

def _breach_dem(domain:geopandas.GeoDataFrame):
    wbt = whitebox.WhiteboxTools()
    wbt.breach_depressions_least_cost(dem    = domain.iloc[0]['fname_dem'],
                                      output = domain.iloc[0]['fname_dem_breached'],
                                      fill   = False,
                                      dist   = 1000)
    
def _breach_dem_fill(domain:geopandas.GeoDataFrame):
    wbt = whitebox.WhiteboxTools()
    wbt.breach_depressions_least_cost(dem    = domain.iloc[0]['fname_dem'],
                                      output = domain.iloc[0]['fname_dem_breached'],
                                      fill   = True,
                                      dist   = 20)

def _calc_facc(domain:geopandas.GeoDataFrame):
    wbt = whitebox.WhiteboxTools()
    wbt.d_inf_flow_accumulation(i        = domain.iloc[0]['fname_dem_breached'],
                                output   = domain.iloc[0]['fname_facc'],
                                out_type = 'sca',
                                log      = False)

def _calc_stream_mask(domain:geopandas.GeoDataFrame,threshold:int):
    wbt = whitebox.WhiteboxTools()
    wbt.extract_streams(flow_accum      = domain.iloc[0]['fname_facc'],
                        output          = domain.iloc[0]['fname_strm_mask'],
                        threshold       = threshold,
                        zero_background = True)

def _calc_slope(domain:geopandas.GeoDataFrame):
    wbt = whitebox.WhiteboxTools()
    wbt.slope(dem    = domain.iloc[0]['fname_dem_breached'],
              output = domain.iloc[0]['fname_slope'],
              units  = 'degrees')

def _calc_twi(domain:geopandas.GeoDataFrame):
    wbt = whitebox.WhiteboxTools()
    wbt.wetness_index(sca    = domain.iloc[0]['fname_facc'],
                      slope  = domain.iloc[0]['fname_slope'],
                      output = domain.iloc[0]['fname_twi'])

def _calc_mean_twi(domain:geopandas.GeoDataFrame):
    with rasterio.open(domain.iloc[0]['fname_twi'],'r') as riods_twi:
        x_rez = riods_twi.transform[0]
        twi = riods_twi.read(1)
        twi = numpy.where(numpy.isclose(twi,riods_twi.meta['nodata']),numpy.nan,twi)
        twi_filled = numpy.where(numpy.isnan(twi),numpy.nanmean(twi),twi)
        twi_mean = uniform_filter(input=twi_filled, size=int(500//x_rez)) # TODO: hardcoded value. change this
        twi_mean = numpy.where(numpy.isnan(twi),numpy.nan,twi_mean)
        twi_mean_meta = riods_twi.meta.copy()
        twi_mean_meta.update({'nodata':numpy.nan})
        with rasterio.open(domain.iloc[0]['fname_twi_mean'],'w',**twi_mean_meta) as riods_twi_mean:
            riods_twi_mean.write(twi_mean, 1)
