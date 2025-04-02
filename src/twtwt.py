import os,sys,math,datetime,geopandas,hf_hydrodata,rasterio,numpy,twtnamelist,twtdomain

def set_wtd(namelist:twtnamelist.Namelist):
    """Get water table depth data for domain"""
    if namelist.options.verbose: print('calling set_wtd')
    _set_raw_parflow_wtd(namelist)
    #_set_fan2013_data(namelist) TO DO: fix this
    _increase_wtd_resolution(namelist)
    _calc_avgwtd_grid(namelist)
    
def _set_raw_parflow_wtd(namelist:twtnamelist.Namelist):
    """Get raw ParFlow water table depth data"""
    if namelist.options.verbose: print('calling _set_raw_parflow_wtd')
    download_flag = False
    for idatetime in namelist.time.datetime_dim:
        fname_wtd = os.path.join(namelist.dirnames.wtd_parflow_raw,'wtd_'+idatetime.strftime('%Y%m%d')+'.tiff')
        if not os.path.isfile(fname_wtd):
            download_flag = True
            break
    if download_flag:
        if not os.path.isfile(namelist.fnames.domain_buffered): sys.exit('ERROR could not file '+namelist.fnames.domain_buffered)
        domain_buffered = geopandas.read_file(namelist.fnames.domain_buffered)
        conus1_proj,_,conus1_transform,conus1_shape = _get_parflow_conus1_grid_info()
        start_date_str = namelist.time.datetime_dim[0].strftime('%Y-%m-%d')
        end_date_str = (namelist.time.datetime_dim[len(namelist.time.datetime_dim)-1]+datetime.timedelta(days=1)).strftime('%Y-%m-%d')
        options_wtd = {"dataset": "conus1_baseline_mod", "variable": "water_table_depth", "temporal_resolution": "daily", "start_time": start_date_str, "end_time": end_date_str, "grid_bounds":[namelist.bbox_parflow.conus1grid_minx,namelist.bbox_parflow.conus1grid_miny,namelist.bbox_parflow.conus1grid_maxx,namelist.bbox_parflow.conus1grid_maxy]}  
        hf_data = hf_hydrodata.get_gridded_data(options_wtd)
        if hf_data is None or not hasattr(hf_data, 'shape'): sys.exit('ERROR hf_hydrodata query failed')
        if hf_data.shape[0] != int((namelist.time.datetime_dim[len(namelist.time.datetime_dim)-1]-namelist.time.datetime_dim[0]).days) + 1: sys.exit('ERROR hf_hydrodata returned data of unexpected time length or invalid structure')
        hf_conus1grid_temp = numpy.empty(conus1_shape)
        for i in range(len(namelist.time.datetime_dim)):
            idatetime = namelist.time.datetime_dim[i]
            fname = os.path.join(namelist.dirnames.wtd_parflow_raw,'wtd_'+idatetime.strftime('%Y%m%d')+'.tiff')
            hf_conus1grid_temp[namelist.bbox_parflow.conus1grid_miny:namelist.bbox_parflow.conus1grid_maxy,namelist.bbox_parflow.conus1grid_minx:namelist.bbox_parflow.conus1grid_maxx] = hf_data[i,:,:]
            memfile = rasterio.io.MemoryFile()
            hf_conus1data = memfile.open(driver = "GTiff", height = hf_conus1grid_temp.shape[0], width = hf_conus1grid_temp.shape[1], crs=conus1_proj, transform = conus1_transform, nodata = numpy.nan, count = 1, dtype = numpy.float64)
            hf_conus1data.write(hf_conus1grid_temp,1)
            wtd_data, wtd_transform = rasterio.mask.mask(hf_conus1data, domain_buffered.to_crs(hf_conus1data.crs), crop=True, all_touched=True, filled=True, nodata=numpy.nan)
            wtd_meta = hf_conus1data.meta
            wtd_meta.update({"driver": "GTiff","height": wtd_data.shape[1],"width": wtd_data.shape[2],"transform": wtd_transform, "nodata" : -9999})
            with rasterio.open(fname,'w',**wtd_meta) as wtd_dataset:
                wtd_dataset.write(wtd_data[0,:,:],1)

def _set_fan2013_data(namelist:twtnamelist.Namelist):
    """Get Fan (2013) equilibrium water table depth"""
    if namelist.options.verbose: print('calling _set_fan2013_data')
    fname_wtd = os.path.join(namelist.dirnames.wtd_fan,'wtd_equilibrium.tiff')
    if not os.path.isfile(fname_wtd):
        if not os.path.isfile(namelist.fnames.domain_buffered): sys.exit('ERROR _get_raw_parflow_wtd could not find '+namelist.fnames.domain_buffered)
        options_wtd = {"dataset": "fan_2013","variable": "water_table_depth", "temporal_resolution": "long_term", "grid_bounds":[namelist.bbox_parflow.conus2grid_minx,namelist.bbox_parflow.conus2grid_miny,namelist.bbox_parflow.conus2grid_maxx,namelist.bbox_parflow.conus2grid_maxy]}  
        hf_data = hf_hydrodata.get_gridded_data(options_wtd)
        if hf_data.shape[0] != 1: sys.exit('ERROR hydroframe returned data of unexpected time length.')
        hf_conus2grid_temp = numpy.empty(namelist.parflow.conus2_shape)
        hf_conus2grid_temp[namelist.bbox_parflow.conus2grid_miny:namelist.bbox_parflow.conus2grid_maxy,namelist.bbox_parflow.conus2grid_minx:namelist.bbox_parflow.conus2grid_maxx] = hf_data[:,:]
        memfile = rasterio.io.MemoryFile()
        hf_conus2data = memfile.open(driver="GTiff",height=hf_conus2grid_temp.shape[0],width=hf_conus2grid_temp.shape[1],crs=namelist.parflow.conus2_proj,transform=namelist.parflow.conus2_transform,nodata=numpy.nan,count=1,dtype=numpy.float64)
        hf_conus2data.write(hf_conus2grid_temp,1)
        wtd_data, wtd_transform = rasterio.mask.mask(hf_conus2data, geopandas.read_file(namelist.fnames.domain_buffered).to_crs(hf_conus2data.crs), crop=True, all_touched=True, filled =True, nodata = numpy.nan)
        wtd_meta = hf_conus2data.meta
        wtd_meta.update({"driver": "GTiff","height": wtd_data.shape[1],"width": wtd_data.shape[2],"transform": wtd_transform, "nodata" : numpy.nan})
        with rasterio.open(fname_wtd,'w',**wtd_meta) as wtd_dataset:
            wtd_dataset.write(wtd_data[0,:,:],1)
        del hf_data, hf_conus2grid_temp, memfile, hf_conus2data, wtd_data, wtd_meta, wtd_dataset

def _increase_wtd_resolution(namelist:twtnamelist.Namelist):
    """Increase resolution of ParFlow grid to match DEM - maybe just do this on the fly, in-memory, per time step, after putting the topographic info into this grid using one instance of the wtd grid"""
    if namelist.options.verbose: print('calling _increase_wtd_resolution')
    #resample_methods = {'bilinear':[rasterio.enums.Resampling.bilinear,namelist.dirnames.wtd_parflow_bilinear], #doing all three eats too much harddrive space
    #                    'cubic'   :[rasterio.enums.Resampling.cubic,   namelist.dirnames.wtd_parflow_cubic],
    #                    'nearest' :[rasterio.enums.Resampling.nearest, namelist.dirnames.wtd_parflow_nearest]}
    resample_methods = {'bilinear':[rasterio.enums.Resampling.bilinear,namelist.dirnames.wtd_parflow_bilinear]} 
    for resample_method_name, [resample_method, resample_dir] in resample_methods.items():
        for idatetime in namelist.time.datetime_dim:
            fname_wtd = os.path.join(namelist.dirnames.wtd_parflow_raw,'wtd_'+idatetime.strftime('%Y%m%d')+'.tiff')
            fname_wtd_hr = os.path.join(resample_dir,'wtd_'+idatetime.strftime('%Y%m%d')+'.tiff')
            if os.path.isfile(fname_wtd):
                if not os.path.isfile(fname_wtd_hr) or namelist.options.overwrite_flag:
                    with rasterio.open(fname_wtd) as wtd_dataset:
                        wtd_data_hr = wtd_dataset.read(out_shape=(wtd_dataset.count,int(wtd_dataset.height * 500),int(wtd_dataset.width * 500)),resampling=resample_method)
                        wtd_transform_hr = wtd_dataset.transform * wtd_dataset.transform.scale((wtd_dataset.width / wtd_data_hr.shape[-1]),(wtd_dataset.height / wtd_data_hr.shape[-2]))
                        wtd_meta_hr = wtd_dataset.meta
                        wtd_meta_hr.update({"driver": "GTiff","height": wtd_data_hr.shape[1],"width": wtd_data_hr.shape[2],"transform": wtd_transform_hr, "nodata" : numpy.nan})
                        with rasterio.open(fname_wtd_hr, "w", **wtd_meta_hr) as wtd_dataset_hr:
                            wtd_dataset_hr.write(wtd_data_hr)

def _calc_avgwtd_grid(namelist:twtnamelist.Namelist):
    """Calculate summary grid of inundated area"""
    if namelist.options.verbose: print('calling _calc_avgwtd_grid')
    if not os.path.isfile(namelist.fnames.domain_mask): sys.exit('ERROR could not find '+namelist.fnames.domain_mask)
    domain_mask = rasterio.open(namelist.fnames.domain_mask,'r').read(1) 
    for wtd_dir in [namelist.dirnames.wtd_parflow_bilinear,
                    namelist.dirnames.wtd_parflow_nearest,
                    namelist.dirnames.output_raw_cubic]:
        start_string = namelist.time.datetime_dim[0].strftime('%Y%m%d')
        end_string   = namelist.time.datetime_dim[len(namelist.time.datetime_dim)-1].strftime('%Y%m%d')
        fname_output = os.path.join(wtd_dir,'mean_wtd_'+start_string+'_to_'+end_string+'.tiff')
        sum = numpy.zeros(shape=domain_mask.shape,dtype=numpy.float64)
        if not os.path.isfile(fname_output) or namelist.options.overwrite_flag:
            count = 0
            for i in range(len(namelist.time.datetime_dim)):
                date_string = namelist.time.datetime_dim[i].strftime('%Y%m%d')
                fname_wtd_local = os.path.join(wtd_dir,'wtd_'+date_string+'.tiff') 
                if os.path.isfile(fname_wtd_local):
                    wtd_data = rasterio.open(fname_wtd_local,'r').read(1)
                    wtd_data = numpy.where(domain_mask==1,wtd_data,0.)
                    sum += wtd_data
                    count += 1
            if count > 0:
                mean_wtd = sum / count
                mean_wtd = numpy.where(domain_mask==1,mean_wtd,numpy.nan)
                with rasterio.open(fname_output, "w", **rasterio.open(namelist._get_dummy_hrwtd_fname(),'r').meta) as summary_dataset:
                    summary_dataset.write(mean_wtd,1)

def _get_latlon_parflow_grid(grid_minx,grid_miny,grid_maxx,grid_maxy):
    """Get latlon bbox from ParFlow CONUS1 grid xy bbox"""
    latlon_bounds = hf_hydrodata.to_latlon("conus1", *[grid_minx,grid_miny,grid_maxx,grid_maxy])
    lon_min = latlon_bounds[1]
    lat_min = latlon_bounds[0]
    lon_max = latlon_bounds[3]
    lat_max = latlon_bounds[2]
    return lon_min,lat_min,lon_max,lat_max

def _get_parflow_conus1_bbox(namelist:twtnamelist.Namelist):
    """Set domain ParFlow bounding box"""
    if namelist.options.verbose: print('calling _get_parflow_conus1_bbox')
    if not os.path.isfile(namelist.fnames.domain): sys.exit('ERROR could not find '+namelist.fnames.domain)
    conus1grid_minx, conus1grid_miny = hf_hydrodata.from_latlon("conus1", namelist.bbox_domain.lat_min, namelist.bbox_domain.lon_min)
    conus1grid_maxx, conus1grid_maxy = hf_hydrodata.from_latlon("conus1", namelist.bbox_domain.lat_max, namelist.bbox_domain.lon_max)
    conus1grid_minx, conus1grid_miny = math.floor(conus1grid_minx), math.floor(conus1grid_miny)
    conus1grid_maxx, conus1grid_maxy = math.ceil(conus1grid_maxx),  math.ceil(conus1grid_maxy)
    return conus1grid_minx,conus1grid_miny,conus1grid_maxx,conus1grid_maxy
    
def _get_parflow_conus2_bbox(namelist:twtnamelist.Namelist):
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