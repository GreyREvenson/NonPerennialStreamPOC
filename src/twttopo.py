import os,sys,numpy,shutil,py3dep,twtnamelist,rasterio,whitebox_workflows

def set_twi(namelist:twtnamelist.Namelist,dem_resolution=None):
    """Create TWI and transmissivity data for domain"""
    if namelist.options.verbose: print('calling set_twi')
    _set_dem(namelist,dem_resultion=dem_resolution)
    _project_dem(namelist)
    _breach_dem(namelist)
    _set_facc(namelist)
    _set_slope(namelist)
    _calc_twi(namelist)
    _upsample_twi(namelist)
    _downsample_twi(namelist)

def _set_dem(namelist:twtnamelist.Namelist,dem_resultion=None):
    """Get DEM using py3dep unless user provided via dem variable in namelist.yaml"""
    if namelist.options.verbose: print('calling _set_dem')
    if not os.path.isfile(namelist.fnames.dem_original) or namelist.options.overwrite_flag:
        if os.path.isfile(namelist.fnames.dem_user):
            shutil.copy2(namelist.fnames.dem_user,namelist.fnames.dem_original)
        else:
            bbox_domain = [namelist.bbox_domain.lon_min,
                           namelist.bbox_domain.lat_min,
                           namelist.bbox_domain.lon_max,
                           namelist.bbox_domain.lat_max]
            if dem_resultion is None:
                dtavail = py3dep.check_3dep_availability(bbox_domain)
                if '1m' in dtavail and dtavail['1m']:
                    print('1m DEM available for domain but cannot be automatically downloaded due to its size - using the next coarser resolution instead')
                if '3m' in dtavail and dtavail['3m']:
                    dem_resultion = 3
                elif '5m' in dtavail and dtavail['5m']:
                    dem_resultion = 5
                elif '10m' in dtavail and dtavail['10m']:
                    dem_resultion = 10
                elif '30m' in dtavail and dtavail['30m']:
                    dem_resultion = 30
                elif '60m' in dtavail and dtavail['60m']:
                    dem_resultion = 60
                else:
                    sys.exit('ERROR: could not find dem resolution via py3dep - py3dep.check_3dep_availability results: '+str(dtavail))
            dem_original = py3dep.get_dem(geometry=bbox_domain,resolution=dem_resultion)
            dem_original.rio.to_raster(namelist.fnames.dem_original)
            del dem_original

def _project_dem(namelist:twtnamelist.Namelist):
    """Reproject DEM to match resampled (high resolution) parflow grid"""
    if namelist.options.verbose: print('calling _project_dem')
    if not os.path.isfile(namelist.fnames.dem) or namelist.options.overwrite_flag:
        with rasterio.open(namelist.fnames.dem_original,'r') as dem_original: 
            with rasterio.open(namelist._get_dummy_grid_fname(),'r') as dummy_hr_data:
                dem_reprojected_data, dem_reprojected_transform = rasterio.warp.reproject(
                        source = dem_original.read(1),
                        destination = dummy_hr_data.read(1),
                        src_transform=dem_original.transform,
                        src_crs=dem_original.crs,
                        dst_transform=dummy_hr_data.transform,
                        dst_crs=dummy_hr_data.crs,
                        resampling=rasterio.enums.Resampling.bilinear)
                domain_mask = rasterio.open(namelist.fnames.domain_mask).read(1)
                #dem_reprojected_data = dem_reprojected_data[0,:,:]
                dem_reprojected_data = numpy.where(domain_mask==1,dem_reprojected_data,numpy.nan)
                dem_reprojected_meta = dummy_hr_data.meta
                dem_reprojected_meta.update({"driver": "GTiff","height": dem_reprojected_data.shape[0],"width": dem_reprojected_data.shape[1],"transform": dem_reprojected_transform, "nodata":numpy.nan})
                with rasterio.open(namelist.fnames.dem, "w", **dem_reprojected_meta) as dem_reprojected:
                    dem_reprojected.write(dem_reprojected_data,1)

def _breach_dem(namelist:twtnamelist.Namelist):
    """Breach the DEM (minimally invasive alternative to filling the DEM?) (see https://www.whiteboxgeo.com/manual/wbt_book/available_tools/hydrological_analysis.html#BreachDepressionsLeastCost)"""
    if namelist.options.verbose: print('calling _breach_dem')
    wbe = whitebox_workflows.WbEnvironment()
    if not os.path.isfile(namelist.fnames.dem_breached) or namelist.options.overwrite_flag:
        dem_wbe = wbe.read_raster(namelist.fnames.dem)
        dem_breached_wbe = wbe.breach_depressions_least_cost(dem=dem_wbe)
        wbe.write_raster(dem_breached_wbe, namelist.fnames.dem_breached, compress=False)
        del dem_wbe, dem_breached_wbe

def _set_facc(namelist:twtnamelist.Namelist):
    """Calculate flow accumulation"""
    if namelist.options.verbose: print('calling _set_facc')
    wbe = whitebox_workflows.WbEnvironment()
    if not os.path.isfile(namelist.fnames.flow_acc) or namelist.options.overwrite_flag:
        dem_breached_wbe = wbe.read_raster(namelist.fnames.dem_breached)
        acc_wbe = wbe.dinf_flow_accum(dem=dem_breached_wbe,out_type='sca',log_transform=False)                                         
        wbe.write_raster(acc_wbe, namelist.fnames.flow_acc, compress=False)
        del dem_breached_wbe, acc_wbe

def _set_slope(namelist:twtnamelist.Namelist):
    """Calculate slope (required for TWI calculation)"""
    if namelist.options.verbose: print('calling _set_slope')
    wbe = whitebox_workflows.WbEnvironment()
    if not os.path.isfile(namelist.fnames.slope) or namelist.options.overwrite_flag:
        dem_wbe = wbe.read_raster(namelist.fnames.dem_breached)
        slp_wbe = wbe.slope(dem=dem_wbe)   
        wbe.write_raster(slp_wbe, namelist.fnames.slope, compress=False)
        del dem_wbe, slp_wbe

def _calc_twi(namelist:twtnamelist.Namelist):
    """Calculate TWI"""
    if namelist.options.verbose: print('calling _calc_twi')
    wbe = whitebox_workflows.WbEnvironment()
    if not os.path.isfile(namelist.fnames.twi) or namelist.options.overwrite_flag:
        acc_wbe = wbe.read_raster(namelist.fnames.flow_acc)
        slp_wbe = wbe.read_raster(namelist.fnames.slope)
        twi_wbe = wbe.wetness_index(specific_catchment_area=acc_wbe,slope=slp_wbe)
        wbe.write_raster(twi_wbe, namelist.fnames.twi, compress=False)
        del acc_wbe, slp_wbe, twi_wbe

def _upsample_twi(namelist:twtnamelist.Namelist):
    """Upsample TWI"""
    if namelist.options.verbose: print('calling _upsample_twi')
    if not os.path.isfile(namelist.fnames.twi_upsample) or namelist.options.overwrite_flag:
        with rasterio.open(namelist.fnames.twi,'r') as twi_dataset:
            twi_upsample_data      = twi_dataset.read(out_shape=(twi_dataset.count,int(twi_dataset.height / 100),int(twi_dataset.width / 100)),resampling=rasterio.enums.Resampling.average)
            twi_upsample_transform = twi_dataset.transform * twi_dataset.transform.scale((twi_dataset.width / twi_upsample_data.shape[-1]),(twi_dataset.height / twi_upsample_data.shape[-2]))
            twi_upsample_meta      = twi_dataset.meta
            twi_upsample_meta.update({"driver": "GTiff","height": twi_upsample_data.shape[1],"width": twi_upsample_data.shape[2],"transform": twi_upsample_transform})
            with rasterio.open(namelist.fnames.twi_upsample, "w", **twi_upsample_meta) as twi_upsample_dataset:
                twi_upsample_dataset.write(twi_upsample_data)

def _downsample_twi(namelist:twtnamelist.Namelist):
    """Downsample TWI"""
    if namelist.options.verbose: print('calling _downsample_twi')
    if not os.path.isfile(namelist.fnames.twi_downsample) or namelist.options.overwrite_flag:
        with rasterio.open(namelist.fnames.twi_upsample,'r') as twi_upsample_dataset:
            twi_downsample_data      = twi_upsample_dataset.read(out_shape=(twi_upsample_dataset.count,int(twi_upsample_dataset.height * 100),int(twi_upsample_dataset.width * 100)),resampling=rasterio.enums.Resampling.nearest)
            twi_downsample_transform = twi_upsample_dataset.transform * twi_upsample_dataset.transform.scale((twi_upsample_dataset.width / twi_downsample_data.shape[-1]),(twi_upsample_dataset.height / twi_downsample_data.shape[-2]))
            twi_downsample_meta      = twi_upsample_dataset.meta
            twi_downsample_meta.update({"driver": "GTiff","height": twi_downsample_data.shape[1],"width": twi_downsample_data.shape[2],"transform": twi_downsample_transform})
            with rasterio.open(namelist.fnames.twi_downsample, "w", **twi_downsample_meta) as twi_downsample_dataset:
                twi_downsample_dataset.write(twi_downsample_data)