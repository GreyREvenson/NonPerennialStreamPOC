import os,sys,pygeohydro,rasterio,geopandas,twtnamelist
    
def set_domain(namelist:twtnamelist.Namelist):
    """Set domain"""
    if namelist.options.verbose: print('calling set_domain')
    _set_domain_boundary(namelist)
    _set_domain_boundary_buffered(namelist)
    _set_domain_bbox(namelist)

def _set_domain_boundary(namelist:twtnamelist.Namelist):
    """Set domain spatial boundary"""
    if namelist.options.verbose: print('calling _set_domain_boundary')
    hucstr = 'huc'+str(namelist.vars.huc_level)
    wbdbasins = pygeohydro.WBD(hucstr) 
    wbdbasins  = wbdbasins.byids(hucstr,[namelist.vars.huc])
    domain = wbdbasins.dissolve()
    domain.to_file(namelist.fnames.domain, driver="GPKG")

def _set_domain_boundary_buffered(namelist:twtnamelist.Namelist):
    """Set buffered domain"""
    if namelist.options.verbose: print('calling _set_domain_boundary_buffered')
    if not os.path.isfile(namelist.fnames.domain): sys.exit('ERROR _set_domain_buffered could not find '+namelist.fnames.domain)
    domain = geopandas.read_file(namelist.fnames.domain)
    domain_buffered = domain.to_crs(namelist.parflow.conus1_proj).buffer(distance=2000).to_crs("EPSG:4326")
    domain_buffered.to_file(namelist.fnames.domain_buffered)

def _set_domain_mask(namelist:twtnamelist.Namelist):
    """Get domain mask"""
    if namelist.options.verbose: print('calling _set_domain_mask')
    if not os.path.isfile(namelist.fnames.domain_mask) or namelist.options.overwrite_flag:
        if not os.path.isfile(namelist.fnames.domain): 
            sys.exit('ERROR _set_domain_maskcould not find '+namelist.fnames.domain)
        fname_wtd_hr = os.path.join(namelist.dirnames.wtd_parflow_bilinear,
                                    'wtd_'+namelist.time.datetime_dim[0].strftime('%Y%m%d')+'.tiff')
        if not os.path.isfile(fname_wtd_hr): 
            sys.exit('ERROR _set_domain_mask could not find '+fname_wtd_hr)
        domain = geopandas.read_file(namelist.fnames.domain)
        with rasterio.open(fname_wtd_hr,'r') as wtd_highres:
            domain_data = wtd_highres.read(1)
            domain_meta = wtd_highres.meta
            domain_crs = wtd_highres.crs
            domain_transform = wtd_highres.transform
            domain_mask = rasterio.features.rasterize(shapes=domain.to_crs(domain_crs)['geometry'],
                                                      out_shape=domain_data.shape,
                                                      transform=domain_transform,
                                                      fill=0,
                                                      all_touched=True,
                                                      dtype=rasterio.uint8,
                                                      default_value=1)
            domain_meta.update({"driver": "GTiff",
                                "height": domain_data.shape[0],
                                "width": domain_data.shape[1],
                                "transform": domain_transform,
                                "dtype": rasterio.uint8,
                                "nodata":0})
            with rasterio.open(namelist.fnames.domain_mask, 'w', **domain_meta) as dst:
                dst.write(domain_mask,indexes=1)

def _set_domain_bbox(namelist:twtnamelist.Namelist):
    """Set domain bounding box using buffered domain boundary"""
    if namelist.options.verbose: print('calling _set_domain_bbox')
    if not os.path.isfile(namelist.fnames.domain): sys.exit('ERROR could not find '+namelist.fnames.domain)
    domain = geopandas.read_file(namelist.fnames.domain)
    domain_buffered = domain.to_crs(namelist.parflow.conus1_proj).buffer(distance=2000).to_crs("EPSG:4326")
    namelist.bbox_domain.lat_min = float(domain_buffered.bounds['miny'].iloc[0])
    namelist.bbox_domain.lat_max = float(domain_buffered.bounds['maxy'].iloc[0])
    namelist.bbox_domain.lon_min = float(domain_buffered.bounds['minx'].iloc[0])
    namelist.bbox_domain.lon_max = float(domain_buffered.bounds['maxx'].iloc[0])
