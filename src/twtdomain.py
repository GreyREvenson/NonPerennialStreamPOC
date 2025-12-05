import os,pygeohydro,twtnamelist,geopandas,sys,shapely,rasterio
    
def set_domain(namelist:twtnamelist.Namelist):
    """Set domain"""
    if namelist.options.verbose: print('calling set_domain')
    _set_domain(namelist)
    _set_paths(namelist)

def _set_domain(namelist:twtnamelist.Namelist):
    """Set domain - TODO: change so that domain can be a non-huc area and subdomain units can be non-huc area"""
    if namelist.options.verbose: print('calling _set_domain_boundary')
    if not os.path.isfile(namelist.fnames.domain) or namelist.options.overwrite:
        try:
            huc_lvl = int(len(namelist.options.domain_hucid))
            if isinstance(namelist.options.huc_break_lvl,int) and namelist.options.huc_break_lvl in (2,4,6,8,10,12): 
                huc_lvl = namelist.options.huc_break_lvl
            colnam = 'huc' + str(len(namelist.options.domain_hucid))
            hucs   = pygeohydro.WBD(colnam) 
            ids    = hucs[colnam].unique().tolist()
            ids    = [id for id in ids if id.startswith(namelist.options.domain_hucid)]
            domain = hucs.byids(colnam, ids)
            domain = domain.drop(columns=[col for col in domain.columns if col not in [colnam,'geometry']]) 
            domain = domain.rename(columns = {colnam : 'domain_id'})
            domain.to_file(namelist.fnames.domain, driver='GPKG')
            return
        except Exception as e_id:
            pass
        try:
            geom = shapely.geometry.box(namelist.options.domain_bbox[0],
                                        namelist.options.domain_bbox[1],
                                        namelist.options.domain_bbox[2],
                                        namelist.options.domain_bbox[3])
            huc_lvl = namelist.options.huc_break_lvl if namelist.options.huc_break_lvl in (2,4,6,8,10,12) else 12
            colnam = f'huc{huc_lvl}'
            hucs   = pygeohydro.WBD(colnam)
            domain = hucs.bygeom(geom)
            domain = domain.drop(columns=[col for col in domain.columns if col not in [colnam,'geometry']]) 
            domain = domain.rename(columns={colnam : 'domain_id'})
            domain.to_file(namelist.fnames.domain, driver='GPKG')
            return
        except Exception as e_bbox:
            pass
        try:
            geom = shapely.geometry.Point(namelist.options.domain_latlon[0],
                                          namelist.options.domain_latlon[1]).buffer(0.01)
            huc_lvl = namelist.options.huc_break_lvl if namelist.options.huc_break_lvl in (2,4,6,8,10,12) else 12
            colnam = f'huc{huc_lvl}'
            hucs   = pygeohydro.WBD(colnam)
            domain = hucs.bygeom(geom)
            domain = domain.drop(columns=[col for col in domain.columns if col not in [colnam,'geometry']]) 
            domain = domain.rename(columns={colnam : 'domain_id'})
            domain.to_file(namelist.fnames.domain, driver='GPKG')
            return
        except Exception as e_latlon:
            pass
        sys.exit(f'ERROR _set_domain_boundary could not set domain from domain_hucid error:{e_id}, domain_bbox error:{e_bbox}, domain_latlon error:{e_latlon}')

def _set_paths(namelist:twtnamelist.Namelist):
    if namelist.options.verbose: print('calling _set_paths')
    domain = geopandas.read_file(namelist.fnames.domain)
    domain['input'] = domain['domain_id'].apply(lambda domain_id: os.path.join(namelist.dirnames.input,
                                                                               str(domain_id)))
    dirs = domain['input'].tolist()
    if len(dirs) == 1: dirs = dirs[0]
    os.makedirs(dirs, exist_ok=True)
    domain['output'] = domain['domain_id'].apply(lambda domain_id: os.path.join(str(namelist.dirnames.output),
                                                                                str(domain_id)))
    dirs = domain['output'].tolist()
    if len(dirs) == 1: dirs = dirs[0]
    os.makedirs(dirs, exist_ok=True)
    if os.path.isfile(namelist.fnames.dem_user):
        domain = domain.to_crs(rasterio.open(namelist.fnames.dem_user,'r').crs.to_string())
    else:
        domain = domain.to_crs('EPSG:4326')
    domain.to_file(namelist.fnames.domain, driver='GPKG')