import os,pygeohydro,twtnamelist,geopandas,sys,shapely,rasterio
    
def set_domain(namelist:twtnamelist.Namelist):
    """Set domain"""
    if namelist.options.verbose: print('calling set_domain')
    _set_domain(namelist)
    _set_paths(namelist)

def _set_domain(namelist:twtnamelist.Namelist):
    """Set domain"""
    if namelist.options.verbose: print('calling _set_domain_boundary')
    if not os.path.isfile(namelist.fnames.domain) or namelist.options.overwrite:
        if len(str(namelist.options.domain_hucid)) > 0:
            if namelist.options.verbose: print(f'  setting domain from domain_hucid {namelist.options.domain_hucid}')
            try:
                if isinstance(namelist.options.huc_break_lvl,int) and namelist.options.huc_break_lvl in (2,4,6,8,10,12): # override if break level specified
                    lvl    = namelist.options.huc_break_lvl
                    colnam = f'huc{lvl}'
                    domain = pygeohydro.watershed.huc_wb_full(lvl)
                    domain = domain[domain[colnam].str.startswith(namelist.options.domain_hucid)]
                else:
                    lvl    = int(len(namelist.options.domain_hucid))
                    colnam = f'huc{lvl}'
                    domain = pygeohydro.WBD(colnam).byids(field=colnam,
                                                          fids=[namelist.options.domain_hucid])
                domain = domain.drop(columns=[col for col in domain.columns if col not in [colnam,'geometry']]) 
                domain = domain.rename(columns = {colnam : 'domain_id'})
                domain.to_file(namelist.fnames.domain, driver='GPKG')
                return
            except Exception as e_id:
                sys.exit(f'ERROR _set_domain_boundary could not set domain from domain_hucid error {e_id}')
        elif len(namelist.options.domain_bbox) == 4:
            if namelist.options.verbose: print(f'  setting domain from domain_bbox {namelist.options.domain_bbox}')
            try:
                geom = shapely.geometry.box(namelist.options.domain_bbox[0],
                                            namelist.options.domain_bbox[1],
                                            namelist.options.domain_bbox[2],
                                            namelist.options.domain_bbox[3])
                huc_lvl = namelist.options.huc_break_lvl if namelist.options.huc_break_lvl in (2,4,6,8,10,12,16) else 12
                colnam = f'huc{huc_lvl}'
                hucs   = pygeohydro.WBD(colnam)
                domain = hucs.bygeom(geom)
                domain = domain.drop(columns=[col for col in domain.columns if col not in [colnam,'geometry']]) 
                domain = domain.rename(columns={colnam : 'domain_id'})
                domain.to_file(namelist.fnames.domain, driver='GPKG')
                return
            except Exception as e_bbox:
                sys.exit(f'ERROR _set_domain_boundary could not set domain from domain_bbox error:{e_bbox}')
        elif len(namelist.options.domain_latlon) == 2:
            if namelist.options.verbose: print(f'  setting domain from domain_latlon {namelist.options.domain_latlon}')
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
                sys.exit(f'ERROR _set_domain_boundary could not set domain from domain_latlon error:{e_latlon}')

def _set_paths(namelist:twtnamelist.Namelist):
    if namelist.options.verbose: print('calling _set_paths')
    domain = geopandas.read_file(namelist.fnames.domain)
    domain['input'] = domain['domain_id'].apply(lambda domain_id: os.path.join(namelist.dirnames.input,
                                                                               str(domain_id)))
    dirs = domain['input'].tolist()
    for dir in dirs: os.makedirs(dir, exist_ok=True)
    domain['output'] = domain['domain_id'].apply(lambda domain_id: os.path.join(str(namelist.dirnames.output),
                                                                                str(domain_id)))
    dirs = domain['output'].tolist()
    for dir in dirs: os.makedirs(dir, exist_ok=True)
    if os.path.isfile(namelist.fnames.dem_user):
        domain = domain.to_crs(rasterio.open(namelist.fnames.dem_user,'r').crs.to_string())
    else:
        domain = domain.to_crs('EPSG:4326')
    domain.to_file(namelist.fnames.domain, driver='GPKG')