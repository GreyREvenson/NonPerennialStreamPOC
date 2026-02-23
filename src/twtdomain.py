import os,pygeohydro,geopandas,shapely
    
def set_domain(**kwargs):
    fname_domain  = kwargs.get('fname_domain',  None)
    verbose       = kwargs.get('verbose',       False)
    overwrite     = kwargs.get('overwrite',     False)
    domain_id     = kwargs.get('domain_id',     None)
    domain_bbox   = kwargs.get('domain_bbox',   None)
    domain_latlon = kwargs.get('domain_latlon', None)
    if verbose: print('calling set_domain')
    if fname_domain is None: 
        raise ValueError(f'_set_domain missing required argument fname_domain')
    if not os.path.isfile(fname_domain) or overwrite:
        if domain_id is not None:
            domain = _set_domain_byhucid(**kwargs)
        elif domain_bbox is not None:
            domain = _set_domain_bybbox(**kwargs)
        elif domain_latlon is not None:
            domain = _set_domain_bylatlonandhuclvl(**kwargs)
        else:
            raise Exception(f'_set_domain could not set domain from arguments')
    else:
        print(f' using existing domain {fname_domain}')
        domain = geopandas.read_file(fname_domain)
    return domain
        
def _set_domain_byhucid(**kwargs):
    fname_domain = kwargs.get('fname_domain', None)
    domain_id    = kwargs.get('domain_id',    None)
    verbose      = kwargs.get('verbose',      False)
    if verbose: print('_set_domain_byhucid')
    if domain_id is None: 
        raise ValueError(f'_set_domain_byhucid missing required argument domain_id')
    if not isinstance(domain_id,str): 
        raise TypeError('_set_domain_byhucid domain_id must be type str') 
    if len(domain_id) not in (2,4,6,8,10,12):
        raise ValueError(f'_set_domain_byhucid domain_id {domain_id} is invalid, must be of len 2, 4, 6, 8, 10, 12')
    colnam = f'huc{len(domain_id)}'
    hucs   = pygeohydro.WBD(colnam) 
    domain = hucs.byids(colnam, domain_id, return_geom=True)
    domain = domain.drop(columns=[col for col in domain.columns if col not in [colnam,'geometry']]) 
    domain = domain.rename(columns = {colnam : 'domain_id'})
    os.makedirs(name=os.path.dirname(fname_domain),exist_ok=True)
    domain.to_file(fname_domain, driver='GPKG')
    return domain

def _set_domain_bybbox(**kwargs):
    fname_domain = kwargs.get('fname_domain', None)
    domain_bbox  = kwargs.get('domain_bbox',  None)
    verbose      = kwargs.get('verbose',      False)
    if verbose: print('_set_domain_bybbox')
    geom = shapely.geometry.box(domain_bbox[0],
                                domain_bbox[1],
                                domain_bbox[2],
                                domain_bbox[3])
    domain = geopandas.GeoDataFrame(geometry=[geom], crs="EPSG:4326") # lat/lon
    os.makedirs(name=os.path.dirname(fname_domain),exist_ok=True)
    domain.to_file(fname_domain, driver='GPKG')
    return domain

def _set_domain_bylatlonandhuclvl(**kwargs):
    fname_domain  = kwargs.get('fname_domain',  None)
    domain_latlon = kwargs.get('domain_latlon', None)
    huc_lvl       = kwargs.get('huc_lvl',         12)
    verbose       = kwargs.get('verbose',      False)
    if verbose: print('_set_domain_bybbox')
    if int(huc_lvl) not in (2,4,6,8,10,12):
        raise ValueError(f'_set_domain_bylatlonandhuclvl huc_lvl {huc_lvl} is invalid, must be 2, 4, 6, 8, 10, 12')
    if not isinstance(domain_latlon,list) or not all(isinstance(v, float) for v in domain_latlon) or len(domain_latlon) != 2:
        raise ValueError(f'_set_domain_bylatlonandhuclvl invalid domain_latlon')
    geom = shapely.geometry.Point(domain_latlon[1],domain_latlon[0]).buffer(0.01)
    colnam = f'huc{huc_lvl}'
    hucs   = pygeohydro.WBD(colnam)
    domain = hucs.bygeom(geom)
    domain = domain.drop(columns=[col for col in domain.columns if col not in [colnam,'geometry']]) 
    os.makedirs(name=os.path.dirname(fname_domain),exist_ok=True)
    domain.to_file(fname_domain, driver='GPKG')
    return domain

def get_conus1_hucs(**kwargs):
    domain            = kwargs.get('domain',            None)
    fname_domain_hucs = kwargs.get('fname_domain_hucs', None)
    if not os.path.isfile(fname_domain_hucs):
        fname_wb_full_temp = os.path.join(os.path.dirname(fname_domain_hucs),'wb_full.gpkg')
        if os.path.isfile(fname_wb_full_temp): 
            wb_full = geopandas.read_file(fname_wb_full_temp)
        else:
            wb_full = pygeohydro.watershed.huc_wb_full(12)
            wb_full.to_file(fname_wb_full_temp)
        wb_full = wb_full.to_crs(domain.crs)
        domain_hucs = geopandas.clip(gdf=wb_full,mask=domain)
        domain_hucs = domain_hucs[~domain_hucs['states'].str.contains('CN')]
        domain_hucs = domain_hucs[domain_hucs['name'] != 'Lake Michigan']
        domain_hucs = domain_hucs.drop(columns=[col for col in domain_hucs.columns if col not in ['huc12','geometry']]) 
        domain_hucs.to_file(fname_domain_hucs,driver='GPKG')
    else:
        domain_hucs = geopandas.read_file(fname_wb_full_temp)
    return domain_hucs