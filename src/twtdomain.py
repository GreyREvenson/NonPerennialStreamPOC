import os,pygeohydro,geopandas,shapely
    
def set_domain(**kwargs):
    fname_domain  = kwargs.get('fname_domain',  None)
    verbose       = kwargs.get('verbose',       False)
    overwrite     = kwargs.get('overwrite',     False)
    domain_hucid     = kwargs.get('domain_hucid',     None)
    domain_bbox   = kwargs.get('domain_bbox',   None)
    domain_latlon = kwargs.get('domain_latlon', None)
    conus1_domain = kwargs.get('conus1_domain', None)
    if verbose: print('calling set_domain')
    if fname_domain is None: 
        raise ValueError(f'_set_domain missing required argument fname_domain')
    if not os.path.isfile(fname_domain) or overwrite:
        if domain_hucid is not None:
            domain = _set_domain_byhucid(**kwargs)
        elif domain_bbox is not None:
            domain = _set_domain_bybbox(**kwargs)
        elif domain_latlon is not None:
            domain = _set_domain_bylatlonandhuclvl(**kwargs)
        else:
            raise Exception(f'_set_domain could not set domain from arguments')
        if conus1_domain is not None:
            conus1_domain = geopandas.read_file(conus1_domain)
            domain = geopandas.clip(gdf=domain,mask=conus1_domain.to_crs(domain.crs))
            domain.to_file(fname_domain, driver='GPKG')
    else:
        print(f' using existing domain {fname_domain}')
        domain = geopandas.read_file(fname_domain)
    return domain
        
def set_domain_buf(**kwargs):
    domain           = kwargs.get('domain',           None) 
    buf_dist_m       = kwargs.get('buf_dist_m',       1000)
    fname_domain_buf = kwargs.get('fname_domain_buf', None)
    verbose          = kwargs.get('verbose',          False)
    overwrite        = kwargs.get('overwrite',        False)
    if verbose: print('calling set_domain_buf')
    if fname_domain_buf is None: 
        raise ValueError(f'set_domain_buf missing required argument fname_domain_buf')
    if not os.path.isfile(fname_domain_buf) or overwrite:
        if verbose: print(f' creating domain buffer {fname_domain_buf} with buffer distance {buf_dist_m} m')
        domain_buf = geopandas.GeoDataFrame(domain.drop(columns=['geometry']), 
                                                        geometry=domain.to_crs(crs="EPSG:5070").buffer(distance=buf_dist_m),              
                                                        crs="EPSG:5070").to_crs(crs=domain.crs)
        os.makedirs(name=os.path.dirname(fname_domain_buf),exist_ok=True)
        domain_buf.to_file(fname_domain_buf, driver='GPKG')
    else:
        if verbose: print(f' found existing domain buffer file {fname_domain_buf}')
        domain_buf = geopandas.read_file(fname_domain_buf)
    return domain_buf

def _set_domain_byhucid(**kwargs):
    fname_domain = kwargs.get('fname_domain', None)
    domain_hucid    = kwargs.get('domain_hucid',    None)
    verbose      = kwargs.get('verbose',      False)
    if verbose: print('_set_domain_byhucid')
    if domain_hucid is None: 
        raise ValueError(f'_set_domain_byhucid missing required argument domain_hucid')
    if not isinstance(domain_hucid,str): 
        raise TypeError('_set_domain_byhucid domain_hucid must be type str') 
    if len(domain_hucid) not in (2,4,6,8,10,12):
        raise ValueError(f'_set_domain_byhucid domain_hucid {domain_hucid} is invalid, must be of len 2, 4, 6, 8, 10, 12')
    colnam = f'huc{len(domain_hucid)}'
    hucs   = pygeohydro.WBD(colnam) 
    domain = hucs.byids(colnam, domain_hucid, return_geom=True)
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
    fname_domain      = kwargs.get('fname_domain',      None)
    fname_domain_hucs = kwargs.get('fname_domain_hucs', None)
    huc_lvl           = kwargs.get('huc_lvl',           8)
    verbose           = kwargs.get('verbose',           False)
    if not os.path.isfile(fname_domain_hucs):
        fname_wb_full_temp = os.path.join(os.path.dirname(fname_domain_hucs),f'wb_full_huc{huc_lvl}.gpkg')
        if os.path.isfile(fname_wb_full_temp): 
            wb_full = geopandas.read_file(fname_wb_full_temp)
        else:
            wb_full = pygeohydro.watershed.huc_wb_full(huc_lvl)
            wb_full.to_file(fname_wb_full_temp)
        domain      = geopandas.read_file(fname_domain)
        domain_hucs = geopandas.clip(gdf=wb_full,mask=domain.to_crs(wb_full.crs))
        domain_hucs = domain_hucs[~domain_hucs['states'].str.contains('CN')]
        domain_hucs = domain_hucs[domain_hucs['name'] != 'Lake Michigan']
        domain_hucs = domain_hucs.drop(columns=[col for col in domain_hucs.columns if col not in [f'huc{huc_lvl}','geometry']]) 
        domain_hucs.to_file(fname_domain_hucs,driver='GPKG')
    else:
        domain_hucs = geopandas.read_file(fname_wb_full_temp)
    return domain_hucs