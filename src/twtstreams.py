import os,geopandas,pynhd,shapely

def set_streams(dt:dict):
    try:
        domain    = dt['domain']
        overwrite = dt['overwrite']
        verbose   = dt['verbose']
        if verbose: 
            print(f'calling _set_streams for domain {domain.iloc[0]['domain_id']}',flush=True)
        fname_out = os.path.join(domain.iloc[0]['input'],'nhd_hr.gpkg')
        if not os.path.isfile(fname_out) or overwrite:
            geom = shapely.ops.unary_union(domain['geometry'].to_list())
            nhd = pynhd.NHDPlusHR("flowline").bygeom(geom   =domain.total_bounds,
                                                     geo_crs=domain.crs.to_string())
            nhd = nhd.to_crs(domain.crs)
            nhd = geopandas.clip(nhd, geom)
            nhd.to_file(fname_out, driver="GPKG")
        return None
    except Exception as e:
        return e