import os,geopandas,pynhd,twtnamelist,multiprocessing,shapely,twtutils

def set_streams_main(namelist:twtnamelist.Namelist):
    if namelist.options.verbose: print('calling set_streams_main')
    domain     = geopandas.read_file(namelist.fnames.domain)
    domain_ids = domain['domain_id'].tolist()
    args = list(zip([domain.iloc[[i]] for i in range(len(domain))],
                    [namelist.options.overwrite]   * len(domain)))
    twtutils.call_func(_set_streams,args,namelist)

def _set_streams(domain:geopandas.GeoDataFrame,overwrite:bool=False):
    try:
        fname_out = domain.iloc[0]['fname_nhd']
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