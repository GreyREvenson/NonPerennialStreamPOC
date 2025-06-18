import os,geopandas,pynhd,twtnamelist,multiprocessing,shapely,twtutils

def set_streams_main(namelist:twtnamelist.Namelist):
    if namelist.options.verbose: print('calling set_streams_main')
    domain = geopandas.read_file(namelist.fnames.domain)
    args = list(zip([domain.iloc[[i]] for i in range(len(domain))],
                    [namelist.options.overwrite]   * len(domain)))
    if namelist.options.pp and len(args) > 1:
        with multiprocessing.Pool(processes=min(namelist.options.core_count, len(args))) as pool:
            _async_out = [pool.apply_async(_set_streams, arg) for arg in args]
            for i in range(len(_async_out)):
                try: 
                    _async_out[i] = _async_out[i].get()
                except Exception as e: 
                    _async_out[i] = e
    for i in range(len(_async_out)):
        if _async_out[i] is not None: 
            id = args[i][0].iloc[0]['domain_id']
            print(f'ERROR _set_streams failed for domain {id} with error {_async_out[i]}')

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