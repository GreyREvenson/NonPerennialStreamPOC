import os,geopandas,pynhd,twtnamelist,multiprocessing,shapely,twtutils

def set_streams(namelist:twtnamelist.Namelist):
    if namelist.options.verbose: print('calling set_streams')
    _set_nhd_flowlines_main(namelist)

def _set_nhd_flowlines_main(namelist:twtnamelist.Namelist):
    if namelist.options.verbose: print('calling _set_nhd_flowlines_main')
    domain = geopandas.read_file(namelist.fnames.domain)
    args = [domain.iloc[[i]] for i in range(len(domain))]
    args = [arg for arg in args
            if not os.path.isfile(arg.iloc[0]['fname_nhd']) or namelist.options.overwrite_flag]
    if len(args) > 0:
        if namelist.options.pp:
            twtutils._pp_func(_set_nhd_flowlines,args,namelist)
        else:
            for arg in args: _set_nhd_flowlines(arg)
    for _,r in domain.iterrows():
        if not os.path.isfile(r['fname_nhd']):
            print(f'ERROR _set_nhd_flowlines_main missing nhd hr for domain {r['domain_id']}')

def _set_nhd_flowlines(domain:geopandas.GeoDataFrame):
    """Get domain nhd flowlines"""
    try:
        geom = shapely.ops.unary_union(domain['geometry'].to_list())
        nhd = pynhd.NHDPlusHR("flowline").bygeom(geom=domain.total_bounds,
                                                 geo_crs=domain.crs.to_string())
        nhd = nhd.to_crs(domain.crs)
        nhd = geopandas.clip(nhd, geom)
        nhd.to_file(domain.iloc[0]['fname_nhd'], driver="GPKG")
        return [None,None]
    except Exception as e:
        return [domain.iloc[0]["domain_id"],e]