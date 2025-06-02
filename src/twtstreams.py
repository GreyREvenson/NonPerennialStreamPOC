import os,sys,geopandas,pynhd,twtnamelist,multiprocessing,shapely

def set_streams(namelist:twtnamelist.Namelist):
    if namelist.options.verbose: print('calling set_streams')
    _set_nhd_flowlines_main(namelist)

def _pp_func(func,args:tuple,namelist:twtnamelist.Namelist,MAX_PP_TIME_LENGTH_SECONDS:int=900):
    with multiprocessing.Pool(processes=min(namelist.pp.core_count, len(args))) as pool:
        if isinstance(args[0], list) or isinstance(args[0], tuple):
            _async_out = [pool.apply_async(func, arg) for arg in args]
        else:
            _async_out = [pool.apply_async(func, (arg,)) for arg in args]
        for _out in _async_out:
            try: _out.get(timeout=MAX_PP_TIME_LENGTH_SECONDS)
            except multiprocessing.TimeoutError: pass

def _set_nhd_flowlines_main(namelist:twtnamelist.Namelist):
    if namelist.options.verbose: print('calling _set_nhd_flowlines_main')
    hucs = geopandas.read_file(namelist.fnames.hucs)
    args = [hucs.iloc[[i]] for i in range(len(hucs))]
    args = [arg for arg in args
            if not os.path.isfile(arg.iloc[0]['fname_nhd']) or namelist.options.overwrite_flag]
    if len(args) > 0:
        if namelist.pp.flag:
            _pp_func(_set_nhd_flowlines,args,namelist)
        else:
            for arg in args: _set_nhd_flowlines(arg)
    if not all([os.path.isfile(fname) 
                for fname in hucs['fname_nhd'].to_list()]):
        print('ERROR _set_nhd_flowlines failed for hucs')
        for hucid in [hucid for hucid,row in hucs.iterrows()
                      if not os.path.isfile(row['fname_nhd'])]: 
            print(f'  {hucid}')
        sys.exit(0)

def _set_nhd_flowlines(huc:geopandas.GeoDataFrame):
    """Get domain nhd flowlines"""
    nhd = pynhd.NHDPlusHR("flowline").bygeom(geom=shapely.ops.unary_union(huc.iloc[0]['geometry']),
                                             geo_crs=huc.crs).to_crs(huc.crs)
    nhd.to_file(huc.iloc[0]['fname_nhd'], driver="GPKG")