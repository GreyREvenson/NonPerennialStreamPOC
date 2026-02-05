import geopandas,twtnamelist,twttopo,twtsoils,twtstreams,twtwt,twtcalc

def _calculate(dt:dict):
    print(f'calculating for domain {dt["domain"].iloc[0]["domain_id"]}')
    e = twttopo.calc_topo(dt)
    if e is not None: return e
    e = twtsoils.set_soil_transmissivity(dt)
    if e is not None: return e
    e = twtstreams.set_streams(dt)
    if e is not None: return e
    e = twtwt.set_wtd(dt)
    if e is not None: return e
    e = twtcalc.calc_inundation(dt)
    return e

def _init_args(domain:geopandas.GeoDataFrame, namelist:twtnamelist.Namelist):
    args = []
    for i in range(len(domain)):
        args.append({'domain'              : geopandas.GeoDataFrame([domain.iloc[i]], crs=domain.crs),
                     'fname_dem_usr'       : namelist.fnames.dem_user,
                     'dem_rez'             : namelist.options.dem_rez,
                     'facc_strm_threshold' : namelist.options.facc_strm_threshold,
                     'overwrite'           : namelist.options.overwrite,
                     'verbose'             : namelist.options.verbose,
                     'dt_start'            : namelist.time.start_date,
                     'dt_end'              : namelist.time.end_date,
                     'write_wtd_mean_resampled' : namelist.options.write_wtd_mean_resampled})
    return args