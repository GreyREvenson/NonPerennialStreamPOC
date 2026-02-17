import geopandas,twtnamelist,twttopo,twtsoils,twtstreams,twtwt,twtcalc,twtdomain,hf_hydrodata,multiprocessing

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


def main(fname_namelist):

    namelist = twtnamelist.Namelist(filename=fname_namelist)
    hf_hydrodata.register_api_pin(namelist.options.hf_hydrodata_un, 
                                  namelist.options.hf_hydrodata_pin)
    kwargs = {'fname_domain':namelist.fnames.domain,
              'verbose':namelist.options.verbose,
              'overwrite':namelist.options.overwrite,
              'domain_hucid': namelist.options.domain_hucid,
              'domain_bbox':namelist.options.domain_bbox,
              'domain_latlon':namelist.options.domain_latlon,
              'huc_lvl':namelist.options.huc_lvl}
    kwargs['domain'] = twtdomain.set_domain(**kwargs)
    kwargs['soiltexture'] = twtsoils.set_soil_texture(**kwargs)
