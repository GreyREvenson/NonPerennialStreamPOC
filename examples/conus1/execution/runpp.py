import os,sys,yaml,asyncio,multiprocessing,geopandas
src = r'..\..\..\src'
sys.path.append(os.path.abspath(src))
import twtdomain,twtmain
if __name__ == "__main__":

    fname_namelist = os.path.abspath(str(sys.argv[1:][0]))
    ppdir = os.path.join(os.path.dirname(fname_namelist),'pp')
    args_nl = yaml.safe_load(open(fname_namelist,'r'))


    fname_domain      = os.path.join(ppdir,'ppDomain.gpkg')
    fname_domain_hucs = os.path.join(ppdir,'ppHUCs.gpkg')
    if not os.path.isfile(fname_domain) or not os.path.isfile(fname_domain_hucs):
        kwargs = {'fname_domain' : fname_domain}
        if 'domain_hucid' in args_nl:
            kwargs.update({'domain_id'     : args_nl['domain_hucid']})
        elif 'domain_latlon' in args_nl:
            kwargs.update({'domain_latlon' : args_nl['domain_latlon']})
        elif 'domain_bbox' in args_nl:
            kwargs.update({'domain_bbox'   : args_nl['domain_bbox']})
        domain = twtdomain.set_domain(**kwargs) #saved to disk inside func
        kwargs['domain'] = domain
        kwargs['fname_domain_hucs'] = fname_domain_hucs
        domain_hucs = twtdomain.get_conus1_hucs(**kwargs)
        domain_hucs.to_file(fname_domain_hucs, driver='GPKG')
    else:
        domain_hucs = geopandas.read_file(fname_domain_hucs)


    tasks = list()
    args_nl
    if 'domain_hucid'  in args_nl: del args_nl['domain_hucid']
    if 'domain_latlon' in args_nl: del args_nl['domain_latlon']
    if 'domain_bbox'   in args_nl: del args_nl['domain_bbox']
    for index, row in domain_hucs.iterrows():
        huc_dir = os.path.join(ppdir,str(row['huc12']))
        kwargs = args_nl.copy()
        kwargs.update({'fname_namelist' : os.path.join(huc_dir,'namelist.yaml'),
                      'domain'         : geopandas.GeoDataFrame([row], crs=domain_hucs.crs),
                      'fname_domain'   : os.path.join(huc_dir,'input','domain.gpkg'),
                      'domain_huc'      : str(row['huc12'])})
        tasks.append(kwargs)

    n_cores = int(round(multiprocessing.cpu_count()*0.5))
    if 'n_cores' in args_nl:
        n_cores_arg = int(args_nl['n_cores'])
        if n_cores_arg >=1 and n_cores_arg <= multiprocessing.cpu_count():
            n_cores = n_cores_arg
        del args_nl['n_cores']
    with multiprocessing.Pool(processes=n_cores) as pool:
        results_async = [pool.apply_async(twtmain.calculate_async_wrapper, kwds=kwargs) for kwargs in tasks]
        results = [res.get() for res in results_async]