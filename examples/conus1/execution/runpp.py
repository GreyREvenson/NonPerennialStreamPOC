import os,sys,yaml,multiprocessing,geopandas
src = r'..\..\..\src'
sys.path.append(os.path.abspath(src))
import twtmain
if __name__ == "__main__":

    fname_namelist = os.path.abspath(str(sys.argv[1:][0]))
    dir_subdomains = os.path.join(os.path.dirname(fname_namelist),'subdomains')
    fname_domain_hucs = os.path.join(os.path.dirname(fname_namelist),'input','domain_hucs.gpkg')

    n_cores = int(round(multiprocessing.cpu_count()*0.5))
    args_nl = yaml.safe_load(open(fname_namelist,'r'))
    if 'n_cores' in args_nl:
        n_cores_arg = int(args_nl['n_cores'])
        if n_cores_arg >=1 and n_cores_arg <= multiprocessing.cpu_count():
            n_cores = n_cores_arg
        del args_nl['n_cores']

    tasks = list()
    domain_hucs = geopandas.read_file(fname_domain_hucs)
    for idx, row in domain_hucs.iterrows():
        subdir = os.path.join(dir_subdomains,str(row['huc8']))
        os.makedirs(subdir, exist_ok=True)
        kwargs = args_nl.copy()
        kwargs.update({'fname_namelist' : os.path.join(subdir,'namelist.yaml'),
                       'domain'         : domain_hucs.loc[[idx]]})
        tasks.append(kwargs)
        
    with multiprocessing.Pool(processes=n_cores) as pool:
        results_async = [pool.apply_async(twtmain.calculate_async_wrapper, kwds=kwargs) for kwargs in tasks]
        results = [res.get() for res in results_async]