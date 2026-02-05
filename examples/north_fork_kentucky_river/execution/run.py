import os,sys
src = r'..\..\..\src'  # edit this path if necessary
sys.path.append(os.path.abspath(src))
import twtmain,twtnamelist,hf_hydrodata,geopandas,multiprocessing,twtsoils,twtdomain
fname_namelist = '../namelist.yaml' # edit this path if necessary

if __name__ == '__main__':
    namelist = twtnamelist.Namelist(filename=fname_namelist)
    twtdomain.set_domain(namelist)
    domain = geopandas.read_file(namelist.fnames.domain)
    args   = twtmain._init_args(domain, namelist)
    twtsoils.set_soil_texture(args,namelist)
    hf_hydrodata.register_api_pin(namelist.options.hf_hydrodata_un, namelist.options.hf_hydrodata_pin)
    print(len(args))
    if namelist.options.pp and len(args) > 1:
        cc = min(namelist.options.core_count, len(args))
        with multiprocessing.Pool(processes=cc) as executor:
            results = list(executor.map(twtmain._calculate, args))
    else:
        results = [twtmain._calculate(arg) for arg in args]
    errmsgs = str()
    for i in range(len(results)):
        if results[i] is not None: 
            errmsgs += f'WARNING {twtmain.__name__} failed for domain {args[i]["domain"].iloc[0]["domain_id"]} with message:\n{results[i]}\n\n'
    if len(errmsgs) > 0:
        print(errmsgs)