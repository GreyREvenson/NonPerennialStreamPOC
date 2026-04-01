import os,sys,multiprocessing
src = r'..\..\..\src'
sys.path.append(os.path.abspath(src))
import twtmain

if __name__ == "__main__":
    dir_subdomains = sys.argv[1:][0]
    n_cores = int(15)
    tasks = list()
    for subdir in os.listdir(dir_subdomains):
        subdir_full = os.path.join(dir_subdomains,subdir)
        if os.path.isdir(subdir_full):
            fname_namelist = os.path.join(subdir_full,'namelist.yaml')
            kwargs = {'fname_namelist' : fname_namelist}
            tasks.append(kwargs)
    with multiprocessing.Pool(processes=n_cores) as pool:
        results_async = [pool.apply_async(twtmain.calculate_async_wrapper, kwds=kwargs) for kwargs in tasks]
        results = [res.get() for res in results_async]