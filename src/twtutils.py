import os,sys,geopandas,twtnamelist,multiprocessing

def _mask(fname:str, huc:geopandas.GeoDataFrame):
    with rasterio.open(fname,'r') as riods:
        masked_array, masked_transform = rasterio.mask.mask(dataset     = riods, 
                                                            shapes      = [huc.iloc[0]['geometry']], 
                                                            crop        = True, 
                                                            all_touched = True, 
                                                            filled      = False, 
                                                            nodata      = riods.meta['nodata'])
        masked_meta = riods.meta.copy()
        masked_meta.update({"height"    : masked_array.shape[1],
                            "width"     : masked_array.shape[2],
                            "transform" : masked_transform})
    return masked_array, masked_meta

def call_func(func,args:tuple,namelist:twtnamelist.Namelist):
    """function wrapper"""
    if isinstance(args[0], list) or isinstance(args[0], tuple):
        domain_ids = [args[i][0].iloc[0]['domain_id'] for i in range(len(args))]
    else:
        domain_ids = [args[i].iloc[0]['domain_id'] for i in range(len(args))]
    if namelist.options.pp and len(args) > 1:
        with multiprocessing.Pool(processes=min(namelist.options.core_count, len(args))) as pool:
            if isinstance(args[0], list) or isinstance(args[0], tuple):
                results  = [pool.apply_async(func, arg) for arg in args]
            else:
                results  = [pool.apply_async(func, (arg,)) for arg in args]
            for i in range(len(results)):
                try: 
                    results[i] = results[i].get()
                except Exception as e:
                    results[i] = e
    else:
        results = [func(*args[i]) for i in range(len(args))]
    errmsgs = str()
    for i in range(len(results)):
        if results[i] is not None: 
            errmsgs += f'ERROR {func.__name__} failed for domain {domain_ids[i]} with error\n{results[i]}\n'
    if len(errmsgs) > 0:
        sys.exit(errmsgs)
        
def merge_grids(fname_col:str, fname_out:str, namelist:twtnamelist.Namelist):
    domain = geopandas.read_file(namelist.fnames.domain)
    if fname_col not in domain.columns:
        raise ValueError(f"ERROR merge_grids column '{fname_col}' not found in domain file {namelist.fnames.domain}")
    fnames = domain[fname_col].to_list()
    for fname in fnames:
        if not os.path.isfile(fname):
            raise FileNotFoundError(f"ERROR merge_grids file '{fname}' is not a valid file but is listed in column {fname_col} in domain file {namelist.fnames.domain}")
    args = zip(fnames, domain['geometry'].tolist())
    masked_data = [pool.apply_async(_mask, arg) for arg in args]
    for i in range(len(masked_data)):
        try: 
            masked_data[i] = masked_data[i].get(timeout=namelist.options.pp_max_time_length_seconds)
        except Exception as e:
            masked_data[i] = [None, e]
    for masked_data_i in masked_data:
        if masked_data_i[0] is None: 
            print(f'ERROR merge_grids _mask failed for file {masked_data_i[0]} with error {masked_data_i[1]}')
    try:
        riodss, memfiles = list(), list()
        for masked_array,masked_meta in masked_data:
            memfile = rasterio.io.MemoryFile()
            riods = memfile.open(**masked_meta)
            riods.write(masked_array)
            riodss.append(riods)
            memfiles.append(memfile)
        from rasterio.merge import merge
        mosaic, out_trans = merge(riodss)
        out_meta = riodss[0].meta.copy()
        out_meta.update({"driver"   : "GTiff",
                        "height"    : mosaic.shape[1],
                        "width"     : mosaic.shape[2],
                        "transform" : out_trans})
        with rasterio.open(fname_out,"w",**out_meta) as riods:
            riods.write(mosaic)
    finally:
        for riods in riodss:
            if riods and not riods.closed:
                 riods.close()
        for memfile in memfiles:
            if memfile and not memfile.closed:
                memfile.close()