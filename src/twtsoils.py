import os,sys,numpy,pandas,geopandas,rasterio,soiltexture,twtnamelist,multiprocessing

def set_soils(namelist:twtnamelist.Namelist):
    """Get soil texture and transmissivity data for domain"""
    if namelist.options.verbose: print('calling get_soils')
    if namelist.dirnames.pysda not in sys.path: sys.path.append(namelist.dirnames.pysda)
    _set_soil_texture_main(namelist)
    _set_soil_transmissivity_main(namelist)

def _pp_func(func,args:tuple,namelist:twtnamelist.Namelist,MAX_PP_TIME_LENGTH_SECONDS:int=900):
    with multiprocessing.Pool(processes=min(namelist.pp.core_count, len(args))) as pool:
        _async_out = [pool.apply_async(func, (arg,)) for arg in args]
        for _out in _async_out:
            try: _out.get(timeout=MAX_PP_TIME_LENGTH_SECONDS)
            #except multiprocessing.TimeoutError: pass
            except: pass

def _set_soil_texture_main(namelist:twtnamelist.Namelist):
    if namelist.options.verbose: print('calling _set_soil_texture_main')
    domain = geopandas.read_file(namelist.fnames.domain)
    args = [domain.iloc[[i]] for i in range(len(domain))]
    args = [arg for arg in args
            if not os.path.isfile(arg.iloc[0]['fname_soil_texture']) or namelist.options.overwrite_flag]
    if len(args) > 0:
        if namelist.options.pp:
            _pp_func(_set_soil_texture,args,namelist)
        else:
            for arg in args: _set_soil_texture(arg)
    if not all([os.path.isfile(fname) 
                for fname in domain['fname_soil_texture'].to_list()]):
        print('ERROR _set_soil_texture failed for domain')
        for domain_id in [r['domain_id'] for _,r in domain.iterrows()
                      if not os.path.isfile(r['fname_soil_texture'])]: 
            print(f'  {domain_id}')
        sys.exit(0)

def _set_soil_transmissivity_main(namelist:twtnamelist.Namelist):
    if namelist.options.verbose: print('calling _set_soil_transmissivity_main')
    domain = geopandas.read_file(namelist.fnames.domain)
    args = [domain.iloc[[i]] for i in range(len(domain))]
    args = [arg for arg in args
            if not os.path.isfile(arg.iloc[0]['fname_soil_transmissivity']) or namelist.options.overwrite_flag]
    if len(args) > 0:
        if namelist.options.pp:
            _pp_func(_set_soil_transmissivity,args,namelist)
        else:
            for arg in args: _set_soil_transmissivity(arg)
    if not all([os.path.isfile(fname) 
                for fname in domain['fname_soil_transmissivity'].to_list()]):
        print('ERROR _set_soil_transmissivity failed for domain')
        for domain_id in [r['domain_id'] for _,r in domain.iterrows()
                      if not os.path.isfile(r['fname_soil_transmissivity'])]: 
            print(f'  {domain_id}')
        sys.exit(0)

def _set_soil_texture(domain:geopandas.GeoDataFrame):
    import sdapoly,sdaprop
    """Get soil texture - uses pysda via https://github.com/ncss-tech/pysda.git"""
    soils_aoi = sdapoly.gdf(domain)
    sandtotal_r=sdaprop.getprop(df=soils_aoi,
                                column='mukey',
                                method='dom_comp_num',
                                top=0,
                                bottom=400,
                                prop='sandtotal_r',
                                minmax=None,
                                prnt=False,
                                meta=False)
    claytotal_r=sdaprop.getprop(df=soils_aoi,
                                column='mukey',
                                method='dom_comp_num',
                                top=0,
                                bottom=400,
                                prop='claytotal_r',
                                minmax=None,
                                prnt=False,
                                meta=False)
    soils_texture = soils_aoi.merge(pandas.merge(sandtotal_r,claytotal_r,on='mukey'),on='mukey')
    def calc_texture(row): 
        try: sand = float(row['sandtotal_r'])
        except: return 'None'
        try: clay = float(row['claytotal_r'])
        except: return 'None'
        return soiltexture.getTexture(sand,clay)
    soils_texture['texture'] = soils_texture.apply(calc_texture, axis=1)
    soils_texture.to_crs(domain.crs).to_file(domain.iloc[0]['fname_soil_texture'], driver="GPKG") #'EPSG:4326'

def _set_soil_transmissivity(domain:geopandas.GeoDataFrame):
    dt_transmissivity = {'clay heavy'    :3.2,
                        'silty clay'     :3.1,
                        'clay'           :2.8,
                        'silty clay loam':2.9,
                        'clay loam'      :2.7,
                        'silt'           :3.4,
                        'silt loam'      :2.6,
                        'sandy clay'     :2.5,
                        'loam'           :2.5,
                        'sandy clay loam':2.4,
                        'sandy loam'     :2.3,
                        'loamy sand'     :2.2,
                        'sand'           :2.1,
                        'organic'        :2.5}
    soil_texture = geopandas.read_file(domain.iloc[0]['fname_soil_texture'])
    def calc_f(row):
        if    row['texture'] in dt_transmissivity: return dt_transmissivity[row['texture']]
        else: return numpy.mean(list(dt_transmissivity.values()))
    soil_texture['f'] = soil_texture.apply(calc_f, axis=1)
    #soil_texture.to_file(fname_texture) # save f values to disc - TODO- can you remove this?
    with rasterio.open(domain.iloc[0]['fname_dem'],'r') as dem:
        soil_texture = soil_texture.to_crs(dem.crs.to_string())
        soils_shapes = ((geom,value) for geom, value in zip(soil_texture.geometry, soil_texture['f']))
        dem_array = dem.read(1)
        transm_data = rasterio.features.rasterize(shapes        = soils_shapes,
                                                  out_shape     = dem_array.shape,
                                                  transform     = dem.transform,
                                                  fill          = numpy.nan,
                                                  all_touched   = True,
                                                  dtype         = rasterio.float32,
                                                  default_value = numpy.nan)
        meta = dem.meta.copy()
        meta.update({"driver"    : "GTiff",
                     "height"    : dem_array.shape[0],
                     "width"     : dem_array.shape[1],
                     "transform" : dem.transform,
                     "dtype"     : rasterio.float32,
                     "nodata"    : numpy.nan})
        with rasterio.open(domain.iloc[0]['fname_soil_transmissivity'], 'w', **meta) as dst:
            dst.write(transm_data,indexes=1)
