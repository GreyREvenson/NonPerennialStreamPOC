import os,sys,numpy,pandas,geopandas,rasterio,soiltexture,twtnamelist,multiprocessing,twtutils

def set_soils_main(namelist:twtnamelist.Namelist):
    """Get soil texture and transmissivity data for domain"""
    if namelist.options.verbose: print('calling set_soils_main')
    domain = geopandas.read_file(namelist.fnames.domain)
    args = list(zip([domain.iloc[[i]] for i in range(len(domain))],
                    [namelist.options.overwrite] * len(domain),
                    [namelist.options.verbose] * len(domain)))
    twtutils.call_func(_set_soils,args,namelist)

def _set_soils(domain:geopandas.GeoDataFrame,overwrite:bool,verbose:bool):
    e = _set_soil_texture(domain,overwrite,verbose)
    if e is not None: return e
    e = _set_soil_transmissivity(domain,overwrite,verbose)
    return e

def _set_soil_texture(domain:geopandas.GeoDataFrame,overwrite:bool,verbose:bool):
    if verbose: print(f'calling _set_soil_texture for domain {domain.iloc[0]['domain_id']}')
    try:
        fname_texture = os.path.join(domain.iloc[0]['input'],'soil_texture.gpkg')
        if not os.path.isfile(fname_texture) or overwrite:
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
            soils_texture.to_crs(domain.crs).to_file(fname_texture, driver="GPKG") #'EPSG:4326'
        return None
    except Exception as e:
        return e

def _set_soil_transmissivity(domain:geopandas.GeoDataFrame,overwrite:bool,verbose:bool):
    if verbose: print(f'calling _set_soil_transmissivity for domain {domain.iloc[0]['domain_id']}')
    try:
        fname_texture        = os.path.join(domain.iloc[0]['input'],'soil_texture.gpkg')
        fname_dem            = os.path.join(domain.iloc[0]['input'],'dem.tiff')
        fname_transmissivity = os.path.join(domain.iloc[0]['input'],'soil_transmissivity.tiff')
        if not os.path.isfile(fname_transmissivity) or overwrite:
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
            soil_texture = geopandas.read_file(fname_texture)
            def calc_f(row):
                if    row['texture'] in dt_transmissivity: return dt_transmissivity[row['texture']]
                else: return numpy.mean(list(dt_transmissivity.values()))
            soil_texture['f'] = soil_texture.apply(calc_f, axis=1)
            with rasterio.open(fname_dem,'r') as dem:
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
                with rasterio.open(fname_transmissivity, 'w', **meta) as dst:
                    dst.write(transm_data,indexes=1)
        return None
    except Exception as e:
        return e