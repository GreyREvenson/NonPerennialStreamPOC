import os,numpy,geopandas,rasterio,soiltexture,soildb,asyncio,multiprocessing,twtnamelist
from urllib import response

def set_soil_transmissivity(dt:dict):
    e = _set_soil_transmissivity(dt)
    return e

def set_soil_texture(args:list,namelist:twtnamelist.Namelist):
    if len(args) == 1: 
        _set_soil_texture_async_wrapper(args[0])
    else: 
        if namelist.options.pp and len(args) > 1:
            cc = min(namelist.options.core_count, len(args))
            with multiprocessing.Pool(processes=cc) as pool:
                results = [pool.apply_async(_set_soil_texture_async_wrapper, (arg,)) for arg in args]
                results = [res.get() for res in results]
        else:
            results = [_set_soil_texture(arg) for arg in args]

def _set_soil_texture_async_wrapper(dt:dict):
    """async wrapper for _set_soil_texture - required for multiprocessing"""
    return asyncio.run(_set_soil_texture(dt))

async def _set_soil_texture(dt:dict):
    ### TODO: verbose isn't printing inside async function when called from multiprocessing pool, or maybe this is IDE related?
    try:
        domain    = dt['domain']
        overwrite = dt['overwrite']
        verbose   = dt['verbose']
        fname_texture = os.path.join(domain.iloc[0]['input'],'soil_texture.gpkg')
        if verbose: 
            print(f'calling _set_soil_texture for domain {domain.iloc[0]['domain_id']}',flush=True)
        if not os.path.isfile(fname_texture) or overwrite:
            response = await soildb.spatial_query(geometry=domain.to_crs(epsg=4326).geometry.union_all().wkt,
                                            table="mupolygon",
                                            spatial_relation="intersects",
                                            return_type="spatial")
            soilsgdf = response.to_geodataframe()
            response = await soildb.fetch_by_keys(soilsgdf.mukey.unique().tolist(), 
                                            "component",
                                            key_column="mukey",
                                            columns=["mukey","cokey", "compname", "comppct_r"])
            comps = response.to_pandas()
            dom_comps = comps.loc[comps.groupby('mukey')['comppct_r'].idxmax()] # get dominant component per map unit
            response = await soildb.fetch_by_keys(dom_comps['cokey'].tolist(), 
                                            "chorizon",
                                            key_column="cokey",
                                            columns=["cokey","sandtotal_r","silttotal_r","claytotal_r","hzdept_r",'hzdepb_r','hzdept_r'])
            chorizons = response.to_pandas()
            chorizons['depth_range'] = chorizons['hzdepb_r'] - chorizons['hzdept_r']
            sand = chorizons.groupby('cokey')[['sandtotal_r', 'depth_range']].apply(lambda x: (x['sandtotal_r'] * x['depth_range']).sum() / x['depth_range'].sum()).reset_index(name='weighted_sandtotal_r')
            silt = chorizons.groupby('cokey')[['silttotal_r', 'depth_range']].apply(lambda x: (x['silttotal_r'] * x['depth_range']).sum() / x['depth_range'].sum()).reset_index(name='weighted_silttotal_r')
            clay = chorizons.groupby('cokey')[['claytotal_r', 'depth_range']].apply(lambda x: (x['claytotal_r'] * x['depth_range']).sum() / x['depth_range'].sum()).reset_index(name='weighted_claytotal_r')
            texture = sand.merge(silt, on='cokey').merge(clay, on='cokey')
            def calc_texture(row): 
                try: sand = float(row['weighted_sandtotal_r'])
                except: return 'None'
                try: clay = float(row['weighted_claytotal_r'])
                except: return 'None'
                return soiltexture.getTexture(sand,clay)
            texture['texture'] = texture.apply(calc_texture, axis=1)
            soilsgdf = soilsgdf.merge(dom_comps.merge(texture, on='cokey')[['mukey','texture']], on='mukey')
            soilsgdf = geopandas.clip(gdf=soilsgdf.to_crs(domain.crs),mask=domain)
            soilsgdf.to_file(fname_texture, driver="GPKG")
        return None
    except Exception as e:
        return e

def _set_soil_transmissivity(dt:dict):
    try:
        domain               = dt['domain']
        overwrite            = dt['overwrite']
        verbose              = dt['verbose']
        fname_texture        = os.path.join(domain.iloc[0]['input'],'soil_texture.gpkg')
        fname_dem            = os.path.join(domain.iloc[0]['input'],'dem.tiff')
        fname_transmissivity = os.path.join(domain.iloc[0]['input'],'soil_transmissivity.tiff')
        fname_domain_mask    = os.path.join(domain.iloc[0]['input'],'domain_mask.tiff')
        if verbose: 
            print(f'calling _set_soil_transmissivity for domain {domain.iloc[0]['domain_id']}',flush=True)
        if not os.path.isfile(fname_transmissivity) or overwrite:
            domain_mask = rasterio.open(fname_domain_mask,'r').read(1)
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
                count_nan = numpy.sum((transm_data == numpy.nan) & (domain_mask==1))
                if count_nan > 0:
                    print(f'Warning: {count_nan/numpy.sum(domain_mask==1)} pixels within the domain have NaN transmissivity values. Filling with mean transmissivity value {numpy.nanmean(transm_data)}.', flush=True)
                    transm_data = numpy.where((transm_data == numpy.nan) & (domain_mask==1), numpy.nanmean(transm_data), transm_data)
                with rasterio.open(fname_transmissivity, 'w', **meta) as dst:
                    dst.write(transm_data,indexes=1)
        return None
    except Exception as e:
        return e