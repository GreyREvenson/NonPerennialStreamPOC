import os,sys,numpy,pandas,geopandas,rasterio,soiltexture,twtnamelist,multiprocessing,twtutils,soildb
from urllib import response
import asyncio

def set_soils_main(namelist:twtnamelist.Namelist):
    """Get soil texture and transmissivity data for domain"""
    if namelist.options.verbose: print('calling set_soils_main')
    domain = geopandas.read_file(namelist.fnames.domain)
    args = list(zip([domain.iloc[[i]] for i in range(len(domain))],
                    [namelist.options.overwrite] * len(domain),
                    [namelist.options.verbose] * len(domain)))
    if len(args) == 1: 
        _set_soil_texture_async_wrapper(*args[0]) # run directly if only one domain
    else: 
        with multiprocessing.Pool() as pool: pool.starmap(_set_soil_texture_async_wrapper, args) # _set_soil_texture is async so needs special handling for multiprocessing. don't use twtutils.call_func here
    twtutils.call_func(_set_soil_transmissivity, args, namelist)

def _set_soil_texture_async_wrapper(domain, overwrite, verbose):
    """async wrapper for _set_soil_texture - required for multiprocessing"""
    return asyncio.run(_set_soil_texture(domain, overwrite, verbose))

async def _set_soil_texture(domain:geopandas.GeoDataFrame,overwrite:bool,verbose:bool):
    ### TODO: verbose isn't printing inside async function when called from multiprocessing pool, or maybe this is IDE related?
    if verbose: print(f'calling _set_soil_texture for domain {domain.iloc[0]['domain_id']}',flush=True)
    try:
        fname_texture = os.path.join(domain.iloc[0]['input'],'soil_texture.gpkg')
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