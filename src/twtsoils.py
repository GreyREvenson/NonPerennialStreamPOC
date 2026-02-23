import os,numpy,geopandas,soiltexture,soildb,rioxarray
from geocube.api.core import make_geocube
#from urllib import response

async def set_soil_texture(**kwargs):
    fname_texture = kwargs.get('fname_texture', None)
    domain        = kwargs.get('domain',        None)
    verbose       = kwargs.get('verbose',       False)
    overwrite     = kwargs.get('overwrite',     False)
    if verbose: print(f'calling set_soil_texture')
    if not os.path.isfile(fname_texture) or overwrite:
        if verbose: print(f' using soildb to download soil texture and saving to {fname_texture}')
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
    else:
        if verbose: print(f' found existing soil texture file {fname_texture}')

def set_soil_transmissivity(**kwargs):
    fname_texture        = kwargs.get('fname_texture', None)
    fname_dem            = kwargs.get('fname_dem',     None)
    fname_transmissivity = kwargs.get('fname_transmissivity', None)
    verbose              = kwargs.get('verbose',       False)
    overwrite            = kwargs.get('overwrite',     False)
    if verbose: print('calling set_soil_transmissivity')
    if not os.path.isfile(fname_transmissivity) or overwrite:
        if verbose: print(f' creating {fname_transmissivity} from {fname_texture}')
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
        with rioxarray.open_rasterio(fname_dem,masked=True) as riox_ds_dem:
            soil_texture = soil_texture.to_crs(riox_ds_dem.rio.crs)
            soil_texture_grid = make_geocube(vector_data=soil_texture,
                                            like=riox_ds_dem,
                                            measurements=['f'])
            soil_texture_grid["f"] = soil_texture_grid["f"].astype("float32")
            soil_texture_grid['f'].rio.to_raster(fname_transmissivity)
            riox_ds_trns = rioxarray.open_rasterio(fname_transmissivity,masked=True)
            mask = riox_ds_trns.isnull() & riox_ds_dem.notnull()
            nancount = int(mask.sum())
            tot      = int(riox_ds_dem.count().compute())
            percnan  = float((nancount / tot) * 100.)
            nanmean  = float(riox_ds_trns.mean(skipna=True))
            if verbose and nancount > 0: 
                print(f' WARNING : {nancount} of {tot} cells (~{percnan:.2f}% of domain) had nan transmissivity - filling with grid mean value {nanmean}')
            soil_texture_grid = soil_texture_grid.where(~mask, nanmean)
            soil_texture_grid = soil_texture_grid.transpose('band', 'y', 'x')
            riox_ds_trns.close()
            soil_texture_grid['f'].rio.to_raster(fname_transmissivity)
    else:
        if verbose: print(f' using existing transmissivity data {fname_transmissivity}')