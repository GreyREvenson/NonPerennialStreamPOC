
import os,sys,numpy,pandas,geopandas,rasterio,soiltexture,twtnamelist

def set_soils(namelist:twtnamelist.Namelist):
    """Get soil texture and transmissivity data for domain"""
    if namelist.options.verbose: print('calling get_soils')
    _set_soil_texture(namelist=namelist)
    _set_soil_transmissivity(namelist=namelist)

def _set_soil_texture(namelist:twtnamelist.Namelist):
    """Get soil texture - uses pysda via https://github.com/ncss-tech/pysda.git"""
    if namelist.options.verbose: print('calling _get_soil_texture')
    if not os.path.isfile(namelist.fnames.soil_texture) or namelist.options.overwrite_flag:
        if not os.path.isfile(namelist.fnames.domain): print('ERROR _get_soil_texture could not find '+namelist.fnames.domain)
        domain = geopandas.read_file(namelist.fnames.domain)
        if namelist.dirnames.pysda not in sys.path: sys.path.append(namelist.dirnames.pysda)
        import sdapoly, sdaprop
        soils_aoi = sdapoly.gdf(domain)
        sandtotal_r=sdaprop.getprop(df=soils_aoi,column='mukey',method='dom_comp_num',top=0,bottom=400,prop='sandtotal_r',minmax=None,prnt=False,meta=False)
        claytotal_r=sdaprop.getprop(df=soils_aoi,column='mukey',method='dom_comp_num',top=0,bottom=400,prop='claytotal_r',minmax=None,prnt=False,meta=False)
        soils_texture = soils_aoi.merge(pandas.merge(sandtotal_r,claytotal_r,on='mukey'),on='mukey')
        def calc_texture(row): 
            try: sand = float(row['sandtotal_r'])
            except: return 'None'
            try: clay = float(row['claytotal_r'])
            except: return 'None'
            return soiltexture.getTexture(sand,clay)
        soils_texture['texture'] = soils_texture.apply(calc_texture, axis=1)
        soils_texture.to_crs('EPSG:4326').to_file(namelist.fnames.soil_texture, driver="GPKG")

def _set_soil_transmissivity(namelist:twtnamelist.Namelist):
    if namelist.options.verbose: print('calling _get_soil_transmissivity')
    if not os.path.isfile(namelist.fnames.soil_transmissivity) or namelist.options.overwrite_flag:
        if not os.path.isfile(namelist.fnames.soil_texture): 
            sys.exit('ERROR could not find '+namelist.fnames.soil_texture)
        soil_texture = geopandas.read_file(namelist.fnames.soil_texture)
        def calc_f(row):
            if row['texture'] in namelist.soil_transmissivity.dt: return namelist.soil_transmissivity.dt[row['texture']]
            else: 
                muname = str(row['muname']).upper()
                if muname.find('WATER') != -1 or muname.find('DAM') != -1: 
                    return numpy.mean(list(namelist.soil_transmissivity.dt.values()))
                else: 
                    sys.exit('ERROR _set_soil_transmissivity could not find transmissivity decay factor for soil texture '''+muname+"'")
        soil_texture['f'] = soil_texture.apply(calc_f, axis=1)
        soil_texture.to_file(namelist.fnames.soil_texture) # save f values to disc
        with rasterio.open(namelist._get_dummy_grid_fname(),'r') as wtd_highres:
            dummy_meta = wtd_highres.meta
            dummy_data = wtd_highres.read(1)
            soil_texture = soil_texture.to_crs(wtd_highres.crs)
            soils_shapes = ((geom,value) for geom, value in zip(soil_texture.geometry, soil_texture['f']))
            texture_data = rasterio.features.rasterize(shapes=soils_shapes,
                                                       out_shape=dummy_data.shape,
                                                       transform=wtd_highres.transform,
                                                       fill=numpy.nan,
                                                       all_touched=True,
                                                       dtype=rasterio.float32,
                                                       default_value=numpy.nan)
            dummy_meta.update({"driver": "GTiff",
                               "height": dummy_data.shape[0],
                               "width": dummy_data.shape[1],
                               "transform": wtd_highres.transform,
                               "dtype": rasterio.float32,
                               "nodata":numpy.nan})
            with rasterio.open(namelist.fnames.soil_transmissivity, 'w', **dummy_meta) as dst:
                dst.write(texture_data,indexes=1)