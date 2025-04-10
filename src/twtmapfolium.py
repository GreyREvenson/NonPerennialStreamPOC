import os,sys,twtnamelist,folium,geopandas,branca,numpy,rasterio,rasterio.warp,rasterio.io

class twtfoliummap(folium.Map):

    def __init__(self,namelist:twtnamelist.Namelist,*args,**kwargs):
        super().__init__(*args, **kwargs)
        self._add_domain(namelist=namelist)
        self._add_nhd(namelist=namelist)

    def add_transmissivity(self,namelist:twtnamelist.Namelist):
        """Add transmissivity data to self"""
        if not os.path.isfile(namelist.fnames.soil_transmissivity): 
            sys.exit('ERROR _add_transmissivity could not find '+namelist.fnames.soil_transmissivity)
        self._add_grid(name='Transmissivity Decay Factor (f)',
                      fname=namelist.fnames.soil_transmissivity,
                      cmap=branca.colormap.linear.viridis)

    def add_twi(self,namelist:twtnamelist.Namelist):
        """Add topological wetness index (TWI) data to self"""
        if not os.path.isfile(namelist.fnames.twi): 
            sys.exit('ERROR add_twi could not find '+namelist.fnames.twi)
        self._add_grid(name='Topological Wetness Index (TWI)',
                      fname=namelist.fnames.twi,
                      cmap=branca.colormap.linear.viridis)

    def add_slope(self,namelist:twtnamelist.Namelist):
        """Add slope data to self"""
        if not os.path.isfile(namelist.fnames.slope): 
            sys.exit('ERROR add_slope could not find '+namelist.fnames.slope)
        self._add_grid(name='Slope (degrees)',
                      fname=namelist.fnames.slope,
                      cmap=branca.colormap.linear.Greys_07)

    def add_facc(self,namelist:twtnamelist.Namelist):
        """Add flow accumulation data to self"""
        if not os.path.isfile(namelist.fnames.flow_acc): 
            sys.exit('ERROR add_facc could not find '+namelist.fnames.flow_acc)
        self._add_grid(name='Flow accumulation',
                      fname=namelist.fnames.flow_acc,
                      cmap=branca.colormap.linear.viridis)

    def add_dem(self,namelist:twtnamelist.Namelist):
        """Add dems to self"""
        if not os.path.isfile(namelist.fnames.dem):          
            sys.exit('ERROR get_fmap_dem could not find '+namelist.fnames.dem)
        self._add_grid(name='DEM (m)',
                      fname=namelist.fnames.dem,
                      cmap=branca.colormap.linear.Greys_07)
        if not os.path.isfile(namelist.fnames.dem_breached): 
            sys.exit('ERROR get_fmap_dem could not find '+namelist.fnames.dem_breached)
        self._add_grid(name='DEM (breached) (m)',
                      fname=namelist.fnames.dem_breached,
                      cmap=branca.colormap.linear.Greys_07)

    def add_meanwtd(self,namelist:twtnamelist.Namelist):
        """Add mean wtd data to self"""
        start_string = namelist.time.datetime_dim[0].strftime('%Y%m%d')
        end_string   = namelist.time.datetime_dim[len(namelist.time.datetime_dim)-1].strftime('%Y%m%d')
        fname        = "".join(['mean_wtd_',start_string,'_to_',end_string,'.tiff'])
        if os.path.isfile(os.path.join(namelist.dirnames.wtd_parflow_bilinear,fname)):
            self._add_grid(name='Mean WTD (m) (bilinear)',
                          fname=os.path.join(namelist.dirnames.wtd_parflow_bilinear,fname),
                          cmap=branca.colormap.linear.viridis)
        if os.path.isfile(os.path.join(namelist.dirnames.wtd_parflow_nearest,fname)):
            self._add_grid(name='Mean WTD (m) (nearest)',
                          fname=os.path.join(namelist.dirnames.wtd_parflow_nearest,fname),
                          cmap=branca.colormap.linear.viridis)
        if os.path.isfile(os.path.join(namelist.dirnames.wtd_parflow_cubic,fname)):
            self._add_grid(name='Mean WTD (m) (cubic)',
                          fname=os.path.join(namelist.dirnames.wtd_parflow_cubic,fname),
                          cmap=branca.colormap.linear.viridis)
        if os.path.isfile(os.path.join(namelist.dirnames.wtd_parflow_raw,fname)):
            self._add_grid(name='Mean WTD (m) (raw)',
                          fname=os.path.join(namelist.dirnames.wtd_parflow_raw,fname),
                          cmap=branca.colormap.linear.viridis)


    def add_percinundated(self,namelist:twtnamelist.Namelist):
        """Get folium map of mean WTD values"""
        fname = "".join(['percent_inundated_grid_',
                         namelist.time.datetime_dim[0].strftime('%Y%m%d'),
                         '_to_',
                         namelist.time.datetime_dim[len(namelist.time.datetime_dim)-1].strftime('%Y%m%d'),
                         '.tiff'])
        if os.path.isfile(os.path.join(namelist.dirnames.output_summary_bilinear,fname)):
            self._add_grid(name='Percent Inundated (bilinear)',
                          fname=os.path.join(namelist.dirnames.output_summary_bilinear,fname),
                          cmap=branca.colormap.linear.Reds_08)
        if os.path.isfile(os.path.join(namelist.dirnames.output_summary_nearest,fname)):
            self._add_grid(name='Percent Inundated (nearest)',
                          fname=os.path.join(namelist.dirnames.output_summary_nearest,fname),
                          cmap=branca.colormap.linear.Reds_08)
        if os.path.isfile(os.path.join(namelist.dirnames.output_summary_cubic,fname)):
            self._add_grid(name='Percent Inundated (cubic)',
                          fname=os.path.join(namelist.dirnames.output_summary_cubic,fname),
                          cmap=branca.colormap.linear.Reds_08)

    def _add_grid(self,name:str,fname:str,cmap:branca.colormap.ColorMap):
        """Add gridded data to folium map"""
        if not os.path.isfile(fname): 
            sys.exit('ERROR _add_grid could not find '+fname)
        if fname.find('.tif') == -1: 
            sys.exit('ERROR _add_grid fname does not end in .tif '+fname)
        folium_crs = str(self.crs).replace('EPSG', 'EPSG:')
        with rasterio.open(fname, 'r') as src:
            dst_transform, dst_width, dst_height = rasterio.warp.calculate_default_transform(
                src.crs, folium_crs, src.width, src.height, *src.bounds
            )
            src_prj_meta = src.meta.copy()
            src_prj_meta.update({
                'driver': 'GTIFF',
                'crs': folium_crs,
                'transform': dst_transform,
                'width': dst_width,
                'height': dst_height,
                'nodata': numpy.nan,
                'count': 1,
                'dtype': numpy.float64
            })
            with rasterio.io.MemoryFile().open(**src_prj_meta) as src_prj:
                rasterio.warp.reproject(
                    source=rasterio.band(src, 1),
                    destination=rasterio.band(src_prj, 1),
                    src_transform=src.transform,
                    src_crs=src.crs,
                    dst_transform=dst_transform,
                    dst_crs=folium_crs,
                    resampling=rasterio.enums.Resampling.nearest
                )
                vals = src_prj.read(1)
                bbox = rasterio.warp.transform_bounds(src_prj.crs, "EPSG:4326", *src_prj.bounds)
                cmap = cmap.scale(numpy.nanmin(vals), numpy.nanmax(vals))
                cmap.caption = name
                cmap.add_to(self)
                cmapf = lambda x: (0, 0, 0, 0) if numpy.isnan(x) else cmap.rgba_floats_tuple(x)
                folium.raster_layers.ImageOverlay(
                    name=name,
                    image=vals,
                    bounds=[[bbox[1], bbox[0]], [bbox[3], bbox[2]]],
                    colormap=cmapf
                ).add_to(self)

    def _add_vector(self,name:str,fname:str,name_in_file:str,cmap:branca.colormap.ColorMap|dict):
        """Add vector data to map"""
        if not os.path.isfile(fname): 
            sys.exit('ERROR _add_vector could not find '+fname)
        if fname.find('.gpkg') != -1 or fname.find('.shp') != -1: 
            sys.exit('ERROR _add_vector fname does not end in .shp or .gpkg '+fname) 
        shp = geopandas.read_file(fname)
        shpfg = folium.FeatureGroup(name=name)
        if isinstance(cmap, dict):
            for _, r in shp.iterrows():
                folium.PolyLine(
                    locations=[(lat, lon) for lon, lat in r.geometry.coords],
                    color=cmap[r[name_in_file]]
                ).add_to(shpfg)
        if isinstance(cmap, branca.colormap.ColorMap):
            for _, r in shp.iterrows():
                folium.PolyLine(
                    locations=[(lat, lon) for lon, lat in r.geometry.coords],
                    color=cmap(r[name_in_file])
                ).add_to(shpfg)
        shpfg.add_to(self)

    def add_texture(self,namelist:twtnamelist.Namelist):
        """Add soil texture data to self"""
        if not os.path.isfile(namelist.fnames.soil_texture): 
            sys.exit('ERROR _add_texture could not find '+namelist.fnames.soil_texture)
        soils = geopandas.read_file(namelist.fnames.soil_texture)
        textures = sorted(set(soils['texture']))
        cmap = branca.colormap.linear.viridis.scale(0, len(textures)).to_step(len(textures))
        texture_colors = {texture: cmap(i) for i, texture in enumerate(textures)}
        soilsfg = folium.FeatureGroup(name='Soil texture')
        for texture, texture_group in soils.groupby('texture'):
            for index, row in texture_group.iterrows():
                folium.GeoJson(
                    data=geopandas.GeoSeries(row['geometry']).to_json(),
                    style_function=lambda x, 
                    color=texture_colors[texture]: {"fillColor": color, 
                                                    "color": "black", 
                                                    "fillOpacity": 1.0}
                ).add_to(soilsfg)
        soilsfg.add_to(self)
        html_legend = """
        <div style="position: fixed; 
        top: 10px; left: 60px; width: 200px; height: auto; 
        border:2px solid grey; z-index:9999; font-size:14px;
        background-color:white; opacity: 0.85; padding: 10px;">
        <b>Soil Texture</b><br>
        """
        for texture, color in texture_colors.items():
            html_legend += f'<div style="display: flex; align-items: center; margin-bottom: 5px;"><div style="width: 20px; height: 20px; background-color: {color}; margin-right: 5px;"></div>{texture}</div>'
        html_legend += "</div>"
        self.get_root().html.add_child(folium.Element(html_legend))

    def _add_domain(self,namelist:twtnamelist.Namelist):
        """Add domain boundary to self"""
        if not os.path.isfile(namelist.fnames.domain): 
            sys.exit('ERROR _add_domain could not find '+namelist.fnames.domain)
        domain = geopandas.read_file(namelist.fnames.domain)
        domainfg = folium.FeatureGroup(name='Domain')
        for _, r in domain.iterrows():
            folium.GeoJson(data=geopandas.GeoSeries(r['geometry']).to_json(),
                           style_function=lambda x:{"color":"black","fillColor":"none"}).add_to(domainfg)
        domainfg.add_to(self)
        domain_centroid = domain.to_crs('+proj=cea').centroid.to_crs(domain.crs)
        self.location = [domain_centroid.y.iloc[0], domain_centroid.x.iloc[0]]
        self.fit_bounds([[domain.bounds.miny.iloc[0], domain.bounds.minx.iloc[0]],
                      [domain.bounds.maxy.iloc[0], domain.bounds.maxx.iloc[0]]])
        
    def _add_nhd(self,namelist:twtnamelist.Namelist):
        """Add nhd boundary to self"""
        if not os.path.isfile(namelist.fnames.nhd): 
            sys.exit('ERROR _add_nhd could not find '+namelist.fnames.nhd)
        nhd = geopandas.read_file(namelist.fnames.nhd)
        nhdfg = folium.FeatureGroup(name='NHD HD')
        for _, r in nhd.iterrows(): 
            folium.GeoJson(data=geopandas.GeoSeries(r['geometry']).to_json()).add_to(nhdfg)
        nhdfg.add_to(self)
