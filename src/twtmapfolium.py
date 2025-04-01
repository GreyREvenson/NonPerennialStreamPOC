import os,sys,twtnamelist,folium,geopandas,branca,numpy,rasterio,rasterio.warp,rasterio.io
 
def get_fmap_transmissivity(namelist:twtnamelist.Namelist):
    """Get folium map of transmissivity values"""
    if not os.path.isfile(namelist.fnames.soil_transmissivity): sys.exit('ERROR get_fmap_transmissivity could not find '+namelist.fnames.soil_transmissivity)
    instructions = {'Transmissivity Decay Factor (f)': [namelist.fnames.soil_transmissivity, branca.colormap.linear.viridis]}
    return _get_fmap_image(namelist=namelist,instructions=instructions)

def get_fmap_twi(namelist:twtnamelist.Namelist):
    """Get folium map of topological wetness index (TWI) values"""
    if not os.path.isfile(namelist.fnames.twi): sys.exit('ERROR get_fmap_twi could not find '+namelist.fnames.twi)
    instructions = {'Topological Wetness Index (TWI)': [namelist.fnames.twi, branca.colormap.linear.viridis]}
    return _get_fmap_image(namelist=namelist,instructions=instructions)

def get_fmap_slope(namelist:twtnamelist.Namelist):
    """Get folium map of slope values"""
    if not os.path.isfile(namelist.fnames.slope): sys.exit('ERROR get_fmap_slope could not find '+namelist.fnames.slope)
    instructions = {'Slope (degrees)': [namelist.fnames.slope, branca.colormap.linear.viridis]}
    return _get_fmap_image(namelist=namelist,instructions=instructions)

def get_fmap_flowacc(namelist:twtnamelist.Namelist):
    """Get folium map of flow accumulation values"""
    if not os.path.isfile(namelist.fnames.flow_acc): sys.exit('ERROR get_fmap_flowacc could not find '+namelist.fnames.flow_acc)
    instructions = {'Flow accumulation': [namelist.fnames.flow_acc, branca.colormap.linear.viridis]}
    return _get_fmap_image(namelist=namelist,instructions=instructions)

def get_fmap_dem(namelist:twtnamelist.Namelist):
    """Get folium map of digital elevation models (DEMs)"""
    if not os.path.isfile(namelist.fnames.dem):          sys.exit('ERROR get_fmap_dem could not find '+namelist.fnames.dem)
    if not os.path.isfile(namelist.fnames.dem_breached): sys.exit('ERROR get_fmap_dem could not find '+namelist.fnames.dem_breached)
    instructions = {'DEM (m)': [namelist.fnames.dem, branca.colormap.linear.viridis],
                    'DEM (breached) (m)': [namelist.fnames.dem_breached, branca.colormap.linear.viridis]}
    return _get_fmap_image(namelist=namelist,instructions=instructions)

def _get_fmap_image(namelist:twtnamelist.Namelist, instructions:dict):
    """Generic folium map construction for images - instructions = {variable_name: [file_name, branca.colormap]}"""
    domainfg        = _get_fmap_domainfg(namelist)
    nhdfg           = _get_fmap_nhdfg(namelist)
    domain_centroid = _get_fmap_centroid(namelist)
    imap = folium.Map([domain_centroid.y.iloc[0], domain_centroid.x.iloc[0]])
    folium_crs = str(imap.crs).replace('EPSG', 'EPSG:')
    for name, [fname, cmap] in instructions.items():
        if os.path.isfile(fname):
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
                    cmap.add_to(imap)
                    cmapf = lambda x: (0, 0, 0, 0) if numpy.isnan(x) else cmap.rgba_floats_tuple(x)
                    folium.raster_layers.ImageOverlay(
                        name=name,
                        image=vals,
                        bounds=[[bbox[1], bbox[0]], [bbox[3], bbox[2]]],
                        colormap=cmapf
                    ).add_to(imap)
    nhdfg.add_to(imap)
    domainfg.add_to(imap)
    folium.LayerControl().add_to(imap)
    return imap

def get_fmap_texture(namelist:twtnamelist.Namelist):
    domainfg        = _get_fmap_domainfg(namelist)
    nhdfg           = _get_fmap_nhdfg(namelist)
    domain_centroid = _get_fmap_centroid(namelist)
    imap = folium.Map([domain_centroid.y.iloc[0], domain_centroid.x.iloc[0]])
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
                color=texture_colors[texture]: {"fillColor": color, "color": "black", "fillOpacity": 1.0}
            ).add_to(soilsfg)
    soilsfg.add_to(imap)
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
    imap.get_root().html.add_child(folium.Element(html_legend))
    nhdfg.add_to(imap)
    domainfg.add_to(imap)
    folium.LayerControl().add_to(imap)
    return imap

def _get_fmap_domainfg(namelist:twtnamelist.Namelist):
    """Get folium.FeatureGroup for domain"""
    if namelist.options.verbose: print('calling _get_fmap_domainfg')
    if not os.path.isfile(namelist.fnames.domain): sys.exit('ERROR could not find '+namelist.fnames.domain)
    domain = geopandas.read_file(namelist.fnames.domain)
    domainfg = folium.FeatureGroup(name='Domain')
    for _, r in domain.iterrows():folium.GeoJson(data=geopandas.GeoSeries(r['geometry']).to_json(),style_function=lambda x:{"color":"black","fillColor":"none"}).add_to(domainfg)
    return domainfg

def _get_fmap_nhdfg(namelist:twtnamelist.Namelist):
    """Get folium.FeatureGroup for nhd"""
    if namelist.options.verbose: print('calling _get_fmap_nhdfg')
    nhd = geopandas.read_file(namelist.fnames.nhd)
    nhdfg = folium.FeatureGroup(name='NHD HD')
    for _, r in nhd.iterrows(): folium.GeoJson(data=geopandas.GeoSeries(r['geometry']).to_json()).add_to(nhdfg)
    return nhdfg

def _get_fmap_centroid(namelist:twtnamelist.Namelist):
    """Get centroid of domain"""
    if namelist.options.verbose: print('calling _get_fmap_centroid')
    if not os.path.isfile(namelist.fnames.domain): sys.exit('ERROR could not find '+namelist.fnames.domain)
    domain = geopandas.read_file(namelist.fnames.domain)
    domain_centroid = domain.to_crs('+proj=cea').centroid.to_crs(domain.crs)
    return domain_centroid

