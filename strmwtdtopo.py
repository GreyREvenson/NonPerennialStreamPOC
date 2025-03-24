import os,sys,pandas,shutil,math,datetime,numpy,py3dep,pygeohydro,pynhd,whitebox_workflows,hf_hydrodata,rasterio,soiltexture,folium

class Namelist:

    class DirectoryNames:
        """Directory names"""
        project                 = ''
        inputs                  = ''
        wtd                     = ''
        wtd_parflow             = ''
        wtd_parflow_raw         = ''
        wtd_parflow_bilinear    = ''
        wtd_parflow_nearest     = ''
        wtd_parflow_cubic       = ''
        wtd_fan                 = ''
        dem                     = ''
        twi                     = ''
        soils                   = ''
        domain                  = ''
        nhd                     = '' 
        pysda                   = ''
        output                  = ''
        output_bilinear         = ''
        output_neareast         = ''
        output_cubic            = ''

    class FileNames:
        """File names including full path"""
        namelist                = ''
        hucs                    = ''
        domain                  = ''
        domain_mask             = ''
        nhd                     = ''
        dem                     = ''
        dem_user                = ''
        dem_original            = ''
        dem_breached            = ''
        soil_texture            = ''
        soil_transmissivity     = ''
        flow_acc                = ''
        slope                   = ''
        twi                     = ''
        twi_upsample            = ''
        twi_downsample          = ''

    class Variables:
        """Variables"""
        hucs                    = ''
        huc_level               = ''
        start_date              = ''
        end_date                = ''

    class Options:
        """Options"""
        overwrite_flag          = False
        verbose                 = False

    class ParflowSpatialInformation:
        """Parflow grid info - see https://hf-hydrodata.readthedocs.io/en/latest/available_grids.html"""
        conus1_proj      = '+proj=lcc +lat_1=33 +lat_2=45 +lon_0=-96.0 +lat_0=39 +a=6378137.0 +b=6356752.31'
        conus1_spatext   = tuple([-121.47939483437318, 31.651836025255015, -76.09875469594509, 50.49802132270979])
        conus1_transform = rasterio.transform.Affine(1000.0,0.0,-1885055.4995,0.0,1000.0,-604957.0654)
        conus1_shape     = (1888,3342)
        conus2_proj      = '+proj=lcc +lat_1=30 +lat_2=60 +lon_0=-97.0 +lat_0=40.0000076294444 +a=6370000.0 +b=6370000'
        conus2_spatext   = tuple([-126.88755692881833, 21.8170599154073, -64.7677149695924, 53.20274381640737])
        conus2_transform = rasterio.transform.Affine(1000.0,0.0,-2208000.30881173,0.0,1000.0,-1668999.65483222)
        conus2_shape     = (3256,4442)

    class SoilTransmissivity:
        """Soil transmissivity factor. Table 1 of Zhang et al. (2016) https://doi.org/10.5194/bg-13-1387-2016"""
        dt = {'clay heavy'     :3.2,
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

    def __init__(self,filename:str=None):
        """Initialize namelist"""
        self._init_vars()
        
        self._read_namelist(filename)
        self._set_user_inputs()
        self._set_names()
        self._make_subdirectory_structure()

    def _init_vars(self):
        """Initialize variables"""
        self.dirnames            = Namelist.DirectoryNames()
        self.fnames              = Namelist.FileNames()
        self.vars                = Namelist.Variables()
        self.options             = Namelist.Options()
        self.parflow             = Namelist.ParflowSpatialInformation()
        self.soil_transmissivity = Namelist.SoilTransmissivity()
        
    def _set_names(self):
        """Set static directory and file names"""
        self._set_subdirectory_names()
        self._set_file_names()

    def _set_subdirectory_names(self):
        """Set project subdirectory names"""
        self.dirnames.inputs                  = os.path.join(self.dirnames.project,'inputs')
        self.dirnames.wtd                     = os.path.join(self.dirnames.inputs,'wtd')
        self.dirnames.wtd_parflow             = os.path.join(self.dirnames.wtd,'parflow')
        self.dirnames.wtd_parflow_raw         = os.path.join(self.dirnames.wtd_parflow,'raw')
        self.dirnames.wtd_parflow_bilinear    = os.path.join(self.dirnames.wtd_parflow,'bilinear')
        self.dirnames.wtd_parflow_nearest     = os.path.join(self.dirnames.wtd_parflow,'nearest')
        self.dirnames.wtd_parflow_cubic       = os.path.join(self.dirnames.wtd_parflow,'cubic')
        self.dirnames.wtd_fan                 = os.path.join(self.dirnames.wtd,'fan')
        self.dirnames.dem                     = os.path.join(self.dirnames.inputs,'dem')
        self.dirnames.twi                     = os.path.join(self.dirnames.inputs,'twi')
        self.dirnames.soils                   = os.path.join(self.dirnames.inputs,'soils')
        self.dirnames.domain                  = os.path.join(self.dirnames.inputs,'domain')
        self.dirnames.nhd                     = os.path.join(self.dirnames.inputs,'nhd')
        self.dirnames.output                  = os.path.join(self.dirnames.project,'outputs')
        self.dirnames.output_bilinear         = os.path.join(self.dirnames.output,'bilinear')
        self.dirnames.output_neareast         = os.path.join(self.dirnames.output,'nearest')
        self.dirnames.output_cubic            = os.path.join(self.dirnames.output,'cubic')

    def _make_subdirectory_structure(self):
        """Make subdirectory structure"""
        for d in self.dirnames.__dict__:
            if not os.path.isdir(self.dirnames.__dict__[d]):
                os.makedirs(self.dirnames.__dict__[d])

    def _set_file_names(self):
        """Set static files names for intermediate output files"""
        self.fnames.domain              = os.path.join(self.dirnames.domain,'domain.gpkg')
        self.fnames.domain_mask         = os.path.join(self.dirnames.domain,'domain_mask.gpkg')
        self.fnames.hucs                = os.path.join(self.dirnames.domain,'hucs.gpkg')
        self.fnames.nhd                 = os.path.join(self.dirnames.nhd,'nhdhr.gpkg')
        self.fnames.dem                 = os.path.join(self.dirnames.dem,'dem.tif')
        self.fnames.dem_original        = os.path.join(self.dirnames.dem,'dem_original.tif')
        self.fnames.dem_breached        = os.path.join(self.dirnames.dem,'dem_breached.tif')
        self.fnames.soil_texture        = os.path.join(self.dirnames.soils,'soiltexture.gpkg')
        self.fnames.soil_transmissivity = os.path.join(self.dirnames.soils,'soiltransmissivity.tif')
        self.fnames.flow_acc            = os.path.join(self.dirnames.dem,'flow_acc.tif')
        self.fnames.slope               = os.path.join(self.dirnames.dem,'slope.tif')
        self.fnames.twi                 = os.path.join(self.dirnames.twi,'twi.tif')
        self.fnames.twi_upsample        = os.path.join(self.dirnames.twi,'twi_upsample.tif')
        self.fnames.twi_downsample      = os.path.join(self.dirnames.twi,'twi_downsample.tif')

    def _remove_whitespace_outside_quotes(self,line:str):
        result = []
        in_quote = False
        quote_char = None
        for char in line:
            if char in ('"', "'"):
                if in_quote and quote_char == char:
                    in_quote = False
                else:
                    in_quote = True
                    quote_char = char
                result.append(char)
            elif not in_quote and char.isspace():
                continue
            else:
                result.append(char)
        return ''.join(result)

    def _read_namelist(self,filename:str):
        """Read namelist file into generic dictionary"""
        self.fnames.namelist = filename
        if not os.path.isfile(self.fnames.namelist):
            sys.exit('ERROR could not find namelist file '+self.fnames.namelist)
        self.vars.file_inputs = dict()
        namelist_lines = list(open(self.fnames.namelist,'r'))
        for l in namelist_lines:
            try:
                l0 = self._remove_whitespace_outside_quotes(line=l)
                if len(l0) > 0:
                    if str(l[0:1]).find('#') == -1:
                        l1 = l0.split('=')
                        var_name = str(l1[0])
                        var_vals = str(l1[1])
                        var_vals = var_vals.split(',')
                        for i in range(len(var_vals)):
                            val = var_vals[i]
                            if val.startswith("'") and val.endswith("'"):
                                val = val[1:len(val)-1]
                            elif val.startswith('"') and val.endswith('"'):
                                val = val[1:len(val)-1]
                            var_vals[i] = val
                        self.vars.file_inputs[var_name] = var_vals
            except:
                sys.exit('ERROR could not read namelist.txt line: '+l)
        for var_name in self.vars.file_inputs:
            if isinstance(self.vars.file_inputs[var_name],list) and len(self.vars.file_inputs[var_name]) == 1:
                self.vars.file_inputs[var_name] = self.vars.file_inputs[var_name][0]

    def _set_user_inputs(self):
        """Set variables using read-in values"""
        name_project_dir      = 'project_directory'
        name_hucs             = 'hucs'
        name_overwrite        = 'overwrite'
        name_verbose          = 'verbose'
        name_pysda            = 'pysda'
        name_dem              = 'dem'
        name_start_date       = 'start_date'
        name_end_date         = 'end_date'
        req = [name_project_dir,name_hucs,name_pysda,name_start_date,name_end_date]
        for name in req:
            if name not in self.vars.file_inputs: sys.exit('ERROR required variable '+name+' not found in namelist file')
        self.dirnames.project = os.path.abspath(self.vars.file_inputs[name_project_dir])
        self.dirnames.pysda = os.path.abspath(self.vars.file_inputs[name_pysda])
        self.vars.start_date = self.vars.file_inputs[name_start_date]
        self.vars.end_date = self.vars.file_inputs[name_end_date]
        if name_dem in self.vars.file_inputs:
            self.fnames.dem_user = os.path.abspath(self.vars.file_inputs[name_dem])
        self.vars.hucs = self.vars.file_inputs[name_hucs]
        if isinstance(self.vars.hucs,str): self.vars.hucs = [self.vars.hucs]
        levs = [len(self.vars.hucs[i]) for i in range(len(self.vars.hucs))]
        if len(set(levs)) != 1 or levs[0] not in [2,4,6,8,10,12]: sys.exit('ERROR invalid huc level in namelist file')
        self.vars.huc_level = levs[0]
        if name_overwrite in self.vars.file_inputs and self.vars.file_inputs[name_overwrite].upper().find('TRUE') != -1:
            self.vars.overwrite_flag = True
        if name_verbose in self.vars.file_inputs and self.vars.file_inputs[name_verbose].upper().find('TRUE') != -1:
            self.vars.verbose = True

class Domain:

    class Time:
        datetime_dim = ''

    class Spatial:
        domain = ''
        domain_buffered = ''
        nhd    = ''

    class DomainBBOX:
        lat_min = ''
        lat_max = ''
        lon_min = ''
        lon_max = ''

    class ParflowBBOX:
        conus1grid_minx = ''
        conus1grid_miny = ''
        conus1grid_maxx = ''
        conus1grid_maxy = ''
        conus2grid_minx = ''
        conus2grid_miny = ''
        conus2grid_maxx = ''
        conus2grid_maxy = ''

    class Visualization:


    def __init__(self,namelist:Namelist):
        """init"""
        self._init_vars()
        self._set_time_dim(namelist)
        self._set_domain(namelist)
        self._set_nhd_flowlines(namelist)
        self._set_domain_bbox(namelist)
        self._set_parflow_bbox(namelist)

    def get_wtd_data(self,namelist:Namelist):
        """Get water table depth data for domain"""
        self._get_raw_parflow_wtd(namelist)
        #self._get_fan2013_data(namelist) need to fix
        self._increase_wtd_resolution(namelist)
        self._get_domain_mask(namelist)

    def get_twi_and_transmissivity_data(self,namelist:Namelist):
        """Create TWI and transmissivity data for domain"""
        self._get_dem(namelist)
        self._project_dem(namelist)
        self._breach_dem(namelist)
        self._get_facc(namelist)
        self._get_slope(namelist)
        self._calc_twi(namelist)
        self._upsample_twi(namelist)
        self._downsample_twi(namelist)
        self._get_soil_transmissivity(namelist=namelist)

    def _init_vars(self):
        """Initialize variables"""
        self.time    = self.Time()
        self.spatial = self.Spatial()
        self.bbox_domain = self.DomainBBOX()
        self.bbox_parflow = self.ParflowBBOX()
        
    def _set_time_dim(self,namelist:Namelist):
        """Create an array of datetime objects, one for each time step in simulation period"""
        start_date_split = namelist.vars.start_date.split('-')
        start_datetime = datetime.datetime(year=int(start_date_split[0]),month=int(start_date_split[1]),day=int(start_date_split[2]))
        end_date_split = namelist.vars.end_date.split('-')
        end_datetime = datetime.datetime(year=int(end_date_split[0]),month=int(end_date_split[1]),day=int(end_date_split[2]))
        datetime_dim = list()
        idatetime = start_datetime
        while idatetime <= end_datetime:
            datetime_dim.append(idatetime)
            idatetime += datetime.timedelta(days=1)
        self.time.datetime_dim = numpy.array(datetime_dim)

    def _set_domain(self,namelist:Namelist):
        """Set domain spatial boundary"""
        hucstr = 'huc'+str(namelist.vars.huc_level)
        wbdbasins = pygeohydro.WBD(hucstr) 
        wbdbasins  = wbdbasins.byids(hucstr,namelist.vars.hucs)
        self.spatial.domain = wbdbasins.dissolve()
        self.spatial.domain.to_file(namelist.fnames.domain, driver="GPKG")
        del wbdbasins

    def _set_nhd_flowlines(self,namelist:Namelist):
        """Set domain nhd flowlines"""
        if not os.path.isfile(namelist.fnames.nhd) or namelist.options.overwrite_flag:
            self.spatial.nhd = pynhd.NHDPlusHR("flowline").bygeom(self.spatial.domain.geometry[0])
            self.spatial.nhd.to_file(namelist.fnames.nhd, driver="GPKG")

    def _set_domain_bbox(self,namelist:Namelist):
        """Set domain bounding box using buffered domain boundary"""
        self.spatial.domain_buffered = self.spatial.domain.to_crs(namelist.parflow.conus1_proj).buffer(distance=2000).to_crs("EPSG:4326")
        self.bbox_domain.lat_min = float(self.spatial.domain_buffered.bounds['miny'].iloc[0])
        self.bbox_domain.lat_max = float(self.spatial.domain_buffered.bounds['maxy'].iloc[0])
        self.bbox_domain.lon_min = float(self.spatial.domain_buffered.bounds['minx'].iloc[0])
        self.bbox_domain.lon_max = float(self.spatial.domain_buffered.bounds['maxx'].iloc[0])
        
    def _set_parflow_bbox(self,namelist:Namelist):
        """Set ParFlow bounding box"""
        self.bbox_parflow.conus1grid_minx, self.bbox_parflow.conus1grid_miny = hf_hydrodata.from_latlon("conus1", self.bbox_domain.lat_min, self.bbox_domain.lon_min)
        self.bbox_parflow.conus1grid_maxx, self.bbox_parflow.conus1grid_maxy = hf_hydrodata.from_latlon("conus1", self.bbox_domain.lat_max, self.bbox_domain.lon_max)
        self.bbox_parflow.conus1grid_minx, self.bbox_parflow.conus1grid_miny = math.floor(self.bbox_parflow.conus1grid_minx), math.floor(self.bbox_parflow.conus1grid_miny)
        self.bbox_parflow.conus1grid_maxx, self.bbox_parflow.conus1grid_maxy = math.ceil(self.bbox_parflow.conus1grid_maxx),  math.ceil(self.bbox_parflow.conus1grid_maxy)
        self.bbox_parflow.conus2grid_minx, self.bbox_parflow.conus2grid_miny = hf_hydrodata.from_latlon("conus2", self.bbox_domain.lat_min, self.bbox_domain.lon_min)
        self.bbox_parflow.conus2grid_maxx, self.bbox_parflow.conus2grid_maxy = hf_hydrodata.from_latlon("conus2", self.bbox_domain.lat_max, self.bbox_domain.lon_max)
        self.bbox_parflow.conus2grid_minx, self.bbox_parflow.conus2grid_miny = math.floor(self.bbox_parflow.conus2grid_minx), math.floor(self.bbox_parflow.conus2grid_miny)
        self.bbox_parflow.conus2grid_maxx, self.bbox_parflow.conus2grid_maxy = math.ceil(self.bbox_parflow.conus2grid_maxx),  math.ceil(self.bbox_parflow.conus2grid_maxy)
        latlon_bounds = hf_hydrodata.to_latlon("conus1", *[self.bbox_parflow.conus1grid_minx, self.bbox_parflow.conus1grid_miny, self.bbox_parflow.conus1grid_maxx, self.bbox_parflow.conus1grid_maxy])
        self.bbox_domain.lon_min = latlon_bounds[1]
        self.bbox_domain.lat_min = latlon_bounds[0]
        self.bbox_domain.lon_max = latlon_bounds[3]
        self.bbox_domain.lat_max = latlon_bounds[2]

    def _get_raw_parflow_wtd(self,namelist:Namelist):
        """Get raw ParFlow water table depth data"""
        download_flag = False
        for idatetime in self.time.datetime_dim:
            fname_wtd = os.path.join(namelist.dirnames.wtd_parflow_raw,'wtd_'+idatetime.strftime('%Y%m%d')+'.tiff')
            if not os.path.isfile(fname_wtd):
                download_flag = True
                break
        if download_flag:
            start_date_str = self.time.datetime_dim[0].strftime('%Y-%m-%d')
            end_date_str = (self.time.datetime_dim[len(self.time.datetime_dim)-1]+datetime.timedelta(days=1)).strftime('%Y-%m-%d')
            options_wtd = {"dataset": "conus1_baseline_mod", "variable": "water_table_depth", "temporal_resolution": "daily", "start_time": start_date_str, "end_time": end_date_str, "grid_bounds":[self.bbox_parflow.conus1grid_minx,self.bbox_parflow.conus1grid_miny,self.bbox_parflow.conus1grid_maxx,self.bbox_parflow.conus1grid_maxy]}  
            hf_data = hf_hydrodata.get_gridded_data(options_wtd)
            if hf_data.shape[0] != int((self.time.datetime_dim[len(self.time.datetime_dim)-1]-self.time.datetime_dim[0]).days) + 1: sys.exit('ERROR hydroframe returned data of unexpected time length.')
            hf_conus1grid_temp = numpy.empty(namelist.parflow.conus1_shape)
            for i in range(len(self.time.datetime_dim)):
                idatetime = self.time.datetime_dim[i]
                hf_conus1grid_temp[self.bbox_parflow.conus1grid_miny:self.bbox_parflow.conus1grid_maxy,self.bbox_parflow.conus1grid_minx:self.bbox_parflow.conus1grid_maxx] = hf_data[i,:,:]
                memfile = rasterio.io.MemoryFile()
                hf_conus1data = memfile.open(driver = "GTiff", height = hf_conus1grid_temp.shape[0], width = hf_conus1grid_temp.shape[1], crs=namelist.parflow.conus1_proj, transform = namelist.parflow.conus1_transform, nodata = numpy.nan, count = 1, dtype = numpy.float64)
                hf_conus1data.write(hf_conus1grid_temp,1)
                fname = os.path.join(namelist.dirnames.wtd_parflow_raw,'wtd_'+idatetime.strftime('%Y%m%d')+'.tiff')
                wtd_data, wtd_transform = rasterio.mask.mask(hf_conus1data, self.spatial.domain_buffered.to_crs(hf_conus1data.crs), crop=True, all_touched=True, filled =True, nodata = numpy.nan)
                wtd_meta = hf_conus1data.meta
                wtd_meta.update({"driver": "GTiff","height": wtd_data.shape[1],"width": wtd_data.shape[2],"transform": wtd_transform, "nodata" : numpy.nan})
                with rasterio.open(fname,'w',**wtd_meta) as wtd_dataset:
                    wtd_dataset.write(wtd_data[0,:,:],1)

    def _get_fan2013_data(self,namelist:Namelist):
        """Get Fan (2013) equilibrium water table depth"""
        fname_wtd = os.path.join(namelist.dirnames.wtd_fan,'wtd_equilibrium.tiff')
        if not os.path.isfile(fname_wtd):
            options_wtd = {"dataset": "fan_2013","variable": "water_table_depth", "temporal_resolution": "long_term", "grid_bounds":[self.bbox_parflow.conus2grid_minx,self.bbox_parflow.conus2grid_miny,self.bbox_parflow.conus2grid_maxx,self.bbox_parflow.conus2grid_maxy]}  
            hf_data = hf_hydrodata.get_gridded_data(options_wtd)
            if hf_data.shape[0] != 1: sys.exit('ERROR hydroframe returned data of unexpected time length.')
            hf_conus2grid_temp = numpy.empty(namelist.parflow.conus2_shape)
            hf_conus2grid_temp[self.bbox_parflow.conus2grid_miny:self.bbox_parflow.conus2grid_maxy,self.bbox_parflow.conus2grid_minx:self.bbox_parflow.conus2grid_maxx] = hf_data[i,:,:]
            memfile = rasterio.io.MemoryFile()
            hf_conus2data = memfile.open(driver="GTiff",height=hf_conus2grid_temp.shape[0],width=hf_conus2grid_temp.shape[1],crs=namelist.parflow.conus2_proj,transform=namelist.parflow.conus2_transform,nodata=numpy.nan,count=1,dtype=numpy.float64)
            hf_conus2data.write(hf_conus2grid_temp,1)
            wtd_data, wtd_transform = rasterio.mask.mask(hf_conus2data, self.spatial.domain_buffered.to_crs(hf_conus2data.crs), crop=True, all_touched=True, filled =True, nodata = numpy.nan)
            wtd_meta = hf_conus2data.meta
            wtd_meta.update({"driver": "GTiff","height": wtd_data.shape[1],"width": wtd_data.shape[2],"transform": wtd_transform, "nodata" : numpy.nan})
            with rasterio.open(fname_wtd,'w',**wtd_meta) as wtd_dataset:
                wtd_dataset.write(wtd_data[0,:,:],1)
            del hf_data, hf_conus2grid_temp, memfile, hf_conus2data, wtd_data, wtd_meta, wtd_dataset

    def _increase_wtd_resolution(self,namelist:Namelist):
        """Increase resolution of ParFlow grid to match DEM - maybe just do this on the fly, in-memory, per time step, after putting the topographic info into this grid using one instance of the wtd grid"""
        #resample_methods = {'bilinear':[rasterio.enums.Resampling.bilinear,namelist.dirnames.wtd_parflow_bilinear], #doing all three eats too much harddrive space
        #                    'cubic'   :[rasterio.enums.Resampling.cubic,   namelist.dirnames.wtd_parflow_cubic],
        #                    'nearest' :[rasterio.enums.Resampling.nearest, namelist.dirnames.wtd_parflow_nearest]}
        resample_methods = {'bilinear':[rasterio.enums.Resampling.bilinear,namelist.dirnames.wtd_parflow_bilinear]} 
        for resample_method_name, [resample_method, resample_dir] in resample_methods.items():
            for idatetime in self.time.datetime_dim:
                fname_wtd = os.path.join(namelist.dirnames.wtd_parflow_raw,'wtd_'+idatetime.strftime('%Y%m%d')+'.tiff')
                fname_wtd_hr = os.path.join(resample_dir,'wtd_'+idatetime.strftime('%Y%m%d')+'.tiff')
                if os.path.isfile(fname_wtd):
                    if not os.path.isfile(fname_wtd_hr) or namelist.options.overwrite_flag:
                        with rasterio.open(fname_wtd) as wtd_dataset:
                            wtd_data_hr = wtd_dataset.read(out_shape=(wtd_dataset.count,int(wtd_dataset.height * 500),int(wtd_dataset.width * 500)),resampling=resample_method)
                            wtd_transform_hr = wtd_dataset.transform * wtd_dataset.transform.scale((wtd_dataset.width / wtd_data_hr.shape[-1]),(wtd_dataset.height / wtd_data_hr.shape[-2]))
                            wtd_meta_hr = wtd_dataset.meta
                            wtd_meta_hr.update({"driver": "GTiff","height": wtd_data_hr.shape[1],"width": wtd_data_hr.shape[2],"transform": wtd_transform_hr, "nodata" : numpy.nan})
                            with rasterio.open(fname_wtd_hr, "w", **wtd_meta_hr) as wtd_dataset_hr:
                                wtd_dataset_hr.write(wtd_data_hr)

    def _get_domain_mask(self,namelist:Namelist):
        """Get domain mask"""
        if not os.path.isfile(namelist.fnames.domain_mask) or namelist.options.overwrite_flag:
            fname_dummy_wtd_hr = self._get_dummy_hrwtd_fname(namelist)
            with rasterio.open(fname_dummy_wtd_hr,'r') as wtd_highres:
                domain_data = wtd_highres.read(1)
                domain_meta = wtd_highres.meta
                domain_crs = wtd_highres.crs
                domain_transform = wtd_highres.transform
                domain_mask = rasterio.features.rasterize(shapes=self.spatial.domain.to_crs(domain_crs)['geometry'],out_shape=domain_data.shape,transform=domain_transform,fill=0,all_touched=True,dtype=rasterio.uint8,default_value=1)
                domain_meta.update({"driver": "GTiff","height": domain_data.shape[0],"width": domain_data.shape[1],"transform": domain_transform,"dtype": rasterio.uint8,"nodata":0})
                with rasterio.open(namelist.fnames.domain_mask, 'w', **domain_meta) as dst:
                    dst.write(domain_mask,indexes=1)

    def _get_soil_transmissivity(self,namelist:Namelist):
        """Get soil transmissivity - uses pysda via https://github.com/ncss-tech/pysda.git"""
        if not os.path.isfile(namelist.fnames.soil_transmissivity) or namelist.options.overwrite_flag:
            if namelist.dirnames.pysda not in sys.path: sys.path.append(namelist.dirnames.pysda)
            import sdapoly, sdaprop
            soils_aoi = sdapoly.gdf(self.spatial.domain)
            sandtotal_r=sdaprop.getprop(df=soils_aoi,column='mukey',method='dom_comp_num',top=0,bottom=400,prop='sandtotal_r',minmax=None,prnt=False,meta=False)
            claytotal_r=sdaprop.getprop(df=soils_aoi,column='mukey',method='dom_comp_num',top=0,bottom=400,prop='claytotal_r',minmax=None,prnt=False,meta=False)
            soils_aoi = soils_aoi.merge(pandas.merge(sandtotal_r,claytotal_r,on='mukey'),on='mukey')
            def calc_texture(row): 
                try: sand = float(row['sandtotal_r'])
                except: return 'None'
                try: clay = float(row['claytotal_r'])
                except: return 'None'
                return soiltexture.getTexture(sand,clay)
            soils_aoi['texture'] = soils_aoi.apply(calc_texture, axis=1)
            def calc_f(row):
                if row['texture'] in namelist.soil_transmissivity.dt: return namelist.soil_transmissivity.dt[row['texture']]
                else: 
                    muname = str(row['muname']).upper()
                    if muname.find('WATER') != -1 or muname.find('DAM') != -1: return numpy.mean(list(namelist.soil_transmissivity.dt.values()))
                    else: sys.exit('ERROR: Could not find transmissivity decay factor for soil texture '''+muname+"'")
            soils_aoi['f'] = soils_aoi.apply(calc_f, axis=1)
            with rasterio.open(self._get_dummy_hrwtd_fname(namelist),'r') as wtd_highres:
                dummy_meta = wtd_highres.meta
                dummy_data = wtd_highres.read(1)
                soils_aoi = soils_aoi.to_crs(wtd_highres.crs)
                soils_aoi.to_crs('EPSG:4326').to_file(namelist.fnames.soil_texture, driver="GPKG")
                soils_shapes = ((geom,value) for geom, value in zip(soils_aoi.geometry, soils_aoi['f']))
                texture_data = rasterio.features.rasterize(shapes=soils_shapes,out_shape=dummy_data.shape,transform=wtd_highres.transform,fill=numpy.nan,all_touched=True,dtype=rasterio.float32,default_value=numpy.nan)
                dummy_meta.update({"driver": "GTiff","height": dummy_data.shape[0],"width": dummy_data.shape[1],"transform": wtd_highres.transform,"dtype": rasterio.float32,"nodata":numpy.nan})
                with rasterio.open(namelist.fnames.soil_transmissivity, 'w', **dummy_meta) as dst:
                    dst.write(texture_data,indexes=1)

    def _get_dummy_hrwtd_fname(self,namelist:Namelist):
        """Check if water table depth data is available"""
        fname = ''
        for wtd_dir in [namelist.dirnames.wtd_parflow_bilinear,namelist.dirnames.wtd_parflow_nearest,namelist.dirnames.wtd_parflow_cubic]:
            for idatetime in self.time.datetime_dim:
                fname_wtd = os.path.join(wtd_dir,'wtd_'+idatetime.strftime('%Y%m%d')+'.tiff')
                if os.path.isfile(fname_wtd):
                    fname = fname_wtd
                    break
            if len(fname) > 0: break
        if len(fname) == 0: sys.exit('ERROR: Resampled water table depth data was not found')
        return fname

    def _get_dem(self,namelist:Namelist):
        """Get DEM -- py3dep.check_3dep_availability may show availability of 1 meter resolution DEM but py3dep.get_dem does not work if called with resolution < 10, in my experience"""
        if not os.path.isfile(namelist.fnames.dem_original) or namelist.options.overwrite_flag:
            if os.path.isfile(namelist.fnames.dem_user):
                shutil.copy2(namelist.fnames.dem_user,namelist.fnames.dem_original)
            else:
                #dtavailability = py3dep.check_3dep_availability([lon_min,lat_min,lon_max,lat_max]) # can see dem resolution availability here
                dem_original = py3dep.get_dem(geometry=[self.bbox_domain.lon_min,self.bbox_domain.lat_min,self.bbox_domain.lon_max,self.bbox_domain.lat_max],resolution=10) # wpn't work with < 10 m
                dem_original.rio.to_raster(namelist.fnames.dem_original)
                del dem_original

    def _project_dem(self,namelist:Namelist):
        """Reproject DEM to match resampled (high resolution) parflow grid"""
        if not os.path.isfile(namelist.fnames.dem) or namelist.options.overwrite_flag:
            with rasterio.open(namelist.fnames.dem_original,'r') as dem_original: 
                fname_dummy_wtd_hr = self._get_dummy_hrwtd_fname(namelist)
                with rasterio.open(fname_dummy_wtd_hr,'r') as dummy_hr_data:
                    dem_reprojected_data, dem_reprojected_transform = rasterio.warp.reproject(
                            source = dem_original.read(1),
                            destination = dummy_hr_data.read(1),
                            src_transform=dem_original.transform,
                            src_crs=dem_original.crs,
                            dst_transform=dummy_hr_data.transform,
                            dst_crs=dummy_hr_data.crs,
                            resampling=rasterio.enums.Resampling.bilinear)
                    domain_mask = rasterio.open(namelist.fnames.domain_mask).read(1)
                    #dem_reprojected_data = dem_reprojected_data[0,:,:]
                    dem_reprojected_data = numpy.where(domain_mask==1,dem_reprojected_data,numpy.nan)
                    dem_reprojected_meta = dummy_hr_data.meta
                    dem_reprojected_meta.update({"driver": "GTiff","height": dem_reprojected_data.shape[0],"width": dem_reprojected_data.shape[1],"transform": dem_reprojected_transform, "nodata":numpy.nan})
                    with rasterio.open(namelist.fnames.dem, "w", **dem_reprojected_meta) as dem_reprojected:
                        dem_reprojected.write(dem_reprojected_data,1)

    def _breach_dem(self,namelist:Namelist):
        """Breach the DEM (minimally invasive alternative to filling the DEM?) (see https://www.whiteboxgeo.com/manual/wbt_book/available_tools/hydrological_analysis.html#BreachDepressionsLeastCost)"""
        wbe = whitebox_workflows.WbEnvironment()
        if not os.path.isfile(namelist.fnames.dem_breached) or namelist.options.overwrite_flag:
            dem_wbe = wbe.read_raster(namelist.fnames.dem)
            dem_breached_wbe = wbe.breach_depressions_least_cost(dem=dem_wbe)
            wbe.write_raster(dem_breached_wbe, namelist.fnames.dem_breached, compress=False)
            del dem_wbe, dem_breached_wbe

    def _get_facc(self,namelist:Namelist):
        wbe = whitebox_workflows.WbEnvironment()
        if not os.path.isfile(namelist.fnames.flow_acc) or namelist.options.overwrite_flag:
            dem_breached_wbe = wbe.read_raster(namelist.fnames.dem_breached)
            acc_wbe = wbe.dinf_flow_accum(dem=dem_breached_wbe,out_type='sca',log_transform=False)                                         
            wbe.write_raster(acc_wbe, namelist.fnames.flow_acc, compress=False)
            del dem_breached_wbe, acc_wbe

    def _get_slope(self,namelist:Namelist):
        """Calculate slope (required for TWI calculation)"""
        wbe = whitebox_workflows.WbEnvironment()
        if not os.path.isfile(namelist.fnames.slope) or namelist.options.overwrite_flag:
            dem_wbe = wbe.read_raster(namelist.fnames.dem_breached)
            slp_wbe = wbe.slope(dem=dem_wbe)   
            wbe.write_raster(slp_wbe, namelist.fnames.slope, compress=False)
            del dem_wbe, slp_wbe

    def _calc_twi(self,namelist:Namelist):
        """Calculate TWI"""
        wbe = whitebox_workflows.WbEnvironment()
        if not os.path.isfile(namelist.fnames.twi) or namelist.options.overwrite_flag:
            acc_wbe = wbe.read_raster(namelist.fnames.flow_acc)
            slp_wbe = wbe.read_raster(namelist.fnames.slope)
            twi_wbe = wbe.wetness_index(specific_catchment_area=acc_wbe,slope=slp_wbe)
            wbe.write_raster(twi_wbe, namelist.fnames.twi, compress=False)
            del acc_wbe, slp_wbe, twi_wbe

    def _upsample_twi(self,namelist:Namelist):
        """Upsample TWI"""
        if not os.path.isfile(namelist.fnames.twi_upsample) or namelist.options.overwrite_flag:
            with rasterio.open(namelist.fnames.twi,'r') as twi_dataset:
                twi_upsample_data      = twi_dataset.read(out_shape=(twi_dataset.count,int(twi_dataset.height / 100),int(twi_dataset.width / 100)),resampling=rasterio.enums.Resampling.average)
                twi_upsample_transform = twi_dataset.transform * twi_dataset.transform.scale((twi_dataset.width / twi_upsample_data.shape[-1]),(twi_dataset.height / twi_upsample_data.shape[-2]))
                twi_upsample_meta      = twi_dataset.meta
                twi_upsample_meta.update({"driver": "GTiff","height": twi_upsample_data.shape[1],"width": twi_upsample_data.shape[2],"transform": twi_upsample_transform})
                with rasterio.open(namelist.fnames.twi_upsample, "w", **twi_upsample_meta) as twi_upsample_dataset:
                    twi_upsample_dataset.write(twi_upsample_data)

    def _downsample_twi(self,namelist:Namelist):
        """Downsample TWI"""
        if not os.path.isfile(namelist.fnames.twi_downsample) or namelist.options.overwrite_flag:
            with rasterio.open(namelist.fnames.twi_upsample,'r') as twi_upsample_dataset:
                twi_downsample_data      = twi_upsample_dataset.read(out_shape=(twi_upsample_dataset.count,int(twi_upsample_dataset.height * 100),int(twi_upsample_dataset.width * 100)),resampling=rasterio.enums.Resampling.nearest)
                twi_downsample_transform = twi_upsample_dataset.transform * twi_upsample_dataset.transform.scale((twi_upsample_dataset.width / twi_downsample_data.shape[-1]),(twi_upsample_dataset.height / twi_downsample_data.shape[-2]))
                twi_downsample_meta      = twi_upsample_dataset.meta
                twi_downsample_meta.update({"driver": "GTiff","height": twi_downsample_data.shape[1],"width": twi_downsample_data.shape[2],"transform": twi_downsample_transform})
                with rasterio.open(namelist.fnames.twi_downsample, "w", **twi_downsample_meta) as twi_downsample_dataset:
                    twi_downsample_dataset.write(twi_downsample_data)

    def calc_inundation(self,namelist:Namelist):
        """Calculate inundation"""
        self._calc_inundated_area(namelist)
        self._calc_summary_grid(namelist)
        self._calc_zerowtd(namelist)

    def _calc_inundation_itime(self,wtd_mean,twi_local,twi_mean,f,domain_mask):
        """Calculate inundation using TOPMODEL-based equation - see Equation 3 of Zhang et al. (2016) (https://doi.org/10.5194/bg-13-1387-2016)
        Arguments:
            wtd_mean:    grid of mean water table depth values  (zeta sub m in equation 3 of Zhang et al.) 
            twi_local:   grid of local TWI values               (lambda sub l in equation 3 of Zhang et al.)
            twi_mean:    grid of mean TWI values                (lamda sub m in equation 3 of Zhang et al.)
            f:           grid of f parameter values             (f in equation 3 and table 1 of Zhang et al.)
            domain_mask: grid of model domain                   (1=domain,0=not domain)
        Returns:
            wtd_local:   grid of local water table depth values (zeta sub l in equation 3 of Zhang et al.)
        """
        wtd_mean  = wtd_mean*(-1)                                                             # values must be negative so multiply by -1
        wtd_local = numpy.where(domain_mask==1,(1/f)*(twi_local-twi_mean)+wtd_mean,numpy.nan) # calculate local water depth using equation 3 from Zhang et al. (2016) where domain_mask=1 (i.e., within the model domain), otherise give a NaN value 
        wtd_local = numpy.where(wtd_local>=0,1,numpy.nan)                                     # give value of 1 where local water table depth is >= 0 (i.e. at or above the surface), otherwise give a NaN value
        return wtd_local

    def _calc_inundated_area(self,namelist:Namelist):
        """Calculate inundated area"""
        twi_local = rasterio.open(namelist.fnames.twi,'r').read(1)
        twi_mean = rasterio.open(namelist.fnames.twi_downsample,'r').read(1)
        domain_mask = rasterio.open(namelist.fnames.domain_mask,'r').read(1)
        transmissivity_decay_factor = rasterio.open(namelist.fnames.soil_transmissivity,'r').read(1)
        for method in ['bilinear','nearest','cubic']:
            if method == 'bilinear': 
                wtd_dir = namelist.dirnames.wtd_parflow_bilinear
                out_dir = namelist.dirnames.output_bilinear
            elif method == 'nearest': 
                wtd_dir = namelist.dirnames.wtd_parflow_nearest
                out_dir = namelist.dirnames.output_neareast
            elif method == 'cubic': 
                wtd_dir = namelist.dirnames.wtd_parflow_cubic
                out_dir = namelist.dirnames.output_cubic
            for idatetime in self.time.datetime_dim:
                fname_wtd_mean = os.path.join(wtd_dir,'wtd_'+idatetime.strftime('%Y%m%d')+'.tiff')
                fname_output = os.path.join(out_dir,'inundatedarea_'+idatetime.strftime('%Y%m%d')+'.tiff')
                if os.path.isfile(fname_wtd_mean):
                    if not os.path.isfile(fname_output) or namelist.options.overwrite_flag:
                        with rasterio.open(fname_wtd_mean,'r') as wtd_mean_dataset:
                            wtd_mean = wtd_mean_dataset.read(1)
                            wtd_local = self._calc_inundation_itime(wtd_mean,twi_local,twi_mean,transmissivity_decay_factor,domain_mask)
                            with rasterio.open(fname_output, "w", **wtd_mean_dataset.meta) as wtd_local_dataset:
                                wtd_local_dataset.write(wtd_local,1)
        del twi_local, twi_mean, domain_mask

    def _calc_summary_grid(self,namelist:Namelist):
        """Calculate summary grid of inundated area"""
        domain_mask = rasterio.open(namelist.fnames.domain_mask,'r').read(1)
        for method in ['bilinear','nearest','cubic']:
            if method == 'bilinear': out_dir = namelist.dirnames.output_bilinear
            elif method == 'nearest': out_dir = namelist.dirnames.output_neareast
            elif method == 'cubic': out_dir = namelist.dirnames.output_cubic
            fname_output = os.path.join(out_dir,'summary_grid_'+self.time.datetime_dim[0].strftime('%Y%m%d')+'_to_'+self.time.datetime_dim[len(self.time.datetime_dim)-1].strftime('%Y%m%d')+'.tiff')
            if not os.path.isfile(fname_output) or namelist.options.overwrite_flag:
                count = numpy.where(domain_mask==1,0,numpy.nan)
                for idatetime in self.time.datetime_dim:
                    fname_wtd_local = os.path.join(out_dir,'inundatedarea_'+idatetime.strftime('%Y%m%d')+'.tiff') 
                    if os.path.isfile(fname_wtd_local):
                        inundation_data = rasterio.open(fname_wtd_local,'r').read(1)
                        count += numpy.where(inundation_data==1,1,0)
                count = numpy.where(count==0,numpy.nan,count)
                if numpy.any(count>0):
                    with rasterio.open(fname_output, "w", **rasterio.open(fname_wtd_local,'r').meta) as summary_dataset:
                        summary_dataset.write(count,1)
        del domain_mask

    def _calc_zerowtd(self,namelist:Namelist):
        """Calculate zero water table depth"""
        for method in ['bilinear','nearest','cubic']:
            if method == 'bilinear': out_dir = namelist.dirnames.output_bilinear
            elif method == 'nearest': out_dir = namelist.dirnames.output_neareast
            elif method == 'cubic': out_dir = namelist.dirnames.output_cubic
            fname_output = os.path.join(out_dir,'mean_wtd_if_local_wtd_equals_0.tiff')
            if not os.path.isfile(fname_output) or namelist.options.overwrite_flag:
                domain_mask = rasterio.open(namelist.fnames.domain_mask,'r').read(1)
                twi_local = rasterio.open(namelist.fnames.twi,'r').read(1)
                twi_local = numpy.where(domain_mask==1,twi_local,numpy.nan)
                twi_mean = rasterio.open(namelist.fnames.twi_downsample,'r').read(1)
                twi_mean = numpy.where(domain_mask==1,twi_mean,numpy.nan)
                trans_decay_factor = rasterio.open(namelist.fnames.soil_transmissivity,'r').read(1)
                trans_decay_factor = numpy.where(domain_mask==1,trans_decay_factor,numpy.nan)
                fname_dummy_highres = self._get_dummy_hrwtd_fname(namelist)
                with rasterio.open(fname_dummy_highres,'r') as wtd_mean_t0:
                    wtd_local = wtd_mean_t0.read(1)
                    wtd_local = numpy.where(domain_mask==1,0,numpy.nan)
                    wtd_mean = numpy.where(domain_mask==1,((1/trans_decay_factor)*(twi_local-twi_mean)-wtd_local)*(-1),numpy.nan)
                    with rasterio.open(fname_output, "w", **wtd_mean_t0.meta) as wtd_mean_dataset_wtd_local_0:
                        wtd_mean_dataset_wtd_local_0.write(wtd_mean,1)
