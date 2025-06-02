import os,sys,math,rasterio,yaml,datetime,numpy,geopandas,hf_hydrodata,pygeohydro,shapely

class Namelist:

    class DirectoryNames:
        """Directory names"""
        project                 = ''
        inputs                  = ''
        wtd                     = ''
        wtd_parflow             = ''
        wtd_parflow_raw         = ''
        wtd_parflow_resampled   = ''
        wtd_fan                 = ''
        topo                    = ''
        dem                     = ''
        dem_breached            = ''
        twi                     = ''
        twi_mean                = ''
        slope                   = ''
        strm_mask               = ''
        facc                    = ''
        soils                   = ''
        domain                  = ''
        nhd                     = '' 
        pysda                   = ''
        output                  = ''
        output_raw              = ''
        output_summary          = ''

    class Time:
        start_date              = ''
        end_date                = ''
        datetime_dim            = ''

    class FileNames:
        """File names including full path"""
        namlistyaml             = ''
        domain                  = ''
        domain_mask             = ''
        hucs                    = ''
        huc                     = ''
        huc_mask                = ''
        huc_buffered            = ''
        nhd                     = ''
        dem                     = ''
        dem_user                = ''
        dem_breached            = ''
        soil_texture            = ''
        soil_transmissivity     = ''
        flow_acc                = ''
        facc_strm_mask          = ''
        slope                   = ''
        twi                     = ''
        twi_mean                = ''

    class BBoxDomain:
        lat_min = ''
        lat_max = ''
        lon_min = ''
        lon_max = ''

    class BBoxParflow:
        conus1grid_minx = ''
        conus1grid_miny = ''
        conus1grid_maxx = ''
        conus1grid_maxy = ''
        conus2grid_minx = ''
        conus2grid_miny = ''
        conus2grid_maxx = ''
        conus2grid_maxy = ''

    class ParallelProcessing:
        flag = False
        core_count              = ''
        huc_break_lvl           = ''

    class Variables:
        """Variables"""
        huc                     = ''
        huc_level               = ''
        start_date              = ''
        end_date                = ''
        namelist                = ''
        facc_strm_threshold     = 0
        dem_rez                 = None
        dem_crs                 = ''
        dem_transform           = ''
        dem_bounds              = ''
        dem_shape               = ''

    class Options:
        """Options"""
        overwrite_flag          = False
        verbose                 = False
        resample_method         = None
        name_resample_method    = ''

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

    def __init__(self,filename:str):
        """Initialize namelist"""
        self._init_vars()
        self._read_inputyaml(filename)
        self._set_user_inputs()
        self._set_names()
        self._make_subdirectories()
        self._set_user_inputs()

    def _init_vars(self):
        """Initialize variables"""
        self.dirnames            = Namelist.DirectoryNames()
        self.fnames              = Namelist.FileNames()
        self.vars                = Namelist.Variables()
        self.options             = Namelist.Options()
        self.parflow             = Namelist.ParflowSpatialInformation()
        self.soil_transmissivity = Namelist.SoilTransmissivity()
        self.time                = Namelist.Time()
        self.bbox_domain         = Namelist.BBoxDomain()
        self.bbox_parflow        = Namelist.BBoxParflow()
        self.pp                  = Namelist.ParallelProcessing()
        
    def _set_names(self):
        """Set static directory and file names"""
        self._set_subdirectory_names()
        self._set_file_names()
        self._set_file_full_path_names()

    def _set_subdirectory_names(self):
        """Set project subdirectory names"""
        self.dirnames.inputs                  = os.path.join(self.dirnames.project,                 'inputs')
        self.dirnames.wtd                     = os.path.join(self.dirnames.inputs,                  'wtd')
        self.dirnames.wtd_parflow             = os.path.join(self.dirnames.wtd,                     'parflow')
        self.dirnames.wtd_parflow_raw         = os.path.join(self.dirnames.wtd_parflow,             'raw')
        self.dirnames.wtd_parflow_resampled   = os.path.join(self.dirnames.wtd_parflow,             'resampled')
        self.dirnames.wtd_fan                 = os.path.join(self.dirnames.wtd,                     'fan')
        self.dirnames.topo                    = os.path.join(self.dirnames.inputs,                  'topo')
        self.dirnames.soils                   = os.path.join(self.dirnames.inputs,                  'soils')
        self.dirnames.domain                  = os.path.join(self.dirnames.inputs,                  'domain')
        self.dirnames.nhd                     = os.path.join(self.dirnames.inputs,                  'nhd')
        self.dirnames.output                  = os.path.join(self.dirnames.project,                 'outputs')
        self.dirnames.output_raw              = os.path.join(self.dirnames.output,                  'raw')
        self.dirnames.output_summary          = os.path.join(self.dirnames.output,                  'summary')

    def _make_subdirectories(self):
        """Make subdirectory structure"""
        for d in self.dirnames.__dict__:
            if not os.path.isdir(self.dirnames.__dict__[d]):
                os.makedirs(self.dirnames.__dict__[d])

    def _set_file_names(self):
        """Set static files names for intermediate output files"""
        self.fnames.huc                 = 'huc.gpkg'
        self.fnames.huc_mask            = 'huc.tiff'
        self.fnames.huc_buffered        = 'huc.gpkg'
        self.fnames.hucs                = 'hucs.gpkg'
        self.fnames.nhd                 = 'nhd_hr.gpkg'
        self.fnames.dem                 = 'dem.tiff'
        self.fnames.dem_breached        = 'dem_breached.tiff'
        self.fnames.soil_texture        = 'soil_texture.gpkg'
        self.fnames.soil_transmissivity = 'soil_transmissivity.tiff'
        self.fnames.flow_acc            = 'flow_acc.tiff'
        self.fnames.slope               = 'slope.tiff'
        self.fnames.facc_strm_mask      = 'facc_strm_mask.tiff'
        self.fnames.twi                 = 'twi.tiff'
        self.fnames.twi_mean            = 'twi_mean.tiff'
    
    def _set_file_full_path_names(self):
        self.fnames.hucs                = os.path.join(self.dirnames.domain,'hucs.gpkg')
        self.fnames.domain              = os.path.join(self.dirnames.domain,'domain.gpkg')
        self.fnames.domain_mask         = os.path.join(self.dirnames.domain,'domain_mask.tiff')

    def _read_inputyaml(self,fname:str):
        self.fnames.namlistyaml = fname
        with open(self.fnames.namlistyaml,'r') as yamlf:
            try: 
                self.vars.namelist = yaml.safe_load(yamlf)
            except yaml.YAMLError as exc: 
                print(exc)

    def _set_user_inputs(self):
        """Set variables using read-in values"""
        name_project_dir         = 'project_directory'
        name_huc                 = 'huc'
        name_overwrite           = 'overwrite'
        name_verbose             = 'verbose'
        name_pysda               = 'pysda'
        name_dem                 = 'dem'
        name_start_date          = 'start_date'
        name_end_date            = 'end_date'
        name_facc_strm_threshold = 'facc_strm_threshold'
        name_dem_rez             = 'dem_resolution'
        name_resample_method     = 'wtd_resample_method'
        name_pp_core_count       = 'pp_core_count'
        name_pp_huc_break_lvl    = 'pp_huc_break_lvl'
        req_names = [name_project_dir,
                     name_huc,
                     name_pysda,
                     name_start_date,
                     name_end_date,
                     name_facc_strm_threshold]
        for name in req_names:
            if name not in self.vars.namelist: 
                sys.exit('ERROR required variable '+name+' not found '+self.fnames.namlistyaml)
        self.dirnames.project = os.path.abspath(self.vars.namelist[name_project_dir])
        self.dirnames.pysda   = os.path.abspath(self.vars.namelist[name_pysda])
        if self.dirnames.pysda not in sys.path: sys.path.append(self.dirnames.pysda)
        self.vars.huc         = self.vars.namelist[name_huc]
        self.time.start_date  = self.vars.namelist[name_start_date]
        self.time.end_date    = self.vars.namelist[name_end_date]
        self.vars.huc_level   = len(self.vars.huc)
        if self.vars.huc_level not in (2,4,6,8,10,12): 
            sys.exit('ERROR invalid huc level in namelist file')
        try:
            threshold = int(self.vars.namelist[name_facc_strm_threshold])
        except ValueError:
            sys.exit('ERROR facc_strm_threshold must be an integer')
        self.vars.facc_strm_threshold = threshold
        if name_overwrite in self.vars.namelist and str(self.vars.namelist[name_overwrite]).upper().find('TRUE') != -1:
            self.options.overwrite_flag = True
        if name_verbose in self.vars.namelist and str(self.vars.namelist[name_verbose]).upper().find('TRUE') != -1:
            self.options.verbose = True
        if name_dem in self.vars.namelist:
            self.fnames.dem_user = os.path.abspath(self.vars.namelist[name_dem])
        if name_dem_rez in self.vars.namelist:
            try: self.vars.dem_rez = int(self.vars.namelist[name_dem_rez])
            except: sys.exit('ERROR '+name_dem_rez+' must be an integer')
        if name_pp_core_count in self.vars.namelist or name_pp_huc_break_lvl in self.vars.namelist:
            self.pp.flag = True
            if name_pp_core_count in self.vars.namelist:
                try: self.pp.core_count = int(self.vars.namelist[name_pp_core_count])
                except: sys.exit('ERROR '+name_pp_core_count+' must be an integer')
            else: 
                self.pp.core_count = int(os.cpu_count()//2) # default
            if name_pp_huc_break_lvl in self.vars.namelist:
                try: self.pp.huc_break_lvl = int(self.vars.namelist[name_pp_huc_break_lvl])
                except: sys.exit('ERROR '+name_pp_huc_break_lvl+' must be an integer')
                if int(self.pp.huc_break_lvl < self.vars.huc_level):
                    sys.exit(f'ERROR {name_pp_huc_break_lvl}) must be <= huc_level ({self.vars.huc_level})')
            else:
                self.pp.huc_break_lvl = 12 # default
        if name_resample_method in self.vars.namelist:
            if str(self.vars.namelist[name_resample_method]).lower().find('bilinear') != -1:
                self.options.resample_method = rasterio.enums.Resampling.bilinear
                self.options.name_resample_method = 'bilinear'
            elif str(self.vars.namelist[name_resample_method]).lower().find('cubic') != -1:
                self.options.resample_method = rasterio.enums.Resampling.cubic
                self.options.name_resample_method = 'cubic'
            elif str(self.vars.namelist[name_resample_method]).lower().find('nearest') != -1:
                self.options.resample_method = rasterio.enums.Resampling.nearest
                self.options.name_resample_method = 'nearest'
        else: # default
            self.options.resample_method = rasterio.enums.Resampling.bilinear
            self.options.name_resample_method = 'bilinear'
        self._set_time_dim()

    def _set_time_dim(self):
        """Create an array of datetime objects, one for each time step in simulation period"""
        start_date_split = self.time.start_date.split('-')
        start_datetime = datetime.datetime(year=int(start_date_split[0]),month=int(start_date_split[1]),day=int(start_date_split[2]))
        end_date_split = self.time.end_date.split('-')
        end_datetime = datetime.datetime(year=int(end_date_split[0]),month=int(end_date_split[1]),day=int(end_date_split[2]))
        datetime_dim = list()
        idatetime = start_datetime
        while idatetime <= end_datetime:
            datetime_dim.append(idatetime)
            idatetime += datetime.timedelta(days=1)
        self.time.datetime_dim = numpy.array(datetime_dim)