import os,sys,math,rasterio,yaml,datetime,numpy,geopandas,hf_hydrodata

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
        output_raw              = ''
        output_raw_bilinear     = ''
        output_raw_neareast     = ''
        output_raw_cubic        = ''
        output_summary          = ''
        output_summary_bilinear = ''
        output_summary_nearest  = ''
        output_summary_cubic    = ''

    class Time:
        start_date              = ''
        end_date                = ''
        datetime_dim            = ''

    class FileNames:
        """File names including full path"""
        namlistyaml             = ''
        hucs                    = ''
        domain                  = ''
        domain_mask             = ''
        domain_buffered         = ''
        nhd                     = ''
        dem                     = ''
        dem_user                = ''
        dem_original            = ''
        dem_breached            = ''
        soil_texture            = ''
        soil_transmissivity     = ''
        flow_acc                = ''
        facc_strm_mask          = ''
        slope                   = ''
        twi                     = ''
        twi_upsample            = ''
        twi_downsample          = ''

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

    class Variables:
        """Variables"""
        huc                     = ''
        huc_level               = ''
        start_date              = ''
        end_date                = ''
        namelist                = ''
        facc_strm_threshold     = 0
        dem_rez                 = None

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

    def __init__(self,filename:str):
        """Initialize namelist"""
        self._init_vars()
        self._read_inputyaml(filename)
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
        self.time                = Namelist.Time()
        self.bbox_domain         = Namelist.BBoxDomain()
        self.bbox_parflow        = Namelist.BBoxParflow()
        
    def _set_names(self):
        """Set static directory and file names"""
        self._set_subdirectory_names()
        self._set_file_names()

    def _set_subdirectory_names(self):
        """Set project subdirectory names"""
        self.dirnames.inputs                  = os.path.join(self.dirnames.project,                 'inputs')
        self.dirnames.wtd                     = os.path.join(self.dirnames.inputs,                  'wtd')
        self.dirnames.wtd_parflow             = os.path.join(self.dirnames.wtd,                     'parflow')
        self.dirnames.wtd_parflow_raw         = os.path.join(self.dirnames.wtd_parflow,             'raw')
        self.dirnames.wtd_parflow_bilinear    = os.path.join(self.dirnames.wtd_parflow,             'bilinear')
        self.dirnames.wtd_parflow_nearest     = os.path.join(self.dirnames.wtd_parflow,             'nearest')
        self.dirnames.wtd_parflow_cubic       = os.path.join(self.dirnames.wtd_parflow,             'cubic')
        self.dirnames.wtd_fan                 = os.path.join(self.dirnames.wtd,                     'fan')
        self.dirnames.dem                     = os.path.join(self.dirnames.inputs,                  'dem')
        self.dirnames.twi                     = os.path.join(self.dirnames.inputs,                  'twi')
        self.dirnames.soils                   = os.path.join(self.dirnames.inputs,                  'soils')
        self.dirnames.domain                  = os.path.join(self.dirnames.inputs,                  'domain')
        self.dirnames.nhd                     = os.path.join(self.dirnames.inputs,                  'nhd')
        self.dirnames.output                  = os.path.join(self.dirnames.project,                 'outputs')
        self.dirnames.output_raw              = os.path.join(self.dirnames.output,                  'raw')
        self.dirnames.output_raw_bilinear     = os.path.join(self.dirnames.output_raw,              'bilinear')
        self.dirnames.output_raw_neareast     = os.path.join(self.dirnames.output_raw,              'nearest')
        self.dirnames.output_raw_cubic        = os.path.join(self.dirnames.output_raw,              'cubic')
        self.dirnames.output_summary          = os.path.join(self.dirnames.output,                  'summary')
        self.dirnames.output_summary_bilinear = os.path.join(self.dirnames.output_summary,          'bilinear')
        self.dirnames.output_summary_nearest  = os.path.join(self.dirnames.output_summary,          'nearest')
        self.dirnames.output_summary_cubic    = os.path.join(self.dirnames.output_summary,          'cubic')

    def _make_subdirectory_structure(self):
        """Make subdirectory structure"""
        for d in self.dirnames.__dict__:
            if not os.path.isdir(self.dirnames.__dict__[d]):
                os.makedirs(self.dirnames.__dict__[d])

    def _set_file_names(self):
        """Set static files names for intermediate output files"""
        self.fnames.domain              = os.path.join(self.dirnames.domain,     'domain.gpkg')
        self.fnames.domain_mask         = os.path.join(self.dirnames.domain,     'domain_mask.gpkg')
        self.fnames.domain_buffered     = os.path.join(self.dirnames.domain,     'domain_buffered.gpkg')
        self.fnames.hucs                = os.path.join(self.dirnames.domain,     'hucs.gpkg')
        self.fnames.nhd                 = os.path.join(self.dirnames.nhd,        'nhdhr.gpkg')
        self.fnames.dem                 = os.path.join(self.dirnames.dem,        'dem.tif')
        self.fnames.dem_original        = os.path.join(self.dirnames.dem,        'dem_original.tif')
        self.fnames.dem_breached        = os.path.join(self.dirnames.dem,        'dem_breached.tif')
        self.fnames.soil_texture        = os.path.join(self.dirnames.soils,      'soiltexture.gpkg')
        self.fnames.soil_transmissivity = os.path.join(self.dirnames.soils,      'soiltransmissivity.tiff')
        self.fnames.flow_acc            = os.path.join(self.dirnames.dem,        'flow_acc.tiff')
        self.fnames.slope               = os.path.join(self.dirnames.dem,        'slope.tiff')
        self.fnames.facc_strm_mask      = os.path.join(self.dirnames.dem,        'facc_strm_mask.tiff')
        self.fnames.twi                 = os.path.join(self.dirnames.twi,        'twi.tiff')
        self.fnames.twi_upsample        = os.path.join(self.dirnames.twi,        'twi_upsample.tiff')
        self.fnames.twi_downsample      = os.path.join(self.dirnames.twi,        'twi_downsample.tiff')

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

    def _get_dummy_grid_fname(self):
        for hrwtd_dir in [self.dirnames.wtd_parflow_bilinear,
                          self.dirnames.wtd_parflow_nearest,
                          self.dirnames.wtd_parflow_cubic]:
            for idatetime in self.time.datetime_dim:
                fname_hrwtd = os.path.join(hrwtd_dir,'wtd_'+idatetime.strftime('%Y%m%d')+'.tiff')
                if os.path.isfile(fname_hrwtd): return fname_hrwtd
        return None